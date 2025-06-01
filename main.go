package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Конфігурація
type Config struct {
	DatabasePath   string   `json:"database_path"`
	Symbols        []string `json:"symbols"`
	ReconnectDelay int      `json:"reconnect_delay_seconds"`
}

// Структури даних
type Trade struct {
	ID        uint   `gorm:"primaryKey"`
	Symbol    string `gorm:"index"`
	Price     string // зберігати як строку без float конвертації
	Quantity  string // зберігати як строку без float конвертації
	Side      string // "buy" або "sell"
	Timestamp int64  // мілісекунди
	TradeID   string `gorm:"index"`
}

type OrderBook struct {
	ID        uint   `gorm:"primaryKey"`
	Symbol    string `gorm:"index"`
	Bids      string // JSON масив [[price, quantity], ...] як строка
	Asks      string // JSON масив [[price, quantity], ...] як строка
	Timestamp int64  // мілісекунди
}

// WebSocket повідомлення
type WSMessage struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
	ID     int         `json:"id,omitempty"`
}

type TradeData struct {
	Symbol    string `json:"symbol"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	Side      string `json:"side"`
	Timestamp int64  `json:"timestamp"`
	TradeID   string `json:"tradeId"`
}

type OrderBookData struct {
	Symbol    string     `json:"symbol"`
	Bids      [][]string `json:"bids"`
	Asks      [][]string `json:"asks"`
	Timestamp int64      `json:"timestamp"`
}

type DataCollector struct {
	config   *Config
	db       *gorm.DB
	conn     *websocket.Conn
	mu       sync.RWMutex
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewDataCollector(configPath string) (*DataCollector, error) {
	// Завантаження конфігурації
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("не вдалося прочитати конфігурацію: %v", err)
	}

	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("не вдалося розпарсити конфігурацію: %v", err)
	}

	// Підключення до бази даних
	db, err := gorm.Open(sqlite.Open(config.DatabasePath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("не вдалося підключитися до БД: %v", err)
	}

	collector := &DataCollector{
		config:   &config,
		db:       db,
		stopChan: make(chan struct{}),
	}

	// Створення таблиць для кожної пари
	if err := collector.createTables(); err != nil {
		return nil, fmt.Errorf("не вдалося створити таблиці: %v", err)
	}

	return collector, nil
}

func (dc *DataCollector) createTables() error {
	for _, symbol := range dc.config.Symbols {
		// Очищуємо назву символу для використання в назві таблиці
		tableName := strings.ToLower(strings.ReplaceAll(symbol, "/", "_"))

		// Створюємо таблиці для трейдів
		tradeTableName := tableName + "_trades"
		if err := dc.db.Table(tradeTableName).AutoMigrate(&Trade{}); err != nil {
			return fmt.Errorf("не вдалося створити таблицю %s: %v", tradeTableName, err)
		}

		// Створюємо таблиці для order book
		obTableName := tableName + "_orderbook"
		if err := dc.db.Table(obTableName).AutoMigrate(&OrderBook{}); err != nil {
			return fmt.Errorf("не вдалося створити таблицю %s: %v", obTableName, err)
		}

		log.Printf("Створено таблиці для пари %s: %s, %s", symbol, tradeTableName, obTableName)
	}
	return nil
}

func (dc *DataCollector) connect() error {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	conn, _, err := dialer.Dial("wss://contract.mexc.com/ws", nil)
	if err != nil {
		return fmt.Errorf("не вдалося підключитися до WebSocket: %v", err)
	}

	dc.mu.Lock()
	dc.conn = conn
	dc.mu.Unlock()

	log.Println("Підключення до MEXC WebSocket встановлено")
	return nil
}

func (dc *DataCollector) subscribe() error {
	dc.mu.RLock()
	conn := dc.conn
	dc.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("WebSocket підключення не встановлено")
	}

	// Підписка на трейди
	for _, symbol := range dc.config.Symbols {
		tradeMsg := WSMessage{
			Method: "sub.deal",
			Params: []string{symbol},
			ID:     1,
		}

		if err := conn.WriteJSON(tradeMsg); err != nil {
			return fmt.Errorf("не вдалося підписатися на трейди для %s: %v", symbol, err)
		}

		log.Printf("Підписано на трейди для %s", symbol)

		// Підписка на order book
		obMsg := WSMessage{
			Method: "sub.depth",
			Params: []interface{}{symbol, 10, "0.1"},
			ID:     2,
		}

		if err := conn.WriteJSON(obMsg); err != nil {
			return fmt.Errorf("не вдалося підписатися на order book для %s: %v", symbol, err)
		}

		log.Printf("Підписано на order book для %s", symbol)
	}

	return nil
}

func (dc *DataCollector) saveTrade(data map[string]interface{}) {
	symbol, ok := data["symbol"].(string)
	if !ok {
		log.Printf("Невірний формат символу в трейді: %v", data)
		return
	}

	price, ok := data["price"].(string)
	if !ok {
		log.Printf("Невірний формат ціни в трейді: %v", data)
		return
	}

	quantity, ok := data["quantity"].(string)
	if !ok {
		log.Printf("Невірний формат кількості в трейді: %v", data)
		return
	}

	side, ok := data["side"].(string)
	if !ok {
		log.Printf("Невірний формат сторони в трейді: %v", data)
		return
	}

	var timestamp int64
	if ts, ok := data["timestamp"].(float64); ok {
		timestamp = int64(ts)
	} else {
		timestamp = time.Now().UnixMilli()
	}

	tradeID, ok := data["tradeId"].(string)
	if !ok {
		if id, ok := data["tradeId"].(float64); ok {
			tradeID = fmt.Sprintf("%.0f", id)
		} else {
			tradeID = fmt.Sprintf("%d", timestamp)
		}
	}

	trade := Trade{
		Symbol:    symbol,
		Price:     price,
		Quantity:  quantity,
		Side:      side,
		Timestamp: timestamp,
		TradeID:   tradeID,
	}

	tableName := strings.ToLower(strings.ReplaceAll(symbol, "/", "_")) + "_trades"

	if err := dc.db.Table(tableName).Create(&trade).Error; err != nil {
		log.Printf("Помилка збереження трейду в %s: %v", tableName, err)
	}
}

func (dc *DataCollector) saveOrderBook(data map[string]interface{}) {
	symbol, ok := data["symbol"].(string)
	if !ok {
		log.Printf("Невірний формат символу в order book: %v", data)
		return
	}

	bidsInterface, ok := data["bids"].([]interface{})
	if !ok {
		log.Printf("Невірний формат bids в order book: %v", data)
		return
	}

	asksInterface, ok := data["asks"].([]interface{})
	if !ok {
		log.Printf("Невірний формат asks в order book: %v", data)
		return
	}

	// Конвертуємо bids та asks в JSON строки
	bidsJSON, err := json.Marshal(bidsInterface)
	if err != nil {
		log.Printf("Помилка конвертації bids в JSON: %v", err)
		return
	}

	asksJSON, err := json.Marshal(asksInterface)
	if err != nil {
		log.Printf("Помилка конвертації asks в JSON: %v", err)
		return
	}

	var timestamp int64
	if ts, ok := data["timestamp"].(float64); ok {
		timestamp = int64(ts)
	} else {
		timestamp = time.Now().UnixMilli()
	}

	orderBook := OrderBook{
		Symbol:    symbol,
		Bids:      string(bidsJSON),
		Asks:      string(asksJSON),
		Timestamp: timestamp,
	}

	tableName := strings.ToLower(strings.ReplaceAll(symbol, "/", "_")) + "_orderbook"

	if err := dc.db.Table(tableName).Create(&orderBook).Error; err != nil {
		log.Printf("Помилка збереження order book в %s: %v", tableName, err)
	}
}

func (dc *DataCollector) handleMessage(message []byte) {
	var wsResp map[string]interface{}
	if err := json.Unmarshal(message, &wsResp); err != nil {
		log.Printf("Помилка розбору повідомлення: %v", err)
		return
	}

	// Перевіряємо тип повідомлення
	if channel, ok := wsResp["channel"].(string); ok {
		data, ok := wsResp["data"].(map[string]interface{})
		if !ok {
			return
		}

		switch {
		case strings.Contains(channel, "deal"):
			dc.saveTrade(data)
		case strings.Contains(channel, "depth"):
			dc.saveOrderBook(data)
		}
	}
}

func (dc *DataCollector) listen() {
	defer dc.wg.Done()

	for {
		select {
		case <-dc.stopChan:
			return
		default:
			dc.mu.RLock()
			conn := dc.conn
			dc.mu.RUnlock()

			if conn == nil {
				time.Sleep(time.Second)
				continue
			}

			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Помилка читання повідомлення: %v", err)
				dc.mu.Lock()
				if dc.conn != nil {
					dc.conn.Close()
					dc.conn = nil
				}
				dc.mu.Unlock()
				return
			}

			go dc.handleMessage(message)
		}
	}
}

func (dc *DataCollector) reconnect() {
	ticker := time.NewTicker(time.Duration(dc.config.ReconnectDelay) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-dc.stopChan:
			return
		case <-ticker.C:
			dc.mu.RLock()
			connected := dc.conn != nil
			dc.mu.RUnlock()

			if !connected {
				log.Println("Спроба переподключення...")
				if err := dc.connect(); err != nil {
					log.Printf("Помилка переподключення: %v", err)
					continue
				}

				if err := dc.subscribe(); err != nil {
					log.Printf("Помилка підписки після переподключення: %v", err)
					dc.mu.Lock()
					if dc.conn != nil {
						dc.conn.Close()
						dc.conn = nil
					}
					dc.mu.Unlock()
					continue
				}

				dc.wg.Add(1)
				go dc.listen()
				log.Println("Переподключення успішне")
			}
		}
	}
}

func (dc *DataCollector) Start() error {
	// Початкове підключення
	if err := dc.connect(); err != nil {
		return err
	}

	if err := dc.subscribe(); err != nil {
		return err
	}

	// Запуск горутин
	dc.wg.Add(1)
	go dc.listen()

	go dc.reconnect()

	log.Println("Збирач даних запущено")
	return nil
}

func (dc *DataCollector) Stop() {
	log.Println("Зупинка збирача даних...")

	close(dc.stopChan)

	dc.mu.Lock()
	if dc.conn != nil {
		dc.conn.Close()
		dc.conn = nil
	}
	dc.mu.Unlock()

	dc.wg.Wait()
	log.Println("Збирач даних зупинено")
}

func main() {
	// Перевірка аргументів командного рядка
	if len(os.Args) < 2 {
		log.Fatal("Використання: go run main.go config.json")
	}

	configPath := os.Args[1]

	// Створення збирача даних
	collector, err := NewDataCollector(configPath)
	if err != nil {
		log.Fatalf("Помилка створення збирача: %v", err)
	}

	// Запуск збирача
	if err := collector.Start(); err != nil {
		log.Fatalf("Помилка запуску збирача: %v", err)
	}

	// Обробка сигналів для graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	collector.Stop()
}
