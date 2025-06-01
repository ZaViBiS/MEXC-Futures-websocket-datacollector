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
	Price     string
	Quantity  string
	Side      string // "buy" або "sell"
	Timestamp int64
	TradeID   string `gorm:"index"`
}

type OrderBook struct {
	ID        uint   `gorm:"primaryKey"`
	Symbol    string `gorm:"index"`
	Bids      string // JSON масив
	Asks      string // JSON масив
	Timestamp int64
}

type Kline struct {
	ID        uint   `gorm:"primaryKey"`
	Symbol    string `gorm:"index"`
	OpenTime  int64
	CloseTime int64
	Open      string
	High      string
	Low       string
	Close     string
	Volume    string
	Timestamp int64
}

type DataCollector struct {
	config   *Config
	db       *gorm.DB
	conn     *websocket.Conn
	mu       sync.RWMutex
	stopChan chan struct{}
	wg       sync.WaitGroup
	msgCount int
}

func NewDataCollector(configPath string) (*DataCollector, error) {
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("не вдалося прочитати конфігурацію: %v", err)
	}

	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("не вдалося розпарсити конфігурацію: %v", err)
	}

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

	if err := collector.createTables(); err != nil {
		return nil, fmt.Errorf("не вдалося створити таблиці: %v", err)
	}

	return collector, nil
}

func (dc *DataCollector) createTables() error {
	for _, symbol := range dc.config.Symbols {
		tableName := strings.ToLower(strings.ReplaceAll(symbol, "_", ""))

		// Таблиці для трейдів
		tradeTableName := tableName + "_trades"
		if err := dc.db.Table(tradeTableName).AutoMigrate(&Trade{}); err != nil {
			return fmt.Errorf("не вдалося створити таблицю %s: %v", tradeTableName, err)
		}

		// Таблиці для order book
		obTableName := tableName + "_orderbook"
		if err := dc.db.Table(obTableName).AutoMigrate(&OrderBook{}); err != nil {
			return fmt.Errorf("не вдалося створити таблицю %s: %v", obTableName, err)
		}

		// Таблиці для klines
		klineTableName := tableName + "_klines"
		if err := dc.db.Table(klineTableName).AutoMigrate(&Kline{}); err != nil {
			return fmt.Errorf("не вдалося створити таблицю %s: %v", klineTableName, err)
		}

		log.Printf("Створено таблиці для пари %s", symbol)
	}
	return nil
}

func (dc *DataCollector) connect() error {
	wsURL := "wss://wbs.mexc.com/ws"

	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 15 * time.Second

	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("не вдалося підключитися до WebSocket: %v", err)
	}

	dc.mu.Lock()
	dc.conn = conn
	dc.mu.Unlock()

	log.Printf("✅ Підключення до MEXC встановлено: %s", wsURL)
	return nil
}

func (dc *DataCollector) subscribe() error {
	dc.mu.RLock()
	conn := dc.conn
	dc.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("WebSocket підключення не встановлено")
	}

	// Підписка на різні типи даних для кожного символу
	id := 1
	for _, symbol := range dc.config.Symbols {
		// Конвертуємо символ (BTC_USDT -> BTCUSDT)
		mexcSymbol := strings.ReplaceAll(symbol, "_", "")

		subscriptions := []map[string]interface{}{
			// Trades (deals)
			{
				"method": "SUBSCRIPTION",
				"params": []string{fmt.Sprintf("spot@public.deals.v3.api@%s", mexcSymbol)},
				"id":     id,
			},
			// Order book ticker
			{
				"method": "SUBSCRIPTION",
				"params": []string{fmt.Sprintf("spot@public.bookTicker.v3.api@%s", mexcSymbol)},
				"id":     id + 1,
			},
			// Kline 1m
			{
				"method": "SUBSCRIPTION",
				"params": []string{fmt.Sprintf("spot@public.kline.v3.api@%s@Min1", mexcSymbol)},
				"id":     id + 2,
			},
			// Depth
			{
				"method": "SUBSCRIPTION",
				"params": []string{fmt.Sprintf("spot@public.increase.depth.v3.api@%s", mexcSymbol)},
				"id":     id + 3,
			},
		}

		for i, sub := range subscriptions {
			if err := conn.WriteJSON(sub); err != nil {
				log.Printf("Помилка підписки %d для %s: %v", i+1, symbol, err)
				continue
			}

			log.Printf("📡 Підписано на %s (тип %d)", symbol, i+1)
			time.Sleep(200 * time.Millisecond)
		}

		id += 10 // Розділяємо ID для різних символів
	}

	return nil
}

func (dc *DataCollector) saveTrade(symbol string, data map[string]interface{}) {
	// Парсимо дані трейду
	var price, quantity, side, tradeID string
	var timestamp int64

	if p, ok := data["p"].(string); ok {
		price = p
	} else if p, ok := data["price"].(string); ok {
		price = p
	}

	if q, ok := data["v"].(string); ok {
		quantity = q
	} else if q, ok := data["quantity"].(string); ok {
		quantity = q
	}

	if s, ok := data["S"].(float64); ok {
		if s == 1 {
			side = "buy"
		} else {
			side = "sell"
		}
	} else if s, ok := data["side"].(string); ok {
		side = s
	}

	if t, ok := data["t"].(float64); ok {
		timestamp = int64(t)
	} else if t, ok := data["timestamp"].(float64); ok {
		timestamp = int64(t)
	} else {
		timestamp = time.Now().UnixMilli()
	}

	if id, ok := data["i"].(string); ok {
		tradeID = id
	} else if id, ok := data["tradeId"].(string); ok {
		tradeID = id
	} else {
		tradeID = fmt.Sprintf("%d_%s", timestamp, symbol)
	}

	if price == "" || quantity == "" {
		return
	}

	trade := Trade{
		Symbol:    symbol,
		Price:     price,
		Quantity:  quantity,
		Side:      side,
		Timestamp: timestamp,
		TradeID:   tradeID,
	}

	tableName := strings.ToLower(strings.ReplaceAll(symbol, "_", "")) + "_trades"

	if err := dc.db.Table(tableName).Create(&trade).Error; err != nil {
		log.Printf("Помилка збереження трейду: %v", err)
	} else {
		log.Printf("💰 Трейд збережено: %s %s %s @ %s", symbol, side, quantity, price)
	}
}

func (dc *DataCollector) saveOrderBook(symbol string, data map[string]interface{}) {
	var bids, asks interface{}
	var timestamp int64

	if b, ok := data["bids"]; ok {
		bids = b
	}
	if a, ok := data["asks"]; ok {
		asks = a
	}

	if t, ok := data["t"].(float64); ok {
		timestamp = int64(t)
	} else {
		timestamp = time.Now().UnixMilli()
	}

	bidsJSON, _ := json.Marshal(bids)
	asksJSON, _ := json.Marshal(asks)

	orderBook := OrderBook{
		Symbol:    symbol,
		Bids:      string(bidsJSON),
		Asks:      string(asksJSON),
		Timestamp: timestamp,
	}

	tableName := strings.ToLower(strings.ReplaceAll(symbol, "_", "")) + "_orderbook"

	if err := dc.db.Table(tableName).Create(&orderBook).Error; err != nil {
		log.Printf("Помилка збереження order book: %v", err)
	} else {
		log.Printf("📊 Order book збережено: %s", symbol)
	}
}

func (dc *DataCollector) handleMessage(message []byte) {
	dc.msgCount++

	var wsResp map[string]interface{}
	if err := json.Unmarshal(message, &wsResp); err != nil {
		log.Printf("Помилка розбору повідомлення: %v", err)
		return
	}

	// Логуємо кожне 10-е повідомлення для debug
	if dc.msgCount%10 == 0 {
		log.Printf("📨 Повідомлення #%d отримано", dc.msgCount)
	}

	// Перевіряємо відповідь на підписку
	if code, ok := wsResp["code"].(float64); ok {
		if msg, ok := wsResp["msg"].(string); ok {
			if code != 0 {
				log.Printf("⚠️ Помилка підписки: %s", msg)
			} else if strings.Contains(msg, "success") {
				log.Printf("✅ Підписка успішна: %s", msg)
			}
		}
		return
	}

	// Обробляємо дані
	if channel, ok := wsResp["channel"].(string); ok {
		if data, ok := wsResp["data"]; ok {
			// Визначаємо символ з каналу
			parts := strings.Split(channel, "@")
			if len(parts) >= 3 {
				symbol := parts[2]
				// Конвертуємо назад (BTCUSDT -> BTC_USDT)
				if len(symbol) >= 6 {
					formattedSymbol := symbol[:3] + "_" + symbol[3:]

					switch {
					case strings.Contains(channel, "deals"):
						if dataArray, ok := data.([]interface{}); ok {
							for _, item := range dataArray {
								if tradeData, ok := item.(map[string]interface{}); ok {
									dc.saveTrade(formattedSymbol, tradeData)
								}
							}
						} else if tradeData, ok := data.(map[string]interface{}); ok {
							dc.saveTrade(formattedSymbol, tradeData)
						}
					case strings.Contains(channel, "depth") || strings.Contains(channel, "bookTicker"):
						if obData, ok := data.(map[string]interface{}); ok {
							dc.saveOrderBook(formattedSymbol, obData)
						}
					}
				}
			}
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

			conn.SetReadDeadline(time.Now().Add(60 * time.Second))

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
				log.Println("🔄 Спроба переподключення...")
				if err := dc.connect(); err != nil {
					log.Printf("Помилка переподключення: %v", err)
					continue
				}

				time.Sleep(2 * time.Second)

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
				log.Println("✅ Переподключення успішне")
			}
		}
	}
}

func (dc *DataCollector) Start() error {
	if err := dc.connect(); err != nil {
		return err
	}

	time.Sleep(2 * time.Second)

	if err := dc.subscribe(); err != nil {
		return err
	}

	dc.wg.Add(1)
	go dc.listen()

	go dc.reconnect()

	log.Println("🚀 Збирач даних MEXC запущено")
	return nil
}

func (dc *DataCollector) Stop() {
	log.Println("⏹️ Зупинка збирача даних...")

	close(dc.stopChan)

	dc.mu.Lock()
	if dc.conn != nil {
		dc.conn.Close()
		dc.conn = nil
	}
	dc.mu.Unlock()

	dc.wg.Wait()
	log.Printf("✅ Збирач даних зупинено (оброблено %d повідомлень)", dc.msgCount)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Використання: go run main.go config.json")
	}

	configPath := os.Args[1]

	collector, err := NewDataCollector(configPath)
	if err != nil {
		log.Fatalf("Помилка створення збирача: %v", err)
	}

	if err := collector.Start(); err != nil {
		log.Fatalf("Помилка запуску збирача: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	collector.Stop()
}
