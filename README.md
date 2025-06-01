# MEXC Futures WebSocket Data Collector

Скрипт для збору даних з MEXC Futures через WebSocket API в реальному часі.

## Функціонал

- ✅ Збір всіх угод (trades) з вибраних futures пар в реальному часі
- ✅ Збір стакана (order book) з глибиною 10 рівнів 
- ✅ Збереження даних в SQLite3 використовуючи GORM
- ✅ Окремі таблиці для кожної пари (`{pair}_trades` та `{pair}_orderbook`)
- ✅ Горутини для паралельної обробки
- ✅ Автоматичне переподключення при розриві з'єднання
- ✅ Обробка помилок та graceful shutdown
- ✅ Збереження числових значень як рядків без конвертації

## Встановлення

1. Склонуйте репозиторій або створіть новий проект:
```bash
mkdir mexc-collector
cd mexc-collector
```

2. Створіть файли `main.go`, `config.json` та `go.mod` з наданого коду

3. Встановіть залежності:
```bash
go mod tidy
```

## Конфігурація

Створіть файл `config.json`:

```json
{
  "database_path": "mexc_data.db",
  "reconnect_delay_seconds": 5,
  "symbols": [
    "BTC_USDT",
    "ETH_USDT",
    "BNB_USDT",
    "SOL_USDT",
    "ADA_USDT"
  ]
}
```

### Параметри конфігурації:

- `database_path` - шлях до файлу SQLite бази даних
- `reconnect_delay_seconds` - затримка між спробами переподключення (в секундах)
- `symbols` - список futures пар для моніторингу

## Запуск

```bash
go run main.go config.json
```

## Структура даних

### Таблиці trades (`{pair}_trades`)
- `id` - первинний ключ
- `symbol` - символ пари (індекс)
- `price` - ціна як рядок
- `quantity` - кількість як рядок
- `side` - сторона угоди ("buy" або "sell")
- `timestamp` - час в мілісекундах
- `trade_id` - ID угоди (індекс)

### Таблиці order book (`{pair}_orderbook`)
- `id` - первинний ключ
- `symbol` - символ пари (індекс)
- `bids` - JSON масив bid'ів [[price, quantity], ...]
- `asks` - JSON масив ask'ів [[price, quantity], ...]
- `timestamp` - час в мілісекундах

## Приклади запитів до бази даних

### Останні 10 угод для BTC_USDT:
```sql
SELECT * FROM btc_usdt_trades ORDER BY timestamp DESC LIMIT 10;
```

### Останній стакан для ETH_USDT:
```sql
SELECT * FROM eth_usdt_orderbook ORDER BY timestamp DESC LIMIT 1;
```

### Угоди за останню годину:
```sql
SELECT * FROM btc_usdt_trades 
WHERE timestamp > (strftime('%s', 'now') - 3600) * 1000 
ORDER BY timestamp DESC;
```

## Особливості

1. **Збереження як рядків**: Всі числові значення (ціни, кількості) зберігаються як рядки для збереження точності
2. **Автоматичне переподключення**: При розриві з'єднання автоматично відбувається переподключення
3. **Горутини**: Використовуються для паралельної обробки повідомлень
4. **Graceful shutdown**: Коректна зупинка при отриманні SIGINT/SIGTERM
5. **Індекси**: Створюються індекси на symbol та trade_id для швидкого пошуку

## Зупинка програми

Використовуйте `Ctrl+C` для коректної зупинки програми.

## Логування

Програма виводить інформацію про:
- Підключення до WebSocket
- Підписки на дані
- Створення таблиць
- Помилки та переподключення
- Процес зупинки

## Вимоги

- Go 1.21+
- Стабільне інтернет-з'єднання
- Достатньо дискового простору для бази даних
