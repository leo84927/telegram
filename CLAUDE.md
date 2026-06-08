# telegram
負責處理所有與 Telegram Bot 相關的任務，目前主要功能為接收 RabbitMQ 訊息並轉發至 Telegram chat

## 架構

```
config/init.go          ← init() 從 Consul 載入設定、建立 RabbitMQ topology
config/common.go        ← TelegramToken、TelegramChatId

handle/message_handler.go   ← RabbitMQ consumer 的進入點
handle/telegram_handler.go  ← TelegramManager：解析 Envelope、格式化訊息、呼叫 Bot API 發送

main.go                 ← 使用 core/initialize.App 啟動，註冊 consumer worker
```

## 訊息處理流程

```
RabbitMQ message
  → MessageHandler 接收
  → protojson 解析 Envelope
  → 依據 EnvelopeType 決定訊息格式：
      TELEGRAM_SUCCESS_EXCHANGE_RATE → "BASE/COUNTER : rate"（TWD 時自動取倒數）
      TELEGRAM_ERROR / default      → 直接送出 Envelope.Data
  → Telegram Bot API 發送
```

## Consul 設定鍵

| 鍵 | 用途 |
|---|---|
| `TELEGRAM_SERVICE_NAME` | 服務名稱 |
| `TELEGRAM_TOKEN` | Bot API token |
| `TELEGRAM_CHAT_ID` | 目標 chat ID |
| `TELEGRAM_RABBITMQ_QUEUE` | 訂閱的 queue 名稱 |
| `TELEGRAM_RABBITMQ_KEY` | routing key |

## 依賴

- `github.com/go-telegram-bot-api/telegram-bot-api/v5` — Telegram Bot API
- `github.com/shopspring/decimal` — 匯率精確運算
- `github.com/leo84927/core` — 共用基礎建設
- `buf.build/gen/go/.../scheduler` — proto 定義（Envelope、ExchangeRate、Currency）
