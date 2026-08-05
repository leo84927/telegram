# telegram
負責處理所有與 Telegram Bot 相關的任務，目前主要功能為接收 RabbitMQ 訊息並轉發至 Telegram chat

# 相關 API

```sh
# 綁定 telegram webhook
curl -F "url=<作為 webhook 接收 server 的 url>" \
     -F "certificate=@webhook.pem" \
     https://api.telegram.org/bot<token>/setWebhook
# header 帶 secret 綁定
curl -F "url=https://<你的IP>:8443/webhook" \
     -F "certificate=@webhook.pem" \
     -F "secret_token=$SECRET" \
     https://api.telegram.org/bot<token>/setWebhook

# 檢查是否綁定成功
https://api.telegram.org/bot<token>/getWebhookInfo

# 內部呼叫
curl -k -X POST https://localhost:8443/health
# 外部呼叫
curl -k -X POST https://<靜態 IP>:8443/health
# 檢驗沒帶 secret 是否會正常返回 401
curl -i -k -X POST https://<靜態 IP>:8443/webhook -d '{}'
```

## 架構

```
config/init.go          ← init() 載入環境變數、建立 RabbitMQ topology
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

## 設定鍵

| 鍵 | 用途 |
|---|---|
| `TELEGRAM_SERVICE_NAME` | 服務名稱 |
| `TELEGRAM_TOKEN` | Bot API token |
| `TELEGRAM_CHAT_ID` | 目標 chat ID |
| `TELEGRAM_RABBITMQ_QUEUE` | 訂閱的 queue 名稱 |
| `TELEGRAM_RABBITMQ_KEY` | routing key |
| `TELEGRAM_WEBHOOK_CERT_PEM` | webhook TLS 憑證（PEM 內容） |
| `TELEGRAM_WEBHOOK_KEY_PEM` | webhook TLS 私鑰（PEM 內容） |
| `TELEGRAM_WEBHOOK_PORT` | webhook 監聽位址，例如 `:8443` |
| `TELEGRAM_WEBHOOK_SECRET` | webhook 驗證用 secret，須與 setWebhook 的 `secret_token` 一致；空值則不驗證 |

## 依賴

- `github.com/go-telegram-bot-api/telegram-bot-api/v5` — Telegram Bot API
- `github.com/shopspring/decimal` — 匯率精確運算
- `github.com/leo84927/core` — 共用基礎建設
- `buf.build/gen/go/.../scheduler` — proto 定義（Envelope、ExchangeRate、Currency）
