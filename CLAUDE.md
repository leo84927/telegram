# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# telegram
負責所有與 Telegram Bot 相關的任務：
- 消費 RabbitMQ 訊息並轉發至 Telegram chat
- 接收 Telegram webhook
- 透過 gRPC 呼叫 `bookkeeping`

## 架構
```
main.go                     ← coreconfig.Load 讀設定、驗 token、組 BotSender / Worker / WebhookServer
config/common.go            ← TELEGRAM:* 設定鍵的具名形狀（無全域變數）

handle/message_handler.go   ← Worker：RabbitMQ consumer 的進入點，解析 Envelope、同步送出、錯誤往上回傳
handle/format.go            ← format：Envelope → 字串的純函式（EnvelopeType 分派、TWD 取倒數）
handle/sender.go            ← Sender 介面與 BotSender：自己發 ctx-aware 請求、重試與錯誤分類
handle/webhook.go           ← 啟動 HTTPS server、建立 bookkeeping gRPC client

router/router.go            ← webhook / health 路由、secret 驗證、instrument（開 span）、指令分派
```

## 日誌與 trace 關聯

接收端的日誌一律要帶 handler 收到的 `ctx`（`slog.InfoContext` / `logger.Error(ctx, …)`），
否則寫不出 `trace_id` / `span_id`，在 Grafana 上就和 span 脫鉤。詳見 `CONTEXT.md` 的系統級不變條件
「trace 跨服務不斷開」。

## 設定鍵

| 鍵 | 用途 |
|---|---|
| `TELEGRAM_SERVICE_NAME` | 服務名稱 |
| `TELEGRAM_TOKEN` | Bot API token，啟動時以 `getMe` 驗證 |
| `TELEGRAM_CHAT_ID` | 告警送達的 chat |
| `TELEGRAM_RABBITMQ_QUEUE` | 訂閱的 queue 名稱 |
| `TELEGRAM_RABBITMQ_KEY` | routing key |
| `TELEGRAM_WEBHOOK_CERT_PEM` | webhook HTTPS 憑證 |
| `TELEGRAM_WEBHOOK_KEY_PEM` | webhook HTTPS 私鑰 |
| `TELEGRAM_WEBHOOK_PORT` | webhook 監聽地址 |
| `TELEGRAM_WEBHOOK_SECRET` | 驗證 telegram 來源的 secret token，**空字串代表刻意不啟用驗證** |

## 測試

```sh
go test ./... -race -v -count=1
golangci-lint run ./...
```
