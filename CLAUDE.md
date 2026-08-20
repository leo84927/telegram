# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# telegram
負責所有與 Telegram Bot 相關的任務：
- 消費 RabbitMQ 訊息並轉發至 Telegram chat
- 接收 Telegram webhook
- 透過 gRPC 呼叫 `bookkeeping`

## 架構
```
main.go                     ← 從 Redis 載入設定、建立 RabbitMQ topology 與 bookkeeping gRPC client
config/common.go            ← 設定值的全域變數

handle/message_handler.go   ← RabbitMQ consumer 的進入點
handle/telegram_handler.go  ← TelegramManager：解析 Envelope、格式化訊息、呼叫 Bot API 發送
handle/webhook.go           ← 啟動 HTTPS server

router/router.go            ← webhook / health 路由、secret 驗證、instrument（開 span）、指令分派
```

設定不由 `config` 的 `init()` 載入，而是在 `main` 依序賦值——順序有意義：`InitFromRedis` 先把
`TELEGRAM:*` 與 `GLOBAL:*` 讀進 `coreconfig.EnvMap`，之後才能取出 topology 與 bookkeeping sock path。
