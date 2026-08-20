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

handle/webhook.go           ← WebhookServer：HTTPS server 啟動；NewBookkeepingClient：已 instrument 的 gRPC client
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

## 日誌與 trace 關聯

消費路徑（`MessageHandler` → `sendMessage` → `buildMessage`）上的日誌**一律使用帶 `ctx` 的版本**
（`slog.InfoContext` / `logger.Error(ctx, ...)`），不要用 `slog.Info`。

core 的 consumer 已從 AMQP headers 萃取上游 `traceparent` 並開了 span（見 `core/CLAUDE.md` 的
「Trace context 傳播」），日誌只有帶著那個 `ctx` 才會寫出 `trace_id` / `span_id`，
Grafana 上才能和上游 `exchange_rate` 的日誌串成同一條。內部函式若還沒有 `ctx` 參數，要一併穿透。

webhook 路徑（`router/`）同樣適用，只是 span 由自己開：`router.instrument` 為每個 webhook 請求開一個
`SpanKindServer` 的 span（名稱 `POST /webhook`，帶 `http.request.method` / `url.path` /
`http.response.status_code`），再把該 span 的 `ctx` 交給 handler。handler 內的日誌——包含拒絕未授權請求
那則——都要用帶 `ctx` 的版本，否則在 Grafana 上會和 span 脫鉤。

`instrument` 只套在 webhook，`health` 刻意不套：部署健康檢查頻率高，開 span 只會灌爆 trace。
未通過 secret 驗證的請求仍會開 span——那正是需要追查來源的場合，代價是外部流量可以驅動 span 產生。

被 `instrument` 包住的 handler 回傳它實際寫出的狀態碼，新增回應路徑時記得回傳值要跟 `WriteHeader` 一致。
狀態碼是唯一的錯誤判準，所以**回應 200 但業務失敗的分支**（例如 `/group` 查詢失敗仍要把錯誤訊息回給使用者）
要自己 `RecordError` + `SetStatus(codes.Error, ...)`，否則 Grafana 上整條 trace 都是綠的。
handler panic 由 `instrument` 補記 500 後照原樣往上拋，交還給 `net/http`。

telegram → bookkeeping 的 gRPC 呼叫由 `handle.NewBookkeepingClient` 掛上 `otelgrpc` 的
`WithStatsHandler`，把 webhook span 的 `traceparent` 注入 gRPC metadata；`bookkeeping` 端同樣掛了
StatsHandler 萃取並開子 span，因此一次使用者輸入是單一 trace：webhook span → gRPC 呼叫端 span →
bookkeeping 服務端 span。少了任一端就會斷成兩條 trace。

注入 `traceparent` 的前提是**全域 propagator 已設定**（production 由 `core` 的 `SetTracer` 設；otel 的
預設是空的 composite，什麼都不會寫出），所以測試裡碰到 span 的案例都要自己補 `otel.SetTextMapPropagator`。

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
- `go.opentelemetry.io/otel`、`go.opentelemetry.io/otel/trace` — webhook 路徑開 span
- `go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc` — bookkeeping gRPC 呼叫端
  span 與 `traceparent` 注入（版本須與 otel 對齊：v0.69.0 ↔ otel v1.44.0）
- `go.opentelemetry.io/otel/sdk` — 僅測試使用（記憶體 tracer provider 驗證 span 與日誌關聯）
