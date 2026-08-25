package router

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"buf.build/gen/go/leo84927-proto/scheduler/grpc/go/bookkeeping/bookkeepinggrpc"
	bookkeepingpb "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/bookkeeping"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/leo84927/core/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

/*
 * 建立自訂的 router，並把 webhook handler 需要的東西注入
 *
 * webhookSecret 改用參數傳入而不是讀 config 全域（#11 交接）。空字串是合法值（= 放行），
 * 所以刻意用位置參數而不是 Config struct：struct 欄位漏填就是零值，等於靜靜地關掉驗證。
 */
func New(bookkeepingConn *grpc.ClientConn, webhookSecret string) *http.ServeMux {
	mux := http.NewServeMux()

	// health 不套 instrument：若未來部署健康檢查頻率高，開 span 會灌爆 trace
	mux.HandleFunc("POST /health", health)
	mux.Handle("POST /webhook", instrument(func(w http.ResponseWriter, r *http.Request) int {
		return webhook(w, r, bookkeepinggrpc.NewBookkeepingServiceClient(bookkeepingConn), webhookSecret)
	}))

	return mux
}

func health(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "health check success")
}

/*
 * instrument 為單一 handler 開 server span，並把 span context 交給 handler
 * handler 回傳實際寫出的狀態碼：比包一層 ResponseWriter 少一個型別，也不會擦掉 Flusher 之類的介面
 * 這個 ctx 是該路徑上日誌拿到 trace_id / span_id 的唯一來源，handler 內的日誌一律要帶它
 */
func instrument(handler func(http.ResponseWriter, *http.Request) int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 生成 server span，並把 span context 注入給 handler
		ctx, span := otel.Tracer("router").Start(
			r.Context(),
			r.Method+" "+r.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				semconv.HTTPRequestMethodKey.String(r.Method),
				semconv.URLPath(r.URL.Path),
			),
		)
		defer span.End()

		/*
		 * handler panic 時 net/http 會在連線層自己 recover，status 永遠不會被賦值，
		 * 最該被看到的那次請求反而會留下一個沒有狀態的空 span；這裡補記後照原樣往上拋，
		 * 交還給 net/http 處理，不改變既有行為
		 */
		defer func() {
			if recovered := recover(); recovered != nil {
				span.SetAttributes(semconv.HTTPResponseStatusCode(http.StatusInternalServerError))
				span.SetStatus(codes.Error, fmt.Sprintf("panic: %v", recovered))
				panic(recovered)
			}
		}()

		// 執行 handler，並把 span context 注入給 handler
		status := handler(w, r.WithContext(ctx))

		// 記錄 handler 寫出的狀態碼，並在 5xx 時標記 span 為 error
		span.SetAttributes(semconv.HTTPResponseStatusCode(status))
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
	})
}

/*
 * 比對 telegram 帶來的 secret token header
 *
 * want 改用參數傳入：原本讀 config.WebhookSecret 全域，導致 router_test.go 有 7 處靠寫全域
 * 建立測試情境（測試互相污染的風險），而介面就是測試面。
 */
func authorizedSecret(got, want string) bool {
	// 未設定 secret 時放行，避免設定 secret 前直接中斷既有 webhook
	if want == "" {
		return true
	}

	// 定時比較，避免 timing attack 洩漏 secret
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func webhook(w http.ResponseWriter, r *http.Request, bk bookkeepinggrpc.BookkeepingServiceClient, secret string) int {
	// 日誌要帶 ctx 才會有 trace_id / span_id（見 CLAUDE.md 的「日誌與 trace 關聯」）
	ctx := r.Context()

	// 驗證 telegram 帶來的 secret token header
	if !authorizedSecret(r.Header.Get("X-Telegram-Bot-Api-Secret-Token"), secret) {
		slog.WarnContext(
			ctx,
			"webhook rejected: invalid secret token",
			"remote", r.RemoteAddr,
		)
		w.WriteHeader(http.StatusUnauthorized)
		return http.StatusUnauthorized
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error(ctx, "read webhook request body failed", err)
		w.WriteHeader(http.StatusBadRequest)
		return http.StatusBadRequest
	}

	slog.InfoContext(ctx, "=== processing webhook start ===")
	slog.InfoContext(
		ctx,
		"received message from telegram webhook",
		"message", string(body),
	)
	defer slog.InfoContext(ctx, "=== processing webhook finished ===")

	var update tgbotapi.Update
	if err := json.Unmarshal(body, &update); err != nil {
		logger.Error(ctx, "decode webhook update failed", err)
		w.WriteHeader(http.StatusBadRequest)
		return http.StatusBadRequest
	}

	if update.Message == nil {
		return http.StatusOK
	}

	switch update.Message.Text {
	case "/hello":
		replyJSON(w, update.Message.Chat.ID, "hello world")
	case "/group":
		resp, err := bk.Group(ctx, &bookkeepingpb.GroupRequest{})
		if err != nil {
			logger.Error(ctx, "query bookkeeping group failed", err)
			// 回應仍是 200（錯誤訊息本身就是要回給使用者的內容），span 不自己標記的話，
			// bookkeeping 掛掉時 Grafana 上每條 trace 都是綠的
			span := trace.SpanFromContext(ctx)
			span.RecordError(err)
			span.SetStatus(codes.Error, "query bookkeeping group failed")
			replyJSON(w, update.Message.Chat.ID, "查詢失敗: "+err.Error())
			return http.StatusOK
		}

		var sb strings.Builder
		for _, g := range resp.Groups {
			fmt.Fprintf(&sb, "%d. %s\n", g.Id, g.Name)
		}
		replyJSON(w, update.Message.Chat.ID, sb.String())
	default:
		replyJSON(w, update.Message.Chat.ID, "unknown command")
	}

	return http.StatusOK
}

/*
 * 若要直接在當次請求回應訊息給使用者，格式必須是 json。
 * Content-Type Header: application/json
 * Body: {
 *   "method": "sendMessage",
 *   "chat_id": <chat_id>,
 *   "text": "<message>"
 * }
 *
 * 刻意不塞進 handle.Sender：這是「回應當次 HTTP 請求」，不是「主動呼叫 API」。
 * 只能送一次、必須在 handler 回傳前送完、chat id 來自當次 update，三個約束 BotSender 都沒有；
 * 而且兩者沒有任何共同呼叫端，統一換不到多型，只換到一個假的共通介面。詳見 monorepo#14。
 */
func replyJSON(w http.ResponseWriter, chatID int64, text string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"method":  "sendMessage",
		"chat_id": chatID,
		"text":    text,
	})
}
