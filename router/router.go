package router

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"telegram/config"

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

func New(bookkeepingConn *grpc.ClientConn) *http.ServeMux {
	bk := bookkeepinggrpc.NewBookkeepingServiceClient(bookkeepingConn)

	mux := http.NewServeMux()
	// health 不套 instrument：部署健康檢查頻率高，開 span 只會灌爆 trace
	// webhook 則是連未通過 secret 驗證的請求也開 span——那正是需要追查來源的場合（見 CLAUDE.md）
	mux.HandleFunc("POST /health", health)
	mux.Handle("POST /webhook", instrument(func(w http.ResponseWriter, r *http.Request) int {
		return webhook(w, r, bk)
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

		status := handler(w, r.WithContext(ctx))

		span.SetAttributes(semconv.HTTPResponseStatusCode(status))
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
	})
}

// authorizedSecret 比對 Telegram 帶來的 secret token header。
// 未設定 WebhookSecret 時放行，避免設定 secret 前直接中斷既有 webhook。
func authorizedSecret(got string) bool {
	if config.WebhookSecret == "" {
		return true
	}
	// 定時比較，避免 timing attack 洩漏 secret
	return subtle.ConstantTimeCompare([]byte(got), []byte(config.WebhookSecret)) == 1
}

func webhook(w http.ResponseWriter, r *http.Request, bk bookkeepinggrpc.BookkeepingServiceClient) int {
	// 日誌要帶 ctx 才會有 trace_id / span_id（見 CLAUDE.md 的「日誌與 trace 關聯」）
	ctx := r.Context()

	if !authorizedSecret(r.Header.Get("X-Telegram-Bot-Api-Secret-Token")) {
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

func replyJSON(w http.ResponseWriter, chatID int64, text string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"method":  "sendMessage",
		"chat_id": chatID,
		"text":    text,
	})
}
