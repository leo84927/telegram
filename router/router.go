package router

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
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

/*
 * Sender 在這裡「又」宣告了一次。
 *
 * 【粗胚發現 1／4：介面放不進同一個 package】handle/webhook.go import telegram/router，
 * 所以 router 不能 import handle —— handle.Sender 這個型別 webhook 這一側拿不到。
 * 要真的共用一個 Sender 型別，只有三條路：
 *   (a) 新開一個 leaf package（例如 telegram/telegram）放 Format + Sender，兩邊都 import；
 *   (b) 把 Sender 放進 router，讓 handle import 它 —— 領域型別住在最外層的傳輸層，方向是反的；
 *   (c) 就像現在這樣各宣告一次，靠 Go 的結構化型別各自滿足 —— 那「同一個介面」是假的。
 */
type Sender interface {
	Send(ctx context.Context, text string) error
}

/*
 * replySender 是本張要驗的東西：把「用當次 HTTP 回應回話」硬塞進 Sender 的形狀。
 *
 * 【粗胚發現 2／4：三個約束在介面上完全看不見】
 *   1. 只能送一次 —— 送第二次會在 body 拼出兩個 JSON 物件，telegram 會忽略整個回應
 *   2. 必須在 handler 回傳前送完 —— handler 一回傳，ResponseWriter 就不能再寫
 *   3. chat id 來自當次 update，不是設定
 * BotSender 三個都沒有：它可以送任意次、任意時候、chat id 來自設定。
 *
 * 【粗胚發現 3／4：error 的語意不同】這裡回 nil 只代表 bytes 寫進了 socket，
 * 不代表 telegram 收到；BotSender 回 nil 代表 Bot API 回了 200 OK。
 * 同一個 error 位置，一邊是「投遞成功」，一邊是「寫檔成功」。
 * 因此重試在這一側是無意義的（連線已斷就是斷了），只有 BotSender 那側需要重試。
 */
type replySender struct {
	w      http.ResponseWriter
	chatID int64
	sent   bool
}

func (s *replySender) Send(_ context.Context, text string) error {
	// 這行就是「歪」的證據：Sender 表達不出「只能呼叫一次」，只能在實作裡自己防守
	if s.sent {
		return errors.New("webhook reply already sent")
	}
	s.sent = true

	/*
	 * 若要直接在當次請求回應訊息給使用者，格式必須是 json。
	 * Content-Type Header: application/json
	 * Body: {"method": "sendMessage", "chat_id": <chat_id>, "text": "<message>"}
	 */
	s.w.Header().Set("Content-Type", "application/json")

	return json.NewEncoder(s.w).Encode(map[string]any{
		"method":  "sendMessage",
		"chat_id": s.chatID,
		"text":    text,
	})
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

	/*
	 * 【粗胚發現 4／4：這一側的 Sender 沒有任何注入價值】Sender 在這裡拿不到任何注入的好處：
	 * 它是從 ResponseWriter 當場組出來的，不是外面傳進來的。測試要替換它？
	 * httptest.ResponseRecorder 本來就已經是替身了。
	 * 也就是說這一側的 Sender 既沒有多型（呼叫端靜態決定），也沒有測試替換價值。
	 */
	var reply Sender = &replySender{w: w, chatID: update.Message.Chat.ID}

	switch update.Message.Text {
	case "/hello":
		send(ctx, reply, "hello world")
	case "/group":
		resp, err := bk.Group(ctx, &bookkeepingpb.GroupRequest{})
		if err != nil {
			logger.Error(ctx, "query bookkeeping group failed", err)
			// 回應仍是 200（錯誤訊息本身就是要回給使用者的內容），span 不自己標記的話，
			// bookkeeping 掛掉時 Grafana 上每條 trace 都是綠的
			span := trace.SpanFromContext(ctx)
			span.RecordError(err)
			span.SetStatus(codes.Error, "query bookkeeping group failed")
			send(ctx, reply, "查詢失敗: "+err.Error())
			return http.StatusOK
		}

		var sb strings.Builder
		for _, g := range resp.Groups {
			fmt.Fprintf(&sb, "%d. %s\n", g.Id, g.Name)
		}
		send(ctx, reply, sb.String())
	default:
		send(ctx, reply, "unknown command")
	}

	return http.StatusOK
}

/*
 * send 把 Sender 回傳的 error 吞掉再記一筆日誌。
 *
 * 這個小函式是「歪」的第二個證據：把 replyJSON 換成回傳 error 的 Sender 之後，
 * 四個呼叫點都多了一個無法處置的 error —— 唯一可能的原因是 client 斷線，
 * 而 client 就是 telegram 的伺服器，斷線時我們已經無路可走。原本的 replyJSON 不回傳 error
 * 反而是誠實的。錯誤往上回傳的價值只在 BotSender 那一側成立。
 */
func send(ctx context.Context, sender Sender, text string) {
	if err := sender.Send(ctx, text); err != nil {
		logger.Error(ctx, "write webhook reply failed", err)
	}
}
