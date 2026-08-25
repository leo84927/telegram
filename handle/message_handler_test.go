package handle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"github.com/cenkalti/backoff/v5"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/leo84927/core/rabbitmq"
	"go.opentelemetry.io/otel/trace"
)

/*
 * ctxRecorder 把每則日誌連同它拿到的 context 留在記憶體
 * otelslog 是從 context 取 span 寫出 trace_id / span_id，所以只要驗 context 有帶到 span 即可
 */
type ctxRecorder struct {
	mu      sync.Mutex
	entries []recordedEntry
}

type recordedEntry struct {
	message string
	ctx     context.Context
}

func (r *ctxRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *ctxRecorder) Handle(ctx context.Context, record slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, recordedEntry{message: record.Message, ctx: ctx})
	return nil
}

func (r *ctxRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *ctxRecorder) WithGroup(string) slog.Handler      { return r }

/*
 * entriesFor 只取指定訊息的日誌
 * MessageHandler 已改為同步，時序浮動的問題消失了，但 slog.Default() 仍是全域的，
 * 篩選是為了不受其他測試的日誌干擾
 */
func (r *ctxRecorder) entriesFor(message string) []recordedEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	var matched []recordedEntry
	for _, entry := range r.entries {
		if entry.message == message {
			matched = append(matched, entry)
		}
	}

	return matched
}

func assertSpan(t *testing.T, entries []recordedEntry, message string, want trace.SpanContext) {
	t.Helper()

	if len(entries) != 1 {
		t.Fatalf("日誌 %q 筆數 = %d, 期望 1", message, len(entries))
	}

	got := trace.SpanContextFromContext(entries[0].ctx)
	if got.TraceID() != want.TraceID() {
		t.Errorf("日誌 %q 的 trace id = %v, 期望 %v", message, got.TraceID(), want.TraceID())
	}
	if got.SpanID() != want.SpanID() {
		t.Errorf("日誌 %q 的 span id = %v, 期望 %v", message, got.SpanID(), want.SpanID())
	}
}

// useRecorder 讓測試期間的預設 logger 改寫進記憶體，結束後還原
func useRecorder(t *testing.T) *ctxRecorder {
	t.Helper()

	recorder := &ctxRecorder{}
	original := slog.Default()
	slog.SetDefault(slog.New(recorder))
	t.Cleanup(func() { slog.SetDefault(original) })

	return recorder
}

// ctxWithSpan 模擬 core 的 consumer 從 AMQP headers 萃取上游 trace 後交給 handler 的 context
func ctxWithSpan() (context.Context, trace.SpanContext) {
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18},
		TraceFlags: trace.FlagsSampled,
	})

	return trace.ContextWithSpanContext(context.Background(), spanContext), spanContext
}

// stubSender 記下被送出的文字，並可指定送出時的錯誤
type stubSender struct {
	sent []string
	err  error
}

func (s *stubSender) Send(_ context.Context, text string) error {
	s.sent = append(s.sent, text)
	return s.err
}

// MessageHandler 的日誌必須沿用 consumer 傳進來的 context，否則 Grafana 上的日誌對不到任何一次訊息處理
func TestMessageHandlerLogsCarryTraceContext(t *testing.T) {
	recorder := useRecorder(t)
	ctx, want := ctxWithSpan()

	tm := NewTelegramManager(&stubSender{})
	if _, err := tm.MessageHandler(ctx, rabbitmq.Message{Body: []byte(`{}`)}, nil); err != nil {
		t.Fatalf("MessageHandler() error = %v, 期望 nil", err)
	}

	for _, message := range []string{
		"=== processing message start ===",
		"received message from RabbitMQ",
		"=== processing message finished ===",
	} {
		assertSpan(t, recorder.entriesFor(message), message, want)
	}
}

/*
 * 送出失敗必須回傳給呼叫端。
 *
 * 這是整張 ticket 的核心：現況是 go ...sendMessage(ctx, body)，五個失敗點全是
 * logger.Error + return，core 的 consumer 永遠拿到 nil，訊息一律 Ack —— 告警內容就這樣消失。
 */
func TestMessageHandlerReturnsSendError(t *testing.T) {
	sender := &stubSender{err: errors.New("telegram api 503")}

	requeue, err := NewTelegramManager(sender).MessageHandler(
		context.Background(),
		rabbitmq.Message{Body: []byte(`{}`)},
		nil,
	)
	if err == nil {
		t.Fatal("MessageHandler() error = nil, 期望把送出失敗往上回傳")
	}
	if requeue {
		t.Error("requeue = true, 期望 false（#9 已裁定恆為 false）")
	}
}

// 訊息格式化失敗同樣要往上回傳，不能吞掉
func TestMessageHandlerReturnsFormatError(t *testing.T) {
	sender := &stubSender{}

	if _, err := NewTelegramManager(sender).MessageHandler(
		context.Background(),
		rabbitmq.Message{Body: []byte("not a json")},
		nil,
	); err == nil {
		t.Fatal("MessageHandler() error = nil, 期望解析失敗")
	}
	if len(sender.sent) != 0 {
		t.Errorf("送出次數 = %d, 期望 0", len(sender.sent))
	}
}

/*
 * Format 是純函式，測它不需要 context、不需要 logger、不需要 bot。
 * TWD 取倒數到小數三位是整個服務唯一的領域規則，現況只能經由 sendMessage 觸達。
 */
func TestFormat(t *testing.T) {
	tests := []struct {
		name     string
		envelope *mqp.Envelope
		want     string
	}{
		{
			name: "base 是 TWD 時反過來報價並取倒數",
			envelope: &mqp.Envelope{
				Type: mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE,
				Data: `{"base_currency":"TWD","counter_currency":"USD","rate":"0.032"}`,
			},
			want: "USD/TWD : 31.25",
		},
		{
			name: "base 不是 TWD 時照原樣報價",
			envelope: &mqp.Envelope{
				Type: mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE,
				Data: `{"base_currency":"USD","counter_currency":"JPY","rate":"157.2"}`,
			},
			want: "USD/JPY : 157.2",
		},
		{
			name:     "錯誤告警原文照送",
			envelope: &mqp.Envelope{Type: mqp.EnvelopeType_TELEGRAM_ERROR, Data: "query rate failed"},
			want:     "query rate failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Format(tt.envelope)
			if err != nil {
				t.Fatalf("Format() error = %v, 期望 nil", err)
			}
			if got != tt.want {
				t.Errorf("Format() = %q, want %q", got, tt.want)
			}
		})
	}
}

// fakeAPI 起一台假的 Bot API，依序回傳 responses 裡的內容，並記下收到幾次請求
type fakeAPI struct {
	mu        sync.Mutex
	requests  int
	responses []fakeResponse
}

type fakeResponse struct {
	status int
	body   string
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	resp := f.responses[min(f.requests, len(f.responses)-1)]
	f.requests++

	w.WriteHeader(resp.status)
	fmt.Fprint(w, resp.body)
}

func (f *fakeAPI) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

// newTestBotSender 指向假的 Bot API，跳過建構時的 getMe（那條路另外測）
func newTestBotSender(t *testing.T, api *fakeAPI) *BotSender {
	t.Helper()

	server := httptest.NewServer(api)
	t.Cleanup(server.Close)

	return &BotSender{
		client:   server.Client(),
		token:    "test-token",
		chatID:   1,
		endpoint: server.URL + "/bot%s/%s",
	}
}

const okResponse = `{"ok":true,"result":{}}`

// 5xx 是暫時性失敗，必須重試 —— 這是 #9 刻意留給本張的那格
func TestBotSenderRetriesOnServerError(t *testing.T) {
	api := &fakeAPI{responses: []fakeResponse{
		{http.StatusBadGateway, `{"ok":false,"error_code":502,"description":"Bad Gateway"}`},
		{http.StatusOK, okResponse},
	}}

	if err := newTestBotSender(t, api).Send(context.Background(), "hi"); err != nil {
		t.Fatalf("Send() error = %v, 期望重試後成功", err)
	}
	if got := api.count(); got != 2 {
		t.Errorf("請求次數 = %d, 期望 2", got)
	}
}

// 401 是 token 錯，重試三次純屬浪費
func TestBotSenderDoesNotRetryOnClientError(t *testing.T) {
	api := &fakeAPI{responses: []fakeResponse{
		{http.StatusUnauthorized, `{"ok":false,"error_code":401,"description":"Unauthorized"}`},
	}}

	err := newTestBotSender(t, api).Send(context.Background(), "hi")
	if err == nil {
		t.Fatal("Send() error = nil, 期望回傳錯誤")
	}
	if got := api.count(); got != 1 {
		t.Errorf("請求次數 = %d, 期望 1（不可重試的錯誤不該重試）", got)
	}
	// unwrapPermanent 要真的解開，否則 eris 只看得到最外層的 PermanentError，堆疊整條消失
	if permanent := (*backoff.PermanentError)(nil); errors.As(err, &permanent) {
		t.Errorf("錯誤最外層仍是 *backoff.PermanentError: %v", err)
	}
	if !strings.Contains(err.Error(), "Unauthorized") {
		t.Errorf("錯誤訊息 = %q, 期望保留 telegram 的描述", err.Error())
	}
}

// 用完重試次數後仍要回傳錯誤，不能靜靜地當成功
func TestBotSenderReturnsErrorWhenRetriesExhausted(t *testing.T) {
	api := &fakeAPI{responses: []fakeResponse{
		{http.StatusBadGateway, `{"ok":false,"error_code":502,"description":"Bad Gateway"}`},
	}}

	if err := newTestBotSender(t, api).Send(context.Background(), "hi"); err == nil {
		t.Fatal("Send() error = nil, 期望重試用完後回傳錯誤")
	}
	if got := api.count(); got != sendMaxTries {
		t.Errorf("請求次數 = %d, 期望 %d", got, sendMaxTries)
	}
}

// 啟動時的 getMe 就是 token 驗證：失敗就不該讓服務起來
func TestNewBotSenderFailsWhenTokenRejected(t *testing.T) {
	api := &fakeAPI{responses: []fakeResponse{
		{http.StatusUnauthorized, `{"ok":false,"error_code":401,"description":"Unauthorized"}`},
	}}
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)

	sender := &BotSender{client: server.Client(), token: "bad", chatID: 1, endpoint: server.URL + "/bot%s/%s"}
	if _, err := sender.call(context.Background(), "getMe", nil); err == nil {
		t.Fatal("getMe error = nil, 期望 token 被拒時回傳錯誤")
	}
	if got := api.count(); got != 1 {
		t.Errorf("請求次數 = %d, 期望 1（啟動時的 getMe 不重試）", got)
	}
}

/*
 * classify 是錯誤分類的全部決策，直接測它比透過 httptest 快得多。
 * 429 那格特別重要：backoff.RetryAfter 回傳的 *RetryAfterError 不包裝內層錯誤，
 * 包裝方式一旦寫錯，backoff 就找不到它，telegram 給的 retry_after 會被無視。
 */
func TestClassify(t *testing.T) {
	tests := []struct {
		name          string
		resp          *tgbotapi.APIResponse
		wantPermanent bool
		wantRetryFor  time.Duration
	}{
		{
			name:          "429 帶 retry_after 改用 telegram 給的秒數",
			resp:          &tgbotapi.APIResponse{ErrorCode: 429, Description: "Too Many Requests", Parameters: &tgbotapi.ResponseParameters{RetryAfter: 7}},
			wantPermanent: false,
			wantRetryFor:  7 * time.Second,
		},
		{
			name:          "429 沒帶 retry_after 就照一般可重試處理",
			resp:          &tgbotapi.APIResponse{ErrorCode: 429, Description: "Too Many Requests"},
			wantPermanent: false,
		},
		{
			name:          "400 參數錯不重試",
			resp:          &tgbotapi.APIResponse{ErrorCode: 400, Description: "Bad Request"},
			wantPermanent: true,
		},
		{
			name:          "403 被封鎖不重試",
			resp:          &tgbotapi.APIResponse{ErrorCode: 403, Description: "Forbidden"},
			wantPermanent: true,
		},
		{
			name:          "5xx 可重試",
			resp:          &tgbotapi.APIResponse{ErrorCode: 500, Description: "Internal Server Error"},
			wantPermanent: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classify(tt.resp)

			permanent := (*backoff.PermanentError)(nil)
			if got := errors.As(err, &permanent); got != tt.wantPermanent {
				t.Errorf("是否為 Permanent = %v, want %v", got, tt.wantPermanent)
			}

			retryAfter := (*backoff.RetryAfterError)(nil)
			if errors.As(err, &retryAfter) {
				if retryAfter.Duration != tt.wantRetryFor {
					t.Errorf("retry after = %v, want %v", retryAfter.Duration, tt.wantRetryFor)
				}
			} else if tt.wantRetryFor != 0 {
				t.Errorf("錯誤鏈裡找不到 *backoff.RetryAfterError: %v", err)
			}

			if !strings.Contains(err.Error(), tt.resp.Description) {
				t.Errorf("錯誤訊息 = %q, 期望保留 telegram 的描述 %q", err.Error(), tt.resp.Description)
			}
		})
	}
}
