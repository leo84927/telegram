package router

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	bookkeepingpb "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/bookkeeping"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

func TestAuthorizedSecret(t *testing.T) {
	tests := []struct {
		name   string
		secret string
		got    string
		want   bool
	}{
		{"未設定則放行", "", "anything", true},
		{"未設定且空 header 也放行", "", "", true},
		{"相符放行", "s3cret", "s3cret", true},
		{"不符擋下", "s3cret", "wrong", false},
		{"缺 header 擋下", "s3cret", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := authorizedSecret(tt.got, tt.secret); got != tt.want {
				t.Errorf("authorizedSecret(%q) with secret %q = %v, want %v",
					tt.got, tt.secret, got, tt.want)
			}
		})
	}
}

/*
 * ctxRecorder 把每則日誌當下 context 裡的 span 留下來
 * otelslog 是從 context 取 span 寫出 trace_id / span_id，所以驗 span context 有效即等同驗日誌帶得到 trace
 */
type ctxRecorder struct {
	mu      sync.Mutex
	entries []recordedEntry
}

type recordedEntry struct {
	message     string
	spanContext trace.SpanContext
}

func (r *ctxRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *ctxRecorder) Handle(ctx context.Context, record slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, recordedEntry{
		message:     record.Message,
		spanContext: trace.SpanContextFromContext(ctx),
	})
	return nil
}

func (r *ctxRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *ctxRecorder) WithGroup(string) slog.Handler      { return r }

func (r *ctxRecorder) snapshot() []recordedEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedEntry(nil), r.entries...)
}

/*
 * recordSpans 把全域 tracer provider 換成記憶體版本
 * 這裡刻意不做「測試結束後還原」：otel 的全域 provider 是靠 sync.Once 綁定委派對象的，
 * 把舊值設回去並不會解除委派，還原只是假象。因此每個會碰到 span 的測試都必須自己呼叫這個函式，
 * 否則它的 span 會落進前一個測試的 recorder，斷言變成空跑
 */
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))

	return recorder
}

// recordLogs 把 slog 預設 handler 換成記憶體版本，測試結束後還原
func recordLogs(t *testing.T) *ctxRecorder {
	t.Helper()

	recorder := &ctxRecorder{}
	original := slog.Default()
	slog.SetDefault(slog.New(recorder))
	t.Cleanup(func() { slog.SetDefault(original) })

	return recorder
}

func attrs(span sdktrace.ReadOnlySpan) map[attribute.Key]attribute.Value {
	got := make(map[attribute.Key]attribute.Value)
	for _, kv := range span.Attributes() {
		got[kv.Key] = kv.Value
	}
	return got
}

func post(t *testing.T, secret, path, body string, header map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	for k, v := range header {
		req.Header.Set(k, v)
	}

	w := httptest.NewRecorder()
	New(nil, secret).ServeHTTP(w, req)

	return w
}

func TestWebhookRequestRecordsSpan(t *testing.T) {
	spans := recordSpans(t)

	post(t, "", "/webhook", `{"message":{"chat":{"id":1},"text":"/hello"}}`, nil)

	ended := spans.Ended()
	if len(ended) != 1 {
		t.Fatalf("span 數量 = %d, want 1", len(ended))
	}

	span := ended[0]
	if span.Name() != "POST /webhook" {
		t.Errorf("span 名稱 = %q, want %q", span.Name(), "POST /webhook")
	}
	if span.SpanKind() != trace.SpanKindServer {
		t.Errorf("span kind = %v, want %v", span.SpanKind(), trace.SpanKindServer)
	}

	got := attrs(span)
	if v := got[semconv.URLPathKey]; v.AsString() != "/webhook" {
		t.Errorf("url.path = %q, want %q", v.AsString(), "/webhook")
	}
	if v := got[semconv.HTTPRequestMethodKey]; v.AsString() != http.MethodPost {
		t.Errorf("http.request.method = %q, want %q", v.AsString(), http.MethodPost)
	}
	if v := got[semconv.HTTPResponseStatusCodeKey]; v.AsInt64() != http.StatusOK {
		t.Errorf("http.response.status_code = %d, want %d", v.AsInt64(), http.StatusOK)
	}
}

func TestRejectedWebhookRequestRecordsStatusOnSpan(t *testing.T) {
	spans := recordSpans(t)

	w := post(t, "s3cret", "/webhook", `{}`, map[string]string{"X-Telegram-Bot-Api-Secret-Token": "wrong"})
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("回應狀態 = %d, want %d", w.Code, http.StatusUnauthorized)
	}

	ended := spans.Ended()
	if len(ended) != 1 {
		t.Fatalf("span 數量 = %d, want 1", len(ended))
	}

	if v := attrs(ended[0])[semconv.HTTPResponseStatusCodeKey]; v.AsInt64() != http.StatusUnauthorized {
		t.Errorf("http.response.status_code = %d, want %d", v.AsInt64(), http.StatusUnauthorized)
	}
}

// health 是給部署健康檢查用的高頻端點，不該灌爆 trace
func TestHealthRequestRecordsNoSpan(t *testing.T) {
	spans := recordSpans(t)

	post(t, "", "/health", "", nil)

	if ended := spans.Ended(); len(ended) != 0 {
		t.Errorf("span 數量 = %d, want 0", len(ended))
	}
}

func TestWebhookLogsCarryTraceContext(t *testing.T) {
	recordSpans(t)
	logs := recordLogs(t)

	post(t, "", "/webhook", `{"message":{"chat":{"id":1},"text":"/hello"}}`, nil)

	entries := logs.snapshot()
	if len(entries) == 0 {
		t.Fatal("沒有捕捉到任何日誌")
	}
	for _, entry := range entries {
		if !entry.spanContext.IsValid() {
			t.Errorf("日誌 %q 沒有帶到 span context", entry.message)
		}
	}
}

// 拒絕未授權請求正是需要追查來源的場合，日誌一樣要帶 trace
func TestRejectedWebhookLogCarriesTraceContext(t *testing.T) {
	recordSpans(t)
	logs := recordLogs(t)

	post(t, "s3cret", "/webhook", `{}`, map[string]string{"X-Telegram-Bot-Api-Secret-Token": "wrong"})

	entries := logs.snapshot()
	if len(entries) != 1 {
		t.Fatalf("日誌數量 = %d, want 1", len(entries))
	}
	if !entries[0].spanContext.IsValid() {
		t.Errorf("日誌 %q 沒有帶到 span context", entries[0].message)
	}
}

// 解析失敗的錯誤日誌走 core 的 logger.Error，一樣要落在 span 裡
func TestWebhookDecodeErrorLogCarriesTraceContext(t *testing.T) {
	recordSpans(t)
	logs := recordLogs(t)

	w := post(t, "", "/webhook", `not json`, nil)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("回應狀態 = %d, want %d", w.Code, http.StatusBadRequest)
	}

	for _, entry := range logs.snapshot() {
		if !entry.spanContext.IsValid() {
			t.Errorf("日誌 %q 沒有帶到 span context", entry.message)
		}
	}
}

// stubBookkeeping 讓 /group 的失敗分支可以在不連 bookkeeping 的情況下測
type stubBookkeeping struct {
	err error
}

func (s stubBookkeeping) Hello(context.Context, *bookkeepingpb.HelloRequest, ...grpc.CallOption) (*bookkeepingpb.HelloResponse, error) {
	return nil, s.err
}

func (s stubBookkeeping) Group(context.Context, *bookkeepingpb.GroupRequest, ...grpc.CallOption) (*bookkeepingpb.GroupResponse, error) {
	return nil, s.err
}

// bookkeeping 掛掉時回應仍是 200，span 沒自己標記的話 Grafana 上會看不出這是失敗
func TestBookkeepingFailureMarksSpanAsError(t *testing.T) {
	spans := recordSpans(t)

	body := `{"message":{"chat":{"id":1},"text":"/group"}}`
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(body))
	handler := instrument(func(w http.ResponseWriter, r *http.Request) int {
		return webhook(w, r, stubBookkeeping{err: errors.New("connection refused")}, "")
	})
	handler.ServeHTTP(httptest.NewRecorder(), req)

	ended := spans.Ended()
	if len(ended) != 1 {
		t.Fatalf("span 數量 = %d, want 1", len(ended))
	}

	span := ended[0]
	if span.Status().Code != codes.Error {
		t.Errorf("span 狀態 = %v, want %v", span.Status().Code, codes.Error)
	}
	if len(span.Events()) == 0 {
		t.Error("span 沒有記錄 exception event")
	}
}

// handler panic 時 net/http 會自己 recover，status 不會被賦值，span 必須自己補記
func TestPanickingHandlerMarksSpanAsError(t *testing.T) {
	spans := recordSpans(t)

	handler := instrument(func(http.ResponseWriter, *http.Request) int {
		panic("boom")
	})

	func() {
		// net/http 在連線層做的事，測試裡自己做一次
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Error("panic 沒有往上拋，既有行為被改掉了")
			}
		}()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/webhook", nil))
	}()

	ended := spans.Ended()
	if len(ended) != 1 {
		t.Fatalf("span 數量 = %d, want 1", len(ended))
	}

	span := ended[0]
	if span.Status().Code != codes.Error {
		t.Errorf("span 狀態 = %v, want %v", span.Status().Code, codes.Error)
	}
	if v := attrs(span)[semconv.HTTPResponseStatusCodeKey]; v.AsInt64() != http.StatusInternalServerError {
		t.Errorf("http.response.status_code = %d, want %d", v.AsInt64(), http.StatusInternalServerError)
	}
}
