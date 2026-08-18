package handle

import (
	"context"
	"log/slog"
	"sync"
	"testing"

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
 * MessageHandler 是用 goroutine 送訊息，那條 goroutine 會在測試結束後才寫日誌，
 * 而 slog.Default() 是全域的，不篩選會讓斷言隨時序浮動
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

// MessageHandler 的日誌必須沿用 consumer 傳進來的 context，否則 Grafana 上的日誌對不到任何一次訊息處理
func TestMessageHandlerLogsCarryTraceContext(t *testing.T) {
	recorder := useRecorder(t)
	ctx, want := ctxWithSpan()

	if _, err := MessageHandler(ctx, rabbitmq.Message{Body: []byte(`{}`)}, nil); err != nil {
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

// 訊息格式化失敗的錯誤日誌同樣要能對回同一條 trace
func TestBuildMessageErrorLogCarriesTraceContext(t *testing.T) {
	recorder := useRecorder(t)
	ctx, want := ctxWithSpan()

	if _, err := buildMessage(ctx, []byte("not a json")); err == nil {
		t.Fatal("buildMessage() error = nil, 期望解析失敗")
	}

	const message = "build message unmarshal envelope json failed"
	assertSpan(t, recorder.entriesFor(message), message, want)
}
