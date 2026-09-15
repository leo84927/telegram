package handle

import (
	"context"
	"errors"
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

// entriesFor 只取指定訊息的日誌，其餘（例如 core 記的錯誤）不進斷言
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

/*
 * senderStub 是 Sender 唯一用得上替身的場合：這幾條測試觀察的是日誌與回傳值，
 * 一次真實的 Bot API 往返在這裡看不出任何東西。
 */
type senderStub struct {
	texts []string
	err   error
}

func (s *senderStub) Send(_ context.Context, text string) error {
	s.texts = append(s.texts, text)
	return s.err
}

// Handle 的日誌必須沿用 consumer 傳進來的 context，否則 Grafana 上的日誌對不到任何一次訊息處理
func TestHandleLogsCarryTraceContext(t *testing.T) {
	recorder := useRecorder(t)
	ctx, want := ctxWithSpan()

	worker := NewWorker(&senderStub{})
	if err := worker.Handle(ctx, rabbitmq.Message{Body: []byte(`{}`)}, nil); err != nil {
		t.Fatalf("Handle() error = %v, 期望 nil", err)
	}

	for _, message := range []string{
		"=== processing message start ===",
		"received message from RabbitMQ",
		"=== processing message finished ===",
	} {
		assertSpan(t, recorder.entriesFor(message), message, want)
	}
}

// 解不出 Envelope 就沒有可送的對象，回 error 讓 core 否認這則訊息
func TestHandleRejectsUnparsableEnvelope(t *testing.T) {
	sender := &senderStub{}
	worker := NewWorker(sender)

	if err := worker.Handle(context.Background(), rabbitmq.Message{Body: []byte("not a json")}, nil); err == nil {
		t.Fatal("Handle() error = nil, 期望解析失敗")
	}

	if len(sender.texts) != 0 {
		t.Errorf("送出 %d 則訊息, 期望 0 則", len(sender.texts))
	}
}

// 送出失敗的錯誤要原樣往上傳，core 才會把 span 標為 Error 並否認訊息
func TestHandleReturnsSenderError(t *testing.T) {
	want := errors.New("send failed")
	worker := NewWorker(&senderStub{err: want})

	err := worker.Handle(context.Background(), rabbitmq.Message{Body: []byte(`{}`)}, nil)
	if !errors.Is(err, want) {
		t.Fatalf("Handle() error = %v, 期望 %v", err, want)
	}
}
