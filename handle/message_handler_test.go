package handle

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
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
