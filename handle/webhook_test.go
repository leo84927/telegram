package handle

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"buf.build/gen/go/leo84927-proto/scheduler/grpc/go/bookkeeping/bookkeepinggrpc"
	bookkeepingpb "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/bookkeeping"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

/*
 * recordSpans 把全域 tracer provider 換成記憶體版本，並補上 production 由 core 設定的 propagator
 * 少了 propagator，otel 的全域預設是空的 composite，traceparent 根本不會被寫進 gRPC metadata
 * 這裡刻意不做「測試結束後還原」：otel 的全域 provider 是靠 sync.Once 綁定委派對象的，
 * 把舊值設回去並不會解除委派，還原只是假象。因此每個會碰到 span 的測試都必須自己呼叫這個函式
 */
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return recorder
}

// bookkeepingStub 只留下收到的 metadata，讓測試不必真的跑起 bookkeeping
type bookkeepingStub struct {
	bookkeepinggrpc.UnimplementedBookkeepingServiceServer
	mu sync.Mutex
	md metadata.MD
}

func (s *bookkeepingStub) Hello(ctx context.Context, _ *bookkeepingpb.HelloRequest) (*bookkeepingpb.HelloResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	s.mu.Lock()
	s.md = md.Copy()
	s.mu.Unlock()

	return &bookkeepingpb.HelloResponse{Message: "hello world"}, nil
}

func (s *bookkeepingStub) header(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	values := s.md.Get(key)
	if len(values) == 0 {
		return ""
	}

	return values[0]
}

/*
 * serveStub 在 unix socket 上跑一個未 instrument 的 stub server，回傳 socket 路徑
 * 用 os.MkdirTemp 而非 t.TempDir()：後者會把測試名稱塞進路徑，unix socket 有長度上限
 */
func serveStub(t *testing.T) (*bookkeepingStub, string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "bk")
	if err != nil {
		t.Fatalf("建立暫存目錄失敗: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	sockFilePath := filepath.Join(dir, "s.sock")
	lis, err := net.Listen("unix", sockFilePath)
	if err != nil {
		t.Fatalf("監聽 unix socket 失敗: %v", err)
	}

	stub := &bookkeepingStub{}
	server := grpc.NewServer()
	bookkeepinggrpc.RegisterBookkeepingServiceServer(server, stub)

	go server.Serve(lis)
	t.Cleanup(server.Stop)

	return stub, sockFilePath
}

func clientSpan(t *testing.T, spans *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()

	for _, span := range spans.Ended() {
		if span.SpanKind() == trace.SpanKindClient {
			return span
		}
	}

	t.Fatal("沒有產生 SpanKindClient 的 span，gRPC 呼叫端沒有被 instrument")
	return nil
}

/*
 * 呼叫端 span 必須掛在 webhook span 底下，否則 gRPC 呼叫會自成一條 root trace，
 * 在 Grafana 上看不出「從使用者輸入開始」的完整鏈路
 */
func TestBookkeepingCallJoinsCallerTrace(t *testing.T) {
	spans := recordSpans(t)
	_, sockFilePath := serveStub(t)

	conn, err := NewBookkeepingClient(sockFilePath)
	if err != nil {
		t.Fatalf("NewBookkeepingClient() error = %v, 期望 nil", err)
	}
	t.Cleanup(func() { conn.Close() })

	// 模擬 router.instrument 開的 webhook span
	ctx, webhook := otel.Tracer("test").Start(context.Background(), "POST /webhook")
	if _, err := bookkeepinggrpc.NewBookkeepingServiceClient(conn).Hello(ctx, &bookkeepingpb.HelloRequest{}); err != nil {
		t.Fatalf("Hello() error = %v, 期望 nil", err)
	}
	webhook.End()

	span := clientSpan(t, spans)
	want := webhook.SpanContext()
	if span.SpanContext().TraceID() != want.TraceID() {
		t.Errorf("呼叫端 span 的 trace id = %v, 期望 %v", span.SpanContext().TraceID(), want.TraceID())
	}
	if span.Parent().SpanID() != want.SpanID() {
		t.Errorf("呼叫端 span 的 parent span id = %v, 期望 %v", span.Parent().SpanID(), want.SpanID())
	}
}

// bookkeeping 端要接得上同一條 trace，靠的是 metadata 裡的 traceparent
func TestBookkeepingCallInjectsTraceparent(t *testing.T) {
	spans := recordSpans(t)
	stub, sockFilePath := serveStub(t)

	conn, err := NewBookkeepingClient(sockFilePath)
	if err != nil {
		t.Fatalf("NewBookkeepingClient() error = %v, 期望 nil", err)
	}
	t.Cleanup(func() { conn.Close() })

	ctx, webhook := otel.Tracer("test").Start(context.Background(), "POST /webhook")
	if _, err := bookkeepinggrpc.NewBookkeepingServiceClient(conn).Hello(ctx, &bookkeepingpb.HelloRequest{}); err != nil {
		t.Fatalf("Hello() error = %v, 期望 nil", err)
	}
	webhook.End()

	traceparent := stub.header("traceparent")
	if traceparent == "" {
		t.Fatal("gRPC metadata 沒有 traceparent，bookkeeping 端會自成一條新 trace")
	}

	// traceparent 帶的是呼叫端 span 自己的 id，bookkeeping 的 server span 會掛在它底下
	span := clientSpan(t, spans)
	if !strings.Contains(traceparent, span.SpanContext().TraceID().String()) {
		t.Errorf("traceparent = %q, 期望含 trace id %v", traceparent, span.SpanContext().TraceID())
	}
	if !strings.Contains(traceparent, span.SpanContext().SpanID().String()) {
		t.Errorf("traceparent = %q, 期望含呼叫端 span id %v", traceparent, span.SpanContext().SpanID())
	}
}
