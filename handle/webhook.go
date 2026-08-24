package handle

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"telegram/router"

	"github.com/rotisserie/eris"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 建立 Webhook(HTTPS) Server 所需的參數
type WebhookServer struct {
	CertPEM    string // PEM 格式的憑證
	KeyPEM     string // PEM 格式的私鑰
	Addr       string // 監聽的地址，例如 ":8443"
	GrpcClient *grpc.ClientConn
	Secret     string // telegram webhook 的 secret token，空字串代表不驗
}

/*
 * NewBookkeepingClient 建立連往 bookkeeping 的 gRPC client
 * StatsHandler 是這條同步呼叫在 trace 上不斷開的唯一關鍵：它把 webhook span 的 traceparent
 * 注入 gRPC metadata，bookkeeping 端才接得上同一條 trace（見 CLAUDE.md 的「日誌與 trace 關聯」）
 */
func NewBookkeepingClient(sockFilePath string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		"unix://"+sockFilePath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, eris.Wrap(err, "new bookkeeping grpc client failed")
	}

	return conn, nil
}

// 啟動 Webhook(HTTPS) Server
func (ws *WebhookServer) Run(ctx context.Context) error {
	// 解析憑證和私鑰
	cert, err := tls.X509KeyPair([]byte(ws.CertPEM), []byte(ws.KeyPEM))
	if err != nil {
		return eris.Wrap(err, "load webhook certificate failed")
	}

	// 建立 Webhook(HTTPS) Server，並使用自訂的 router
	server := &http.Server{
		Addr:    ws.Addr,
		Handler: router.New(ws.GrpcClient, ws.Secret),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
	}

	// 在 context 被取消時關閉 server
	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	// 手動建立 TLS listener
	ln, err := tls.Listen("tcp", ws.Addr, server.TLSConfig)
	if err != nil {
		return eris.Wrap(err, "failed to create TLS listener")
	}

	// Serve 只接受 net.Listener，TLS 或 plain HTTP 都可以
	err = server.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return err
}
