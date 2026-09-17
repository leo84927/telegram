package handle

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/rotisserie/eris"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/leo84927/core/v2/logger"
	"telegram/config"
	"telegram/router"
)

// 關機時等進行中的 webhook 請求做完的上限
const shutdownTimeout = 5 * time.Second

// 建立 Webhook(HTTPS) Server 所需的參數
type WebhookServer struct {
	certPEM    string // PEM 格式的憑證
	keyPEM     string // PEM 格式的私鑰
	addr       string // 監聽的地址，例如 ":8443"
	secret     string // 驗證 telegram 來源的 secret token，空字串代表不驗
	grpcClient *grpc.ClientConn
}

func NewWebhookServer(cfg config.Config, grpcClient *grpc.ClientConn) *WebhookServer {
	return &WebhookServer{
		certPEM:    cfg.WebhookCertPEM,
		keyPEM:     cfg.WebhookKeyPEM,
		addr:       cfg.WebhookPort,
		secret:     cfg.WebhookSecret,
		grpcClient: grpcClient,
	}
}

/*
 * 建立連往 bookkeeping 的 gRPC client
 * StatsHandler 把 webhook span 的 traceparent 注入 gRPC metadata，bookkeeping 端才接得上同一條 trace
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
	cert, err := tls.X509KeyPair([]byte(ws.certPEM), []byte(ws.keyPEM))
	if err != nil {
		return eris.Wrap(err, "load webhook certificate failed")
	}

	server := ws.newServer(ctx, cert)

	// 在 context 被取消時關閉 server
	go func() {
		<-ctx.Done()

		// 不沿用 ctx —— 它已經取消了，Shutdown 會立刻放棄等待，進行中的請求當場斷線。
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error(ctx, "webhook server shutdown failed", err)
		}
	}()

	// 手動建立 TLS listener
	ln, err := tls.Listen("tcp", ws.addr, server.TLSConfig)
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

/*
 * 建立 Webhook(HTTPS) Server，並使用自訂的 router
 *
 * BaseContext 是每一則進來的請求的 ctx 根源。不接上的話，handler 手上的 ctx 與關機訊號毫無關係 ——
 * SIGTERM 砍不到進行中的請求，它們只會一路跑到 Shutdown 的上限為止。
 *
 * 接上之後，關機時進行中的請求隨 ctx 被砍，靠 Telegram 自己的重送補回。
 */
func (ws *WebhookServer) newServer(ctx context.Context, cert tls.Certificate) *http.Server {
	return &http.Server{
		Addr:        ws.addr,
		Handler:     router.New(ws.grpcClient, ws.secret),
		BaseContext: func(net.Listener) context.Context { return ctx },
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
	}
}
