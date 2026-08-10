package handle

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"telegram/router"

	"github.com/rotisserie/eris"
	"google.golang.org/grpc"
)

type WebhookServer struct {
	CertPEM    string
	KeyPEM     string
	Addr       string
	GrpcClient *grpc.ClientConn
}

func (ws *WebhookServer) Run(ctx context.Context) error {
	cert, err := tls.X509KeyPair([]byte(ws.CertPEM), []byte(ws.KeyPEM))
	if err != nil {
		// 用 eris.Wrap 而非 fmt.Errorf：非 eris 的最外層會讓 logger 的 exception.stacktrace 找不到堆疊
		return eris.Wrap(err, "load webhook certificate failed")
	}

	server := &http.Server{
		Addr:    ws.Addr,
		Handler: router.New(ws.GrpcClient),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
	}

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
