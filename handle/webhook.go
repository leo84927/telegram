package handle

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"telegram/router"

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
		return fmt.Errorf("load webhook certificate failed: %w", err)
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
		return fmt.Errorf("failed to create TLS listener: %w", err)
	}

	// Serve 只接受 net.Listener，TLS 或 plain HTTP 都可以
	err = server.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return err
}
