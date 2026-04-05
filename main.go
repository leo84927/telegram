package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"telegram/config"
	"telegram/handle"
	"time"

	"github.com/leo84927/core/logger"
	"github.com/leo84927/core/rabbitmq"
	"github.com/rotisserie/eris"
	"golang.org/x/sync/errgroup"
)

func init() {
	// 啟動時先清理，防止上次異常結束殘留
	os.Remove("/tmp/ready")
}

// 包裝 errgroup，就可以不用每個 goroutine 都宣告 defer recover
func graceful(g *errgroup.Group, fn func() error) {
	g.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("recovered: %v\n%s", r, debug.Stack())
				log.Println(err)
				slog.Error(
					"panic happened",
					"error", eris.ToJSON(err, true),
				)
			}
		}()

		return fn()
	})
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lm := logger.NewLogManager(&logger.Config{
		ServiceName: config.ServiceName,
		Host:        config.AlloyHost,
		Port:        config.AlloyPort,
	})
	err := lm.SetLogger(ctx)
	if err != nil {
		log.Fatalf("SetLogger failed, err: %v\n", err)
	}
	defer lm.CloseLogger(ctx)

	cm := rabbitmq.NewConnectionManager(config.GetRabbitMQConfig().Config)
	defer cm.Close()

	connReady := make(chan struct{})
	consumer := cm.NewConsumer(config.GetRabbitMQConfig().TelegramQueue.Name, "", 5, 20*time.Second)

	group, groupCtx := errgroup.WithContext(ctx)

	graceful(group, func() error {
		// 建立連線＆拓樸
		if err := cm.InitTopology(config.GetRabbitMQConfig().Topology); err != nil {
			return err
		}

		// 建立 ready 檔案用來做 health check
		if err := os.WriteFile("/tmp/ready", []byte("ok"), 0644); err != nil {
			return err
		}

		// 連線就緒，關閉 connReady channel，讓 consumer 知道可以啟動了
		close(connReady)

		slog.Info("rabbitmq connection and topology ready")
		return cm.WatchConnAndRetry(groupCtx)
	})

	// 拓樸建立後才能訂閱 queue 並常駐 consumer
	graceful(group, func() error {
		<-connReady

		slog.Info("consumer start")
		return consumer.WaitForConsume(groupCtx, handle.MessageHandler)
	})

	// 定期發送心跳給 consul
	graceful(group, func() error {
		return config.Client.SendHeartbeat(groupCtx, config.ServiceName, 60*time.Second)
	})

	// 等待所有 goroutine 結束
	if err := group.Wait(); err != nil {
		slog.Error(
			"shutdown with err",
			"error", eris.ToJSON(err, true),
		)
		return
	}

	slog.Info("normal shutdown")
}
