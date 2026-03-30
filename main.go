package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"telegram/config"
	"telegram/handle"
	"time"

	"github.com/leo84927/core/rabbitmq"
	"golang.org/x/sync/errgroup"
)

func init() {
	// 啟動時先清理，防止上次異常結束殘留
	os.Remove("/tmp/ready")
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cm := rabbitmq.NewConnectionManager(config.GetRabbitMQConfig().Config)
	defer cm.Close()

	connReady := make(chan struct{})
	consumer := cm.NewConsumer(config.GetRabbitMQConfig().TelegramQueue.Name, "", 5, 20*time.Second)

	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		// 建立連線＆拓樸
		if err := cm.InitTopology(config.GetRabbitMQConfig().Topology); err != nil {
			return err
		}

		// 建立 ready 檔案用來做 health check
		if err := os.WriteFile("/tmp/ready", []byte("ok"), 0644); err != nil {
			return err
		}

		// 連線就緒
		log.Println("RabbitMQ connection and topology ready")
		close(connReady)

		return cm.WatchConnAndRetry(groupCtx)
	})

	// 拓樸建立後才能訂閱 queue 並常駐 consumer
	group.Go(func() error {
		<-connReady
		return consumer.WaitForConsume(groupCtx, handle.MessageHandler)
	})

	// 等待所有 goroutine 結束
	if err := group.Wait(); err != nil {
		log.Println("exit with error:", err)
		return
	}

	log.Println("normal shutdown")
}
