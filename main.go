package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"telegram/config"
	"telegram/handle"

	cp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/consul"
	coreconfig "github.com/leo84927/core/config"
	"github.com/leo84927/core/initialize"
	"github.com/leo84927/core/rabbitmq"
)

func init() {
	// 啟動時先清理，防止上次異常結束殘留
	if err := os.Remove("/tmp/ready"); err != nil && !os.IsNotExist(err) {
		panic(err)
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	coreconfig.InitFromRedis(ctx, "TELEGRAM")
	coreconfig.ServiceName = coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_SERVICE_NAME.String()]
	config.TelegramToken = coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_TOKEN.String()]
	config.TelegramChatId = coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_CHAT_ID.String()]
	coreconfig.LoadBasicRabbitMQ()
	coreconfig.LoadCompleteTopology(rabbitmq.Queue{
		Name: coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_RABBITMQ_QUEUE.String()],
		Keys: []string{
			coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_RABBITMQ_KEY.String()],
		},
	})

	app, err := initialize.New(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer app.Close(ctx)

	app.Workers = []func(ctx context.Context) error{
		func(ctx context.Context) error {
			return app.Consumer.WaitForConsume(ctx, handle.MessageHandler)
		},
	}
	app.Run(ctx)
}
