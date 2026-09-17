package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	coreconfig "github.com/leo84927/core/v2/config"
	"github.com/leo84927/core/v2/initialize"

	"telegram/config"
	"telegram/handle"
)

const botAPITimeout = 5 * time.Second

func newBotClient() *http.Client {
	return &http.Client{Timeout: botAPITimeout}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// os.Exit 不跑 defer，所以整個啟動流程收在 run 裡，讓 Close 有機會配對執行
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	settings, err := coreconfig.Load(ctx, coreconfig.Spec{
		Prefix:         "TELEGRAM",
		ServiceNameKey: env.TelegramEnvKey_TELEGRAM_SERVICE_NAME,
		Queue: &coreconfig.QueueKeys{
			NameKey:    env.TelegramEnvKey_TELEGRAM_RABBITMQ_QUEUE,
			RoutingKey: env.TelegramEnvKey_TELEGRAM_RABBITMQ_KEY,
		},
		ServiceKeys: []fmt.Stringer{
			env.TelegramEnvKey_TELEGRAM_TOKEN,
			env.TelegramEnvKey_TELEGRAM_CHAT_ID,
			env.TelegramEnvKey_TELEGRAM_WEBHOOK_CERT_PEM,
			env.TelegramEnvKey_TELEGRAM_WEBHOOK_KEY_PEM,
			env.TelegramEnvKey_TELEGRAM_WEBHOOK_PORT,
			env.TelegramEnvKey_TELEGRAM_WEBHOOK_SECRET,
		},
	})
	if err != nil {
		return err
	}

	cfg := config.New(settings.Service)

	sender := handle.NewBotSender(newBotClient(), cfg)
	if err := sender.ValidateToken(ctx); err != nil {
		return err
	}

	bookkeepingConn, err := handle.NewBookkeepingClient(settings.GrpcSockPath)
	if err != nil {
		return err
	}

	// &handle.Worker
	worker := handle.NewWorker(sender)
	webhook := handle.NewWebhookServer(cfg, bookkeepingConn)

	app, err := initialize.New(ctx, settings, &initialize.App{
		MQWorker: initialize.MQWorker{
			MsgHandler: worker.Handle,
		},
		HttpWorker: initialize.HttpWorker{
			WebhookServer: webhook.Run,
		},
	})
	if err != nil {
		return err
	}
	defer app.Close(ctx)

	app.Run(ctx)

	return nil
}
