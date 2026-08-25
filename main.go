package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"telegram/config"
	"telegram/handle"

	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	coreconfig "github.com/leo84927/core/config"
	"github.com/leo84927/core/initialize"
	"github.com/leo84927/core/rabbitmq"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	coreconfig.InitFromRedis(ctx, "TELEGRAM")
	coreconfig.ServiceName = coreconfig.EnvMap[env.TelegramEnvKey_TELEGRAM_SERVICE_NAME.String()]
	config.TelegramToken = coreconfig.EnvMap[env.TelegramEnvKey_TELEGRAM_TOKEN.String()]
	config.TelegramChatId = coreconfig.EnvMap[env.TelegramEnvKey_TELEGRAM_CHAT_ID.String()]
	config.WebhookCertPEM = coreconfig.EnvMap[env.TelegramEnvKey_TELEGRAM_WEBHOOK_CERT_PEM.String()]
	config.WebhookKeyPEM = coreconfig.EnvMap[env.TelegramEnvKey_TELEGRAM_WEBHOOK_KEY_PEM.String()]
	config.WebhookPort = coreconfig.EnvMap[env.TelegramEnvKey_TELEGRAM_WEBHOOK_PORT.String()]
	webhookSecret := coreconfig.EnvMap[env.TelegramEnvKey_TELEGRAM_WEBHOOK_SECRET.String()]
	coreconfig.LoadBasicRabbitMQ()
	coreconfig.LoadCompleteTopology(rabbitmq.Queue{
		Name: coreconfig.EnvMap[env.TelegramEnvKey_TELEGRAM_RABBITMQ_QUEUE.String()],
		Keys: []string{
			coreconfig.EnvMap[env.TelegramEnvKey_TELEGRAM_RABBITMQ_KEY.String()],
		},
	})

	bookkeepingConn, err := handle.NewBookkeepingClient(
		coreconfig.EnvMap[env.GlobalEnvKey_GLOBAL_BOOKKEEPING_SOCK_FILE_PATH.String()],
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	webhook := &handle.WebhookServer{
		CertPEM:    config.WebhookCertPEM,
		KeyPEM:     config.WebhookKeyPEM,
		Addr:       config.WebhookPort,
		GrpcClient: bookkeepingConn,
		Secret:     webhookSecret,
	}

	/*
	 * bot 在這裡建立一次，整個 process 共用；建構時的 getMe 順便驗掉 token。
	 * 失敗就讓 process 起不來，交給 systemd 的 Restart=on-failure —— 不在應用層再實作一次 supervisor。
	 * client 的 Timeout 不設：單次嘗試的上限由 BotSender 用 context.WithTimeout 控制。
	 */
	sender, err := handle.NewBotSender(ctx, &http.Client{}, config.TelegramToken, config.TelegramChatId)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	app, err := initialize.New(ctx, &initialize.App{
		MQWorker: initialize.MQWorker{
			MsgHandler: handle.NewTelegramManager(sender).MessageHandler,
		},
		HttpWorker: initialize.HttpWorker{
			WebhookServer: webhook.Run,
		},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer app.Close(ctx)

	app.Run(ctx)
}
