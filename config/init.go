package config

import (
	cp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/consul"
	coreconfig "github.com/leo84927/core/config"
	"github.com/leo84927/core/rabbitmq"
)

func init() {
	coreconfig.InitFromConsul("TELEGRAM")

	coreconfig.ServiceName = coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_SERVICE_NAME.String()]
	TelegramToken = coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_TOKEN.String()]
	TelegramChatId = coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_CHAT_ID.String()]

	coreconfig.LoadBasicRabbitMQ()
	coreconfig.LoadCompleteTopology(rabbitmq.Queue{
		Name: coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_RABBITMQ_QUEUE.String()],
		Keys: []string{
			coreconfig.EnvMap[cp.TelegramEnvKey_TELEGRAM_RABBITMQ_KEY.String()],
		},
	})
}
