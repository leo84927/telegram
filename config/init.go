package config

import (
	"log"
	"maps"

	cp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/consul"
	"github.com/leo84927/core/consul"
	"github.com/rotisserie/eris"
)

func init() {
	var err error

	Client, err = consul.NewClient()
	if err != nil {
		log.Fatalf("New consul client failed, err: %v\n", eris.ToJSON(err, true))
	}

	if EnvMap, err = Client.List("GLOBAL"); err != nil {
		log.Fatalf("Get env from consul failed, err: %v\n", eris.ToJSON(err, true))
	}

	if serviceMap, err := Client.List("TELEGRAM"); err != nil {
		log.Fatalf("Get env from consul failed, err: %v\n", eris.ToJSON(err, true))
	} else {
		maps.Copy(EnvMap, serviceMap)
	}

	ServiceName = EnvMap[cp.TelegramEnvKey_TELEGRAM_SERVICE_NAME.String()]
	TelegramToken = EnvMap[cp.TelegramEnvKey_TELEGRAM_TOKEN.String()]
	TelegramChatId = EnvMap[cp.TelegramEnvKey_TELEGRAM_CHAT_ID.String()]
	AlloyHost = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_HOST.String()]
	AlloyPort = EnvMap[cp.GlobalEnvKey_GLOBAL_ALLOY_PORT.String()]

	LoadRabbitMQ()
}
