package config

import (
	"log"
	"strconv"
	"time"

	cp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/consul"
	"github.com/leo84927/core/rabbitmq"
)

var rabbitmqCfg RabbitMQ

type RabbitMQ struct {
	Config        *rabbitmq.Config
	Topology      rabbitmq.Topology
	TelegramQueue rabbitmq.Queue
}

func LoadRabbitMQ() {
	connMaxRetries, err := strconv.Atoi(EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_RETRIES.String()])
	if err != nil {
		log.Printf("Transfer RABBITMQ_CONN_MAX_RETRIES failed, invalid value: %v, error: %v\n", EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_RETRIES.String()], err)
		connMaxRetries = 5
	}
	connMaxElapsed, err := time.ParseDuration(EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_ELAPSED_TIME.String()])
	if err != nil {
		log.Printf("Transfer RABBITMQ_CONN_MAX_ELAPSED_TIME failed, invalid value: %v, error: %v\n", EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_CONN_MAX_ELAPSED_TIME.String()], err)
		connMaxElapsed = 20 * time.Second
	}
	topologyMaxRetries, err := strconv.Atoi(EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_RETRIES.String()])
	if err != nil {
		log.Printf("Transfer RABBITMQ_TOPOLOGY_MAX_RETRIES failed, invalid value: %v, error: %v\n", EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_RETRIES.String()], err)
		topologyMaxRetries = 3
	}
	topologyMaxElapsed, err := time.ParseDuration(EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME.String()])
	if err != nil {
		log.Printf("Transfer RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME failed, invalid value: %v, error: %v\n", EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME.String()], err)
		topologyMaxElapsed = 20 * time.Second
	}

	telegramQueue := rabbitmq.Queue{
		Name: EnvMap[cp.TelegramEnvKey_TELEGRAM_RABBITMQ_QUEUE.String()],
		Keys: []string{
			EnvMap[cp.TelegramEnvKey_TELEGRAM_RABBITMQ_KEY.String()],
		},
	}
	rabbitmqCfg = RabbitMQ{
		Config: &rabbitmq.Config{
			ServiceName:    ServiceName,
			User:           EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_USER.String()],
			Password:       EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_PASSWORD.String()],
			Host:           EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_HOST.String()],
			Port:           EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_PORT.String()],
			Vhost:          EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_VHOST.String()],
			MaxRetries:     uint(connMaxRetries),
			MaxElpasedTime: connMaxElapsed,
		},
		Topology: rabbitmq.Topology{
			Exchange: rabbitmq.Exchange{
				Name: EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_JOB_EXCHANGE.String()],
				Kind: EnvMap[cp.GlobalEnvKey_GLOBAL_RABBITMQ_EXCHANGE_KIND.String()],
			},
			Queues: []rabbitmq.Queue{
				telegramQueue,
			},
			MaxRetries:     uint(topologyMaxRetries),
			MaxElpasedTime: topologyMaxElapsed,
		},
		TelegramQueue: telegramQueue,
	}
}

func GetRabbitMQConfig() RabbitMQ {
	return rabbitmqCfg
}
