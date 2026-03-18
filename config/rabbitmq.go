package config

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/leo84927/rabbitmq/v2"
)

var rabbitmqCfg RabbitMQ

type RabbitMQ struct {
	Config        *rabbitmq.Config
	Topology      rabbitmq.Topology
	TelegramQueue rabbitmq.Queue
}

func LoadRabbitMQ() {
	connMaxRetries, err := strconv.Atoi(os.Getenv("RABBITMQ_CONN_MAX_RETRIES"))
	if err != nil {
		log.Printf("Transfer RABBITMQ_CONN_MAX_RETRIES failed, invalid value: %v, error: %v\n", os.Getenv("RABBITMQ_CONN_MAX_RETRIES"), err)
		connMaxRetries = 5
	}
	connMaxElapsed, err := time.ParseDuration(os.Getenv("RABBITMQ_CONN_MAX_ELAPSED_TIME"))
	if err != nil {
		log.Printf("Transfer RABBITMQ_CONN_MAX_ELAPSED_TIME failed, invalid value: %v, error: %v\n", os.Getenv("RABBITMQ_CONN_MAX_ELAPSED_TIME"), err)
		connMaxElapsed = 20 * time.Second
	}
	topologyMaxRetries, err := strconv.Atoi(os.Getenv("RABBITMQ_TOPOLOGY_MAX_RETRIES"))
	if err != nil {
		log.Printf("Transfer RABBITMQ_TOPOLOGY_MAX_RETRIES failed, invalid value: %v, error: %v\n", os.Getenv("RABBITMQ_TOPOLOGY_MAX_RETRIES"), err)
		topologyMaxRetries = 3
	}
	topologyMaxElapsed, err := time.ParseDuration(os.Getenv("RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME"))
	if err != nil {
		log.Printf("Transfer RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME failed, invalid value: %v, error: %v\n", os.Getenv("RABBITMQ_TOPOLOGY_MAX_ELAPSED_TIME"), err)
		topologyMaxElapsed = 20 * time.Second
	}

	telegramQueue := rabbitmq.Queue{
		Name: os.Getenv("RABBITMQ_TELEGRAM_QUEUE"),
		Keys: []string{
			os.Getenv("RABBITMQ_TELEGRAM_KEY"),
		},
	}
	rabbitmqCfg = RabbitMQ{
		Config: &rabbitmq.Config{
			ServiceName:    ServiceName(),
			User:           os.Getenv("RABBITMQ_USER"),
			Password:       os.Getenv("RABBITMQ_PASSWORD"),
			Host:           os.Getenv("RABBITMQ_HOST"),
			Port:           os.Getenv("RABBITMQ_PORT"),
			Vhost:          os.Getenv("RABBITMQ_VHOST"),
			MaxRetries:     uint(connMaxRetries),
			MaxElpasedTime: connMaxElapsed,
		},
		Topology: rabbitmq.Topology{
			Exchange: rabbitmq.Exchange{
				Name: os.Getenv("RABBITMQ_JOB_EXCHANGE"),
				Kind: os.Getenv("RABBITMQ_EXCHANGE_KIND"),
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
