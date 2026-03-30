package handle

import (
	"context"
	"log"

	"github.com/leo84927/core/rabbitmq"
)

func MessageHandler(ctx context.Context, msg rabbitmq.Message, publisher rabbitmq.PublishHandler) (requeue bool, err error) {
	log.Printf("=== Start processing message ===")
	log.Printf("Message body: %s", msg.Body)
	defer log.Printf("=== End processing message ===")

	// send message to telegram
	go NewTelegramManager().sendMessage(msg.Body)

	return false, nil
}
