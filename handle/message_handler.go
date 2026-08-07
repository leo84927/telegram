package handle

import (
	"context"
	"log/slog"

	"github.com/leo84927/core/rabbitmq"
)

func MessageHandler(ctx context.Context, msg rabbitmq.Message, publisher rabbitmq.PublishHandler) (requeue bool, err error) {
	slog.Info("=== processing message start ===")
	slog.Info(
		"received message from RabbitMQ",
		"message", msg.Body,
	)
	defer slog.Info("=== processing message finished ===")

	// send message to telegram
	go NewTelegramManager().sendMessage(ctx, msg.Body)

	return false, nil
}
