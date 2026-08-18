package handle

import (
	"context"
	"log/slog"

	"github.com/leo84927/core/rabbitmq"
)

func MessageHandler(ctx context.Context, msg rabbitmq.Message, publisher rabbitmq.PublishHandler) (requeue bool, err error) {
	// 日誌要帶 ctx 才會有 trace_id / span_id（見 CLAUDE.md 的「日誌與 trace 關聯」）
	slog.InfoContext(ctx, "=== processing message start ===")
	slog.InfoContext(
		ctx,
		"received message from RabbitMQ",
		"message", msg.Body,
	)
	defer slog.InfoContext(ctx, "=== processing message finished ===")

	// send message to telegram
	go NewTelegramManager().sendMessage(ctx, msg.Body)

	return false, nil
}
