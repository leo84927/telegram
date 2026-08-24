package handle

import (
	"context"
	"log/slog"

	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"github.com/leo84927/core/rabbitmq"
	"github.com/rotisserie/eris"
	"google.golang.org/protobuf/encoding/protojson"
)

/*
 * MessageHandler 把一則 RabbitMQ 訊息格式化後送去 Telegram。
 *
 * 與現況的三個差別：
 *  1. 同步執行 —— 原本是 go NewTelegramManager().sendMessage(ctx, msg.Body)，
 *     錯誤根本沒有回傳的路徑可走。併發歸 core 的 Consumer（#8 已裁定）。
 *  2. 錯誤往上回傳 —— 原本五個失敗點全是 logger.Error + return，呼叫端拿不到任何訊息。
 *  3. Envelope 的解析留在這一層，Format 只認 Envelope，不認 bytes。
 *
 * requeue 恆為 false（#9 已裁定），這個回傳值等 core 收斂簽名後會消失。
 */
func (tm *TelegramManager) MessageHandler(ctx context.Context, msg rabbitmq.Message, _ rabbitmq.PublishHandler) (requeue bool, err error) {
	// 日誌要帶 ctx 才會有 trace_id / span_id（見 CLAUDE.md 的「日誌與 trace 關聯」）
	slog.InfoContext(ctx, "=== processing message start ===")
	slog.InfoContext(
		ctx,
		"received message from RabbitMQ",
		"message", msg.Body,
	)
	defer slog.InfoContext(ctx, "=== processing message finished ===")

	var envelope mqp.Envelope
	if err := protojson.Unmarshal(msg.Body, &envelope); err != nil {
		return false, eris.Wrap(err, "unmarshal envelope json failed")
	}

	text, err := Format(&envelope)
	if err != nil {
		return false, err
	}

	return false, tm.sender.Send(ctx, text)
}
