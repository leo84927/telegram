package handle

import (
	"context"
	"log/slog"

	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"github.com/rotisserie/eris"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/leo84927/core/rabbitmq"
)

/*
 * Worker 是 consumer 的進入點。
 *
 * Handle 同步執行、錯誤往上回傳，core 才會在真的送達之後 Ack（詳見 docs/adr/0002）。
 */
type Worker struct {
	sender Sender
}

func NewWorker(sender Sender) *Worker {
	return &Worker{sender: sender}
}

func (w *Worker) Handle(ctx context.Context, msg rabbitmq.Message, _ rabbitmq.PublishHandler) error {
	// 日誌要帶 ctx 才會有 trace_id / span_id（見 CLAUDE.md 的「日誌與 trace 關聯」）
	slog.InfoContext(ctx, "=== processing message start ===")
	slog.InfoContext(
		ctx,
		"received message from RabbitMQ",
		"message", msg.Body,
	)
	defer slog.InfoContext(ctx, "=== processing message finished ===")

	// Envelope 的解析留在這一層，format 只認 Envelope 不認 bytes
	var envelope mqp.Envelope
	if err := protojson.Unmarshal(msg.Body, &envelope); err != nil {
		return eris.Wrap(err, "unmarshal envelope failed")
	}

	text, err := format(&envelope)
	if err != nil {
		return err
	}

	// 錯誤往上回傳後由 core 記一次並把 span 標為 Error
	return w.sender.Send(ctx, text)
}
