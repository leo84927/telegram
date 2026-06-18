package router

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"buf.build/gen/go/leo84927-proto/scheduler/grpc/go/bookkeeping/bookkeepinggrpc"
	bookkeepingpb "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/bookkeeping"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/rotisserie/eris"
	"google.golang.org/grpc"
)

func New(bookkeepingConn *grpc.ClientConn) *http.ServeMux {
	bk := bookkeepinggrpc.NewBookkeepingServiceClient(bookkeepingConn)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /health", health)
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		webhook(w, r, bk)
	})

	return mux
}

func health(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "health check success")
}

func webhook(w http.ResponseWriter, r *http.Request, bk bookkeepinggrpc.BookkeepingServiceClient) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error(
			"read webhook request body failed",
			"error", eris.ToJSON(err, true),
		)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	slog.Info("=== processing webhook start ===")
	slog.Info(
		"received message from telegram webhook",
		"message", string(body),
	)
	defer slog.Info("=== processing webhook finished ===")

	var update tgbotapi.Update
	if err := json.Unmarshal(body, &update); err != nil {
		slog.Error(
			"decode webhook update failed",
			"error", eris.ToJSON(err, true),
		)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if update.Message == nil {
		return
	}

	switch update.Message.Text {
	case "/hello":
		replyJSON(w, update.Message.Chat.ID, "hello world")
	case "/group":
		resp, err := bk.Group(r.Context(), &bookkeepingpb.GroupRequest{})
		if err != nil {
			replyJSON(w, update.Message.Chat.ID, "查詢失敗: "+err.Error())
			return
		}

		var sb strings.Builder
		for _, g := range resp.Groups {
			fmt.Fprintf(&sb, "%d. %s\n", g.Id, g.Name)
		}
		replyJSON(w, update.Message.Chat.ID, sb.String())
	default:
		replyJSON(w, update.Message.Chat.ID, "unknown command")
	}
}

func replyJSON(w http.ResponseWriter, chatID int64, text string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"method":  "sendMessage",
		"chat_id": chatID,
		"text":    text,
	})
}
