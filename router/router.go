package router

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/rotisserie/eris"
)

func New() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /health", health)
	mux.HandleFunc("POST /webhook", webhook)

	return mux
}

func health(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "health check success")
}

func webhook(w http.ResponseWriter, r *http.Request) {
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
