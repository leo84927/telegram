package router

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"telegram/config"

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

// authorizedSecret 比對 Telegram 帶來的 secret token header。
// 未設定 WebhookSecret 時放行，避免設定 secret 前直接中斷既有 webhook。
func authorizedSecret(got string) bool {
	if config.WebhookSecret == "" {
		return true
	}
	// 定時比較，避免 timing attack 洩漏 secret
	return subtle.ConstantTimeCompare([]byte(got), []byte(config.WebhookSecret)) == 1
}

func webhook(w http.ResponseWriter, r *http.Request, bk bookkeepinggrpc.BookkeepingServiceClient) {
	if !authorizedSecret(r.Header.Get("X-Telegram-Bot-Api-Secret-Token")) {
		slog.Warn(
			"webhook rejected: invalid secret token",
			"remote", r.RemoteAddr,
		)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

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
