package config

import (
	"os"
	"sync"
)

var (
	ServiceName = sync.OnceValue(func() string {
		return os.Getenv("SERVICE_NAME")
	})
	TelegramToken = sync.OnceValue(func() string {
		return os.Getenv("TELEGRAM_TOKEN")
	})
	TelegramChatId = sync.OnceValue(func() string {
		return os.Getenv("TELEGRAM_CHAT_ID")
	})
)
