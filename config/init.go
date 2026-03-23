package config

import (
	"os"

	"github.com/joho/godotenv"
)

func init() {
	godotenv.Load()

	ServiceName = os.Getenv("SERVICE_NAME")
	TelegramToken = os.Getenv("TELEGRAM_TOKEN")
	TelegramChatId = os.Getenv("TELEGRAM_CHAT_ID")
	LoadRabbitMQ()
}
