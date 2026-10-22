package config

import (
	env "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/env"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

/*
 * 端點格式沿用 tgbotapi 的常數（"https://api.telegram.org/bot%s/%s"，依序帶入 token 與 method）。
 */
const defaultAPIURL = tgbotapi.APIEndpoint

type Config struct {
	Token  string
	ChatID string // 使用者與 bot 的私人對話，值即使用者的 user id；推送目的地，也是 webhook 唯一放行的 chat
	APIURL string // Bot API 端點格式，測試時指向 httptest

	WebhookCertPEM string
	WebhookKeyPEM  string
	WebhookPort    string
	WebhookSecret  string
}

func New(service map[string]string) Config {
	return Config{
		Token:  service[env.TelegramEnvKey_TELEGRAM_TOKEN.String()],
		ChatID: service[env.TelegramEnvKey_TELEGRAM_CHAT_ID.String()],
		APIURL: defaultAPIURL,

		WebhookCertPEM: service[env.TelegramEnvKey_TELEGRAM_WEBHOOK_CERT_PEM.String()],
		WebhookKeyPEM:  service[env.TelegramEnvKey_TELEGRAM_WEBHOOK_KEY_PEM.String()],
		WebhookPort:    service[env.TelegramEnvKey_TELEGRAM_WEBHOOK_PORT.String()],
		WebhookSecret:  service[env.TelegramEnvKey_TELEGRAM_WEBHOOK_SECRET.String()],
	}
}
