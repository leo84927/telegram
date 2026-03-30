package config

import "github.com/leo84927/core/consul"

var (
	Client         *consul.Client
	EnvMap         map[string]string
	ServiceName    string
	TelegramToken  string
	TelegramChatId string
)
