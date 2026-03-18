package config

import (
	"os"
	"sync"
)

var (
	ServiceName = sync.OnceValue(func() string {
		return os.Getenv("SERVICE_NAME")
	})
	ExchangeRateApiKey = sync.OnceValue(func() string {
		return os.Getenv("EXCHANGE_RATE_API_KEY")
	})
)
