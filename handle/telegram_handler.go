package handle

import (
	"context"
	"fmt"
	"strconv"
	"telegram/config"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/leo84927/core/logger"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/protojson"
)

type TelegramManager struct {
	Token  string
	ChatId string
}

func NewTelegramManager() *TelegramManager {
	return &TelegramManager{
		Token:  config.TelegramToken,
		ChatId: config.TelegramChatId,
	}
}

func (tm *TelegramManager) sendMessage(ctx context.Context, body []byte) {
	bot, err := tgbotapi.NewBotAPI(tm.Token)
	if err != nil {
		logger.Error(ctx, "new telegram bot failed", err)
		return
	}

	chatId, err := strconv.Atoi(tm.ChatId)
	if err != nil {
		logger.Error(ctx, "translate chat id failed", err)
		return
	}

	message, err := buildMessage(ctx, body)
	if err != nil {
		logger.Error(ctx, "build message failed", err)
		return
	}

	botMsg := tgbotapi.NewMessage(int64(chatId), message)
	// TODO 之後視情況加入 botMsg.DisableNotification = true
	_, err = bot.Send(botMsg)
	if err != nil {
		logger.Error(ctx, "send message failed", err)
		return
	}
}

func buildMessage(ctx context.Context, body []byte) (string, error) {
	var envelope mqp.Envelope
	err := protojson.Unmarshal(body, &envelope)
	if err != nil {
		logger.Error(ctx, "build message unmarshal envelope json failed", err)
		return "", err
	}

	switch envelope.Type {
	case mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE:
		var exchangeRate erp.ExchangeRate
		err := protojson.Unmarshal([]byte(envelope.Data), &exchangeRate)
		if err != nil {
			logger.Error(ctx, "build message unmarshal exchangeRate json failed", err)
			return "", err
		}

		var messageFormat = "%s/%s : %s"
		if exchangeRate.BaseCurrency == erp.Currency_TWD {
			rate, err := decimal.NewFromString(exchangeRate.Rate)
			if err != nil {
				logger.Error(ctx, "build message translate exchange rate failed", err)
				return "", err
			}
			reverseRate := decimal.NewFromInt(1).DivRound(rate, 3).String()
			message := fmt.Sprintf(messageFormat, exchangeRate.CounterCurrency, erp.Currency_TWD, reverseRate)
			return message, nil
		}

		message := fmt.Sprintf(messageFormat, exchangeRate.BaseCurrency, exchangeRate.CounterCurrency, exchangeRate.Rate)
		return message, nil
	case mqp.EnvelopeType_TELEGRAM_ERROR:
		fallthrough
	default:
		return envelope.Data, nil
	}
}
