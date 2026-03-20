package handle

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"telegram/config"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/protojson"
)

type TelegramManager struct {
	Token  string
	ChatId string
	bot    *tgbotapi.BotAPI
	mutex  sync.RWMutex
}

func NewTelegramManager() *TelegramManager {
	return &TelegramManager{
		Token:  config.TelegramToken(),
		ChatId: config.TelegramChatId(),
	}
}

func (tm *TelegramManager) sendMessage(body []byte) {
	bot, err := tm.getBot()
	if err != nil {
		log.Printf("Get telegram bot failed, err: %v\n", err)
		return
	}

	chatId, err := strconv.Atoi(tm.ChatId)
	if err != nil {
		log.Printf("Translate chat id failed, err: %v\n", err)
		return
	}

	message, err := buildMessage(body)
	if err != nil {
		log.Printf("buildMessage failed, err: %v\n", err)
		return
	}

	botMsg := tgbotapi.NewMessage(int64(chatId), message)
	// TODO 之後視情況加入 botMsg.DisableNotification = true
	_, err = bot.Send(botMsg)
	if err != nil {
		log.Printf("Send message failed, err: %v\n", err)
		return
	}
}

func (tm *TelegramManager) getBot() (*tgbotapi.BotAPI, error) {
	tm.mutex.RLock()
	if tm.bot != nil {
		tm.mutex.RUnlock()
		return tm.bot, nil
	}
	tm.mutex.RUnlock()

	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// double check
	if tm.bot != nil {
		return tm.bot, nil
	}

	bot, err := tgbotapi.NewBotAPI(tm.Token)
	if err != nil {
		return nil, err
	}

	tm.bot = bot
	return tm.bot, nil
}

func buildMessage(body []byte) (string, error) {
	var envelope mqp.Envelope
	err := protojson.Unmarshal(body, &envelope)
	if err != nil {
		log.Printf("buildMessage unmarshal envelope json failed, err: %v\n", err)
		return "", err
	}

	switch envelope.Type {
	case mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE:
		var exchangeRate erp.ExchangeRate
		err := protojson.Unmarshal([]byte(envelope.Data), &exchangeRate)
		if err != nil {
			log.Printf("buildMessage unmarshal exchangeRate json failed, err: %v\n", err)
			return "", err
		}

		var messageFormat = "%s/%s : %s"
		if exchangeRate.BaseCurrency == erp.Currency_TWD {
			rate, err := decimal.NewFromString(exchangeRate.Rate)
			if err != nil {
				log.Printf("buildMessage translate exchange rate failed, err: %v\n", err)
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
