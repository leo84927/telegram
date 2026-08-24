package handle

import (
	"context"
	"fmt"
	"strconv"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/rotisserie/eris"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/protojson"
)

/*
 * Sender 把一段已經格式化好的文字送到 Telegram。
 *
 * 【粗胚結論見 router/router.go 的 replySender】介面本身只表達「送出去、可能失敗」，
 * 送到哪個 chat、用什麼 parse mode、能送幾次，全部藏在實作裡。
 */
type Sender interface {
	Send(ctx context.Context, text string) error
}

/*
 * TelegramManager 持有一個長生命週期的 Sender，並把 RabbitMQ 訊息接到它身上。
 * 原本的 Token / ChatId 兩個欄位往下推進 BotSender —— 這一層不再認識 telegram 的 API。
 */
type TelegramManager struct {
	sender Sender
}

func NewTelegramManager(sender Sender) *TelegramManager {
	return &TelegramManager{sender: sender}
}

/*
 * BotSender 是主動呼叫 Telegram Bot API 的 adapter。
 *
 * bot 與 chat id 都在建構時解析一次：現況每則訊息都 NewBotAPI，而 NewBotAPI 內部會打一次
 * getMe（tgbotapi bot.go:64），等於每則告警有兩個網路往返可以失敗，其中一個純屬浪費。
 */
type BotSender struct {
	bot    *tgbotapi.BotAPI
	chatID int64
}

func NewBotSender(token, chatID string) (*BotSender, error) {
	id, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return nil, eris.Wrap(err, "parse telegram chat id failed")
	}

	// 這一次 getMe 同時是 token 的驗證：token 錯或網路不通，服務就不會啟動
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, eris.Wrap(err, "new telegram bot failed")
	}

	return &BotSender{bot: bot, chatID: id}, nil
}

/*
 * Send 把文字送去 Telegram。
 *
 * 【粗胚發現】ctx 在這裡是裝飾品：tgbotapi 的 MakeRequest 用的是 http.NewRequest 而不是
 * http.NewRequestWithContext（bot.go:100），所以取消訊號進不去、沒有 timeout，
 * 而且 traceparent 也注不進這個 HTTP 請求 —— trace 在這裡斷掉。
 */
func (s *BotSender) Send(_ context.Context, text string) error {
	// TODO 之後視情況加入 botMsg.DisableNotification = true
	if _, err := s.bot.Send(tgbotapi.NewMessage(s.chatID, text)); err != nil {
		return eris.Wrap(err, "send telegram message failed")
	}

	return nil
}

/*
 * Format 把 Envelope 轉成要送給 Telegram 的文字。
 *
 * 純函式：不吃 ctx、不打網路、不寫日誌。錯誤一律往上回傳，由呼叫端決定要不要記。
 * 這是整個 telegram 服務唯一有領域規則的地方（EnvelopeType 分派、TWD 取倒數到小數三位），
 * 原本藏在 handle 的私有函式 buildMessage，只能經由 sendMessage 觸達。
 */
func Format(envelope *mqp.Envelope) (string, error) {
	const messageFormat = "%s/%s : %s"

	switch envelope.Type {
	case mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE:
		var exchangeRate erp.ExchangeRate
		if err := protojson.Unmarshal([]byte(envelope.Data), &exchangeRate); err != nil {
			return "", eris.Wrap(err, "unmarshal exchange rate json failed")
		}

		// base 是 TWD 時反過來報價，例如 USD/TWD 而不是 TWD/USD
		if exchangeRate.BaseCurrency == erp.Currency_TWD {
			rate, err := decimal.NewFromString(exchangeRate.Rate)
			if err != nil {
				return "", eris.Wrap(err, "translate exchange rate failed")
			}

			reverseRate := decimal.NewFromInt(1).DivRound(rate, 3).String()

			return fmt.Sprintf(messageFormat, exchangeRate.CounterCurrency, erp.Currency_TWD, reverseRate), nil
		}

		return fmt.Sprintf(messageFormat, exchangeRate.BaseCurrency, exchangeRate.CounterCurrency, exchangeRate.Rate), nil
	case mqp.EnvelopeType_TELEGRAM_ERROR:
		fallthrough
	default:
		return envelope.Data, nil
	}
}
