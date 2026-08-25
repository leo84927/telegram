package handle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"github.com/cenkalti/backoff/v5"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/rotisserie/eris"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/protojson"
)

// Sender 把一段已經格式化好的文字送到 Telegram
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
 * 送出一則訊息的預算。
 *
 * 三個數字必須一起看：backoff 的 MaxElapsedTime 檢查是 time.Since(startedAt)+next，
 * 分母包含 operation 自己執行的時間（backoff/v5 retry.go:121）。
 * 所以 MaxElapsedTime 一旦小於 timeout × MaxTries，設了 MaxTries 也跑不到。
 * 這組數字刻意不與 core 的 ConnectionManager 預算共用 —— 那邊的 operation 是 TCP dial，這邊是整個 HTTP 往返。
 */
const (
	sendTimeout        = 5 * time.Second
	sendMaxTries       = 3
	sendMaxElapsedTime = 20 * time.Second
)

const defaultAPIEndpoint = "https://api.telegram.org/bot%s/%s"

/*
 * BotSender 是主動呼叫 Telegram Bot API 的 adapter。
 *
 * 刻意不用 tgbotapi 的 bot.Send：它的 MakeRequest 用 http.NewRequest 而非
 * NewRequestWithContext（bot.go:100），所以吃不到 ctx —— 沒有 timeout（現況是無限等待，
 * 一個掛住的請求會永久占住一個 prefetch 額度）、關機時砍不掉、traceparent 也注不進去。
 * 回應仍解碼進 tgbotapi 的公開型別：要換掉的是「請求怎麼發」，不是「回應長什麼樣」。
 */
type BotSender struct {
	client   *http.Client
	token    string
	chatID   int64
	endpoint string // 測試用；預設 defaultAPIEndpoint
}

/*
 * NewBotSender 建立 bot 並在啟動時打一次 getMe 驗證 token。
 *
 * client 是必填參數（與 exchange_rate 的供應商 adapter 同一慣例）。
 * getMe 不重試：失敗就讓 process 起不來，交給 systemd 的 Restart=on-failure。
 * 這改變了現有部署行為 —— 現況是 token 錯也照跑，只是每則告警各自失敗然後消失。
 */
func NewBotSender(ctx context.Context, client *http.Client, token, chatID string) (*BotSender, error) {
	id, err := strconv.ParseInt(chatID, 10, 64)
	if err != nil {
		return nil, eris.Wrap(err, "parse telegram chat id failed")
	}

	sender := &BotSender{
		client:   client,
		token:    token,
		chatID:   id,
		endpoint: defaultAPIEndpoint,
	}

	if _, err := sender.call(ctx, "getMe", nil); err != nil {
		return nil, eris.Wrap(unwrapPermanent(err), "verify telegram token failed")
	}

	return sender, nil
}

func (s *BotSender) Send(ctx context.Context, text string) error {
	params := url.Values{}
	params.Set("chat_id", strconv.FormatInt(s.chatID, 10))
	params.Set("text", text)
	// TODO 之後視情況加入 params.Set("disable_notification", "true")

	_, err := backoff.Retry(
		ctx,
		func() (*tgbotapi.APIResponse, error) { return s.call(ctx, "sendMessage", params) },
		backoff.WithMaxTries(sendMaxTries),
		backoff.WithMaxElapsedTime(sendMaxElapsedTime),
	)
	if err != nil {
		return eris.Wrap(unwrapPermanent(err), "send telegram message failed")
	}

	return nil
}

// call 送出一次 Bot API 請求，錯誤已分好可重試與不可重試
func (s *BotSender) call(ctx context.Context, method string, params url.Values) (*tgbotapi.APIResponse, error) {
	// 單次嘗試的上限。ctx 一路傳進請求，所以關機時進行中的請求會一起被砍
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf(s.endpoint, s.token, method),
		strings.NewReader(params.Encode()),
	)
	if err != nil {
		// 組不出請求是程式碼問題，重試沒有意義
		return nil, backoff.Permanent(eris.Wrap(err, "build telegram request failed"))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.client.Do(req)
	if err != nil {
		// 網路層失敗：可重試
		return nil, eris.Wrap(err, "call telegram api failed")
	}
	defer resp.Body.Close()

	var apiResp tgbotapi.APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, eris.Wrap(err, "decode telegram response failed")
	}

	if !apiResp.Ok {
		return nil, classify(&apiResp)
	}

	return &apiResp, nil
}

/*
 * classify 把 Telegram 的錯誤回應分成可重試與不可重試。
 *
 * 429 是流量控制，不論有沒有帶 retry_after 都是暫時性的，必須先攔在 4xx 分支之前。
 * 帶 retry_after 時改用 telegram 給的秒數當退避。backoff.RetryAfter 回傳的
 * *RetryAfterError 不包裝任何內層錯誤（backoff error.go:35），直接回傳會讓錯誤描述與
 * eris 堆疊整條消失，所以包一層 —— errors.As 會沿著 Unwrap 找到它。
 * 其他 4xx（400 參數錯、401 token 錯、403 被使用者封鎖）重試純屬浪費。
 * 5xx 與未知狀態碼視為可重試。
 */
func classify(resp *tgbotapi.APIResponse) error {
	// 429 一律可重試，先攔下來 —— 否則會落進下面的 4xx 分支被當成不可重試
	if resp.ErrorCode == http.StatusTooManyRequests {
		if resp.Parameters != nil && resp.Parameters.RetryAfter > 0 {
			return eris.Wrapf(
				backoff.RetryAfter(resp.Parameters.RetryAfter),
				"telegram api error %d: %s", resp.ErrorCode, resp.Description,
			)
		}

		return eris.Errorf("telegram api error %d: %s", resp.ErrorCode, resp.Description)
	}

	err := eris.Errorf("telegram api error %d: %s", resp.ErrorCode, resp.Description)

	if resp.ErrorCode >= http.StatusBadRequest && resp.ErrorCode < http.StatusInternalServerError {
		return backoff.Permanent(err)
	}

	return err
}

/*
 * unwrapPermanent 解開 backoff 的 Permanent 包裝，所有 backoff.Retry 的錯誤出口都要經過這裡。
 *
 * backoff.Retry 只有在「還沒用完重試次數」時才會自己解開；不可重試的錯誤若剛好落在最後一次嘗試，
 * 回傳的最外層會是 *backoff.PermanentError。eris 只認得最外層，屆時 exception.stacktrace 會整條看不到堆疊。
 * core/rabbitmq/common.go:72 有同一份，但未匯出。
 */
func unwrapPermanent(err error) error {
	if permanent := (*backoff.PermanentError)(nil); errors.As(err, &permanent) {
		return permanent.Unwrap()
	}

	return err
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
