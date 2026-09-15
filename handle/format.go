package handle

import (
	"fmt"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"github.com/rotisserie/eris"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	twdReverseScale = 3
	rateFormat = "%s/%s : %s"
)

/*
 * 不吃 ctx、不寫日誌、不碰網路 —— 這是本服務唯一有領域規則的地方。
 *
 * bytes 的解析留在 worker 那一層，這裡只認 Envelope。
 */
func format(envelope *mqp.Envelope) (string, error) {
	switch envelope.Type {
	case mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE:
		return formatExchangeRate(envelope.Data)

	// TELEGRAM_ERROR 和 default 一律原樣送出
	case mqp.EnvelopeType_TELEGRAM_ERROR:
		fallthrough
	default:
		return envelope.Data, nil
	}
}

func formatExchangeRate(data string) (string, error) {
	var exchangeRate erp.ExchangeRate
	if err := protojson.Unmarshal([]byte(data), &exchangeRate); err != nil {
		return "", eris.Wrap(err, "unmarshal exchange rate failed")
	}

	/*
	 * base 是 TWD 時改報「1 counter 值多少 TWD」。
	 * 供應商只提供 TWD→counter 的方向，但使用者要看的是反過來的那個數字。
	 */
	if exchangeRate.BaseCurrency == erp.Currency_TWD {
		rate, err := decimal.NewFromString(exchangeRate.Rate)
		if err != nil {
			return "", eris.Wrapf(err, "parse exchange rate %q failed", exchangeRate.Rate)
		}

		reverseRate := decimal.NewFromInt(1).DivRound(rate, twdReverseScale).String()

		return fmt.Sprintf(rateFormat, exchangeRate.CounterCurrency, erp.Currency_TWD, reverseRate), nil
	}

	return fmt.Sprintf(rateFormat, exchangeRate.BaseCurrency, exchangeRate.CounterCurrency, exchangeRate.Rate), nil
}
