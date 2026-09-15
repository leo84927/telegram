package handle

import (
	"testing"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"google.golang.org/protobuf/encoding/protojson"
)

// rateData 把 ExchangeRate 轉成 Envelope.Data 該有的樣子（protojson 字串）
func rateData(t *testing.T, rate *erp.ExchangeRate) string {
	t.Helper()

	body, err := protojson.Marshal(rate)
	if err != nil {
		t.Fatalf("組裝 ExchangeRate 失敗: %v", err)
	}

	return string(body)
}

// 格式化是純函式，各 EnvelopeType 的輸出字串在這裡釘住
func TestFormat(t *testing.T) {
	tests := []struct {
		name     string
		envelope *mqp.Envelope
		want     string
	}{
		{
			name: "匯率照 base/counter 原樣呈現",
			envelope: &mqp.Envelope{
				Type: mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE,
				Data: rateData(t, &erp.ExchangeRate{
					BaseCurrency:    erp.Currency_USD,
					CounterCurrency: erp.Currency_JPY,
					Rate:            "150.5",
				}),
			},
			want: "USD/JPY : 150.5",
		},
		{
			// 供應商只給 TWD→counter 的方向，使用者要看的是「1 counter 值多少 TWD」
			name: "base 是 TWD 時取倒數並保留三位小數",
			envelope: &mqp.Envelope{
				Type: mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE,
				Data: rateData(t, &erp.ExchangeRate{
					BaseCurrency:    erp.Currency_TWD,
					CounterCurrency: erp.Currency_USD,
					Rate:            "32.5",
				}),
			},
			want: "USD/TWD : 0.031",
		},
		{
			name: "錯誤告警的 Data 本身就是要給人看的文字",
			envelope: &mqp.Envelope{
				Type: mqp.EnvelopeType_TELEGRAM_ERROR,
				Data: "quote batch failed: responded 500",
			},
			want: "quote batch failed: responded 500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := format(tt.envelope)
			if err != nil {
				t.Fatalf("format() error = %v, 期望 nil", err)
			}
			if got != tt.want {
				t.Errorf("format() = %q, 期望 %q", got, tt.want)
			}
		})
	}
}

// Data 解不出 ExchangeRate 時回 error，而不是把一段 JSON 原文送進 chat
func TestFormatRejectsUnparsableExchangeRate(t *testing.T) {
	_, err := format(&mqp.Envelope{
		Type: mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE,
		Data: "not json",
	})
	if err == nil {
		t.Fatal("format() error = nil, 期望解析失敗")
	}
}

// Rate 不是數字時 TWD 那條路不可 panic，要回 error
func TestFormatRejectsNonNumericRate(t *testing.T) {
	_, err := format(&mqp.Envelope{
		Type: mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE,
		Data: rateData(t, &erp.ExchangeRate{
			BaseCurrency:    erp.Currency_TWD,
			CounterCurrency: erp.Currency_USD,
			Rate:            "n/a",
		}),
	})
	if err == nil {
		t.Fatal("format() error = nil, 期望轉換失敗")
	}
}
