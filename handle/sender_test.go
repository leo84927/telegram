package handle

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	erp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/exchange_rate"
	mqp "buf.build/gen/go/leo84927-proto/scheduler/protocolbuffers/go/rabbitmq"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/leo84927/core/rabbitmq"

	"telegram/config"
)

/*
 * seam 落在 worker 進入點：對外 HTTP 用 httptest 頂住，真的 BotSender 進測試。
 *
 * Sender 介面依然存在，但不是測試的主要入口 —— 替身只在「透過 HTTP 觀察不到」時才用得上，
 * 而下面每一條斷言（重試次數、錯誤分類、retry-after）都觀察得到。
 */

const testToken = "test-token"

// ─────────────────────────────────────────────
// 測試替身
// ─────────────────────────────────────────────

type recordedRequest struct {
	path string
	form url.Values
}

/*
 * botStub 記下 Bot API 端收到的每一次請求。
 *
 * 需要 mutex：httptest 的 handler 跑在自己的 goroutine 上，而重試那幾條測試會在斷言的同時
 * 還有請求在飛。
 */
type botStub struct {
	mu       sync.Mutex
	requests []recordedRequest
	url      string
}

func newBotStub(t *testing.T, respond http.HandlerFunc) *botStub {
	t.Helper()

	stub := &botStub{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()

		stub.mu.Lock()
		stub.requests = append(stub.requests, recordedRequest{path: r.URL.Path, form: r.PostForm})
		stub.mu.Unlock()

		respond(w, r)
	}))
	t.Cleanup(server.Close)

	stub.url = server.URL
	return stub
}

func (s *botStub) received() []recordedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]recordedRequest(nil), s.requests...)
}

// respondJSON 讓每個測試只需要說「這次回什麼」
func respondJSON(statusCode int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(body))
	}
}

/*
 * respondInSequence 依序回傳每一個回應，用完之後重複最後一個。
 * 「先 429 再成功」這種序列沒有它就表達不出來。
 */
func respondInSequence(responses ...http.HandlerFunc) http.HandlerFunc {
	var (
		mu sync.Mutex
		n  int
	)

	return func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		respond := responses[min(n, len(responses)-1)]
		n++
		mu.Unlock()

		respond(w, r)
	}
}

// ─────────────────────────────────────────────
// 共用組裝與斷言
// ─────────────────────────────────────────────

func newTestSender(stub *botStub) *BotSender {
	return NewBotSender(testClient(), config.Config{
		Token:  testToken,
		ChatID: "42",
		APIURL: stub.url + "/bot%s/%s",
	})
}

// 測試用 client 的上限只是防止測試掛住；真正的 5s 由 main 提供，見 TestBotClientBoundsEveryCall
func testClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Second}
}

// 用 Fatalf 而非 Errorf：後面的斷言會直接索引 received()，數量對不上就會是 index out of range
func assertRequestCount(t *testing.T, stub *botStub, want int) {
	t.Helper()

	if got := len(stub.received()); got != want {
		t.Fatalf("Bot API 收到 %d 次請求, 期望 %d 次", got, want)
	}
}

func exchangeRateMessage(t *testing.T, rate *erp.ExchangeRate) rabbitmq.Message {
	t.Helper()

	body, err := protojson.Marshal(&mqp.Envelope{
		Type: mqp.EnvelopeType_TELEGRAM_SUCCESS_EXCHANGE_RATE,
		Data: rateData(t, rate),
	})
	if err != nil {
		t.Fatalf("組裝 Envelope 訊息失敗: %v", err)
	}

	return rabbitmq.Message{Body: body}
}

// ─────────────────────────────────────────────
// 送出
// ─────────────────────────────────────────────

// 一則告警走完 worker → format → BotSender，落在 Bot API 上的就是 sendMessage 與格式化後的文字
func TestHandleSendsFormattedMessage(t *testing.T) {
	stub := newBotStub(t, respondJSON(http.StatusOK, `{"ok":true,"result":{}}`))
	worker := NewWorker(newTestSender(stub))

	msg := exchangeRateMessage(t, &erp.ExchangeRate{
		BaseCurrency:    erp.Currency_USD,
		CounterCurrency: erp.Currency_JPY,
		Rate:            "150.5",
	})

	if err := worker.Handle(context.Background(), msg, nil); err != nil {
		t.Fatalf("Handle() error = %v, 期望 nil", err)
	}

	assertRequestCount(t, stub, 1)

	request := stub.received()[0]
	if want := "/bot" + testToken + "/" + sendMessageMethod; request.path != want {
		t.Errorf("請求路徑 = %q, 期望 %q", request.path, want)
	}
	if got := request.form.Get("chat_id"); got != "42" {
		t.Errorf("chat_id = %q, 期望 %q", got, "42")
	}
	if got := request.form.Get("text"); got != "USD/JPY : 150.5" {
		t.Errorf("text = %q, 期望 %q", got, "USD/JPY : 150.5")
	}
}

/*
 * 送不出去就必須回 error —— 這是整張票的核心：
 * 原本是 logger.Error 之後 return，訊息照樣被 core 當成處理完成而 Ack，告警就此消失。
 *
 * 同時釘住 5xx 可重試與重試次數上限。
 */
func TestHandleRetriesServerErrorsAndReturnsError(t *testing.T) {
	stub := newBotStub(t, respondJSON(http.StatusInternalServerError, `{"ok":false,"error_code":500,"description":"Internal Server Error"}`))
	worker := NewWorker(newTestSender(stub))

	msg := exchangeRateMessage(t, &erp.ExchangeRate{
		BaseCurrency:    erp.Currency_USD,
		CounterCurrency: erp.Currency_JPY,
		Rate:            "150.5",
	})

	err := worker.Handle(context.Background(), msg, nil)
	if err == nil {
		t.Fatal("Handle() error = nil, 期望送出失敗時回傳 error")
	}
	if !strings.Contains(err.Error(), "Internal Server Error") {
		t.Errorf("error = %q, 期望帶著 Bot API 的描述", err)
	}

	assertRequestCount(t, stub, maxSendTries)
}

// 其他 4xx 是請求本身的問題，重試只是重複同一個錯
func TestSendDoesNotRetryClientErrors(t *testing.T) {
	stub := newBotStub(t, respondJSON(http.StatusBadRequest, `{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`))

	err := newTestSender(stub).Send(context.Background(), "hello")
	if err == nil {
		t.Fatal("Send() error = nil, 期望 400 回傳 error")
	}
	if !strings.Contains(err.Error(), "chat not found") {
		t.Errorf("error = %q, 期望帶著 Bot API 的描述", err)
	}

	assertRequestCount(t, stub, 1)
}

/*
 * 429 一律可重試，且採用對方指定的等待時間。
 *
 * retry_after 遠大於總預算時 backoff 會直接放棄 —— 這正好讓「有沒有採用對方的時間」看得出來：
 * 若忽略 retry_after，走的是 0.5s 起跳的指數退避，會打滿三次。
 *
 * 同時釘住 retry-after 變體不可裸回傳：backoff 的 *RetryAfterError 不包裝內層錯誤，
 * 直接回傳的話這裡拿到的訊息會是 "retry after 1h0m0s"，Bot API 的描述整段消失。
 */
func TestSendAdoptsRetryAfterAndKeepsDescription(t *testing.T) {
	stub := newBotStub(t, respondJSON(
		http.StatusTooManyRequests,
		`{"ok":false,"error_code":429,"description":"Too Many Requests: retry later","parameters":{"retry_after":3600}}`,
	))

	err := newTestSender(stub).Send(context.Background(), "hello")
	if err == nil {
		t.Fatal("Send() error = nil, 期望 429 用盡預算後回傳 error")
	}
	if !strings.Contains(err.Error(), "Too Many Requests: retry later") {
		t.Errorf("error = %q, 期望保住 Bot API 的描述而不是只剩 retry after", err)
	}

	assertRequestCount(t, stub, 1)
}

// retry_after 在預算之內時要真的等滿再送，不是照自己的退避節奏搶跑
func TestSendWaitsForRetryAfterBeforeRetrying(t *testing.T) {
	const retryAfter = time.Second

	stub := newBotStub(t, respondInSequence(
		respondJSON(http.StatusTooManyRequests, `{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":1}}`),
		respondJSON(http.StatusOK, `{"ok":true,"result":{}}`),
	))

	start := time.Now()
	if err := newTestSender(stub).Send(context.Background(), "hello"); err != nil {
		t.Fatalf("Send() error = %v, 期望第二次成功", err)
	}
	elapsed := time.Since(start)

	assertRequestCount(t, stub, 2)

	// 預設的指數退避首段是 0.5s 上下，等滿 1s 才代表用的是對方指定的時間
	if elapsed < retryAfter {
		t.Errorf("兩次請求相隔 %v, 期望至少 %v", elapsed, retryAfter)
	}
}

// ─────────────────────────────────────────────
// 啟動時的 token 驗證
// ─────────────────────────────────────────────

func TestValidateTokenRejectsBadToken(t *testing.T) {
	stub := newBotStub(t, respondJSON(http.StatusUnauthorized, `{"ok":false,"error_code":401,"description":"Unauthorized"}`))

	err := newTestSender(stub).ValidateToken(context.Background())
	if err == nil {
		t.Fatal("ValidateToken() error = nil, 期望 token 錯時回傳 error")
	}
	if !strings.Contains(err.Error(), "Unauthorized") {
		t.Errorf("error = %q, 期望帶著 Bot API 的描述", err)
	}

	// 不重試 —— 重試是 systemd 的事
	assertRequestCount(t, stub, 1)
}

func TestValidateTokenAcceptsGoodToken(t *testing.T) {
	stub := newBotStub(t, respondJSON(http.StatusOK, `{"ok":true,"result":{"id":1,"is_bot":true,"username":"test_bot"}}`))

	if err := newTestSender(stub).ValidateToken(context.Background()); err != nil {
		t.Fatalf("ValidateToken() error = %v, 期望 nil", err)
	}

	assertRequestCount(t, stub, 1)

	if want := "/bot" + testToken + "/" + getMeMethod; stub.received()[0].path != want {
		t.Errorf("請求路徑 = %q, 期望 %q", stub.received()[0].path, want)
	}
}
