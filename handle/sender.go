package handle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/rotisserie/eris"

	"telegram/config"
)

const (
	sendMessageMethod = "sendMessage"
	getMeMethod       = "getMe"

	/*
	 * maxSendTries 是送出的重試次數。單次重試上限由呼叫端傳進來的 *http.Client 提供。
	 *
	 * sendMaxBudget 必須 > 單次重試上限 x maxSendTries
	 */
	maxSendTries  = 3
	sendMaxBudget = 20 * time.Second
)

type BotSender struct {
	client *http.Client
	apiURL string
	token  string
	chatID string
}

type Sender interface {
	Send(ctx context.Context, text string) error
}

func NewBotSender(client *http.Client, cfg config.Config) *BotSender {
	return &BotSender{
		client: client,
		apiURL: cfg.APIURL,
		token:  cfg.Token,
		chatID: cfg.ChatID,
	}
}

/*
 * 在啟動時打一次 getMe，token 錯就讓服務啟動失敗
 */
func (s *BotSender) ValidateToken(ctx context.Context) error {
	if err := s.do(ctx, getMeMethod, nil); err != nil {
		return eris.Wrap(unwrapPermanent(err), "validate telegram bot token failed")
	}

	return nil
}

func (s *BotSender) Send(ctx context.Context, text string) error {
	form := url.Values{}
	form.Set("chat_id", s.chatID)
	form.Set("text", text)

	operation := func() (struct{}, error) {
		return struct{}{}, s.do(ctx, sendMessageMethod, form)
	}

	_, err := backoff.Retry(
		ctx,
		operation,
		backoff.WithMaxTries(maxSendTries),
		backoff.WithMaxElapsedTime(sendMaxBudget),
	)

	return unwrapPermanent(err)
}

/*
 * do 發一次請求並把回應轉成錯誤分類。
 *
 * 不走 tgbotapi 的 bot.Send / MakeRequest，它們內部以 http.NewRequest 建請求，沒有 ctx，
 * 於是 timeout 放哪都不生效、關機時砍不掉、traceparent 也沒有地方注入。
 * 換掉的只有「請求怎麼發」—— 回應仍然解碼進 tgbotapi 的公開型別。
 */
func (s *BotSender) do(ctx context.Context, method string, form url.Values) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf(s.apiURL, s.token, method),
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		// 請求組不起來是程式或設定的問題，不重試
		return backoff.Permanent(eris.Wrapf(err, "build %s request failed", method))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.client.Do(req)
	if err != nil {
		// 連線層失敗（含 client.Timeout 觸發）沒有可分類的資訊，一律當可重試
		return eris.Wrapf(err, "%s request failed", method)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return eris.Wrapf(err, "read %s response failed", method)
	}

	return classify(method, resp.StatusCode, body)
}

/*
 * 決定這次失敗值不值得再試一次
 */
func classify(method string, statusCode int, body []byte) error {
	// 回應仍解碼進 tgbotapi 的公開型別，retry_after 就在它的 Parameters 裡
	var apiResp tgbotapi.APIResponse
	decodeErr := json.Unmarshal(body, &apiResp)

	if statusCode == http.StatusOK && decodeErr == nil && apiResp.Ok {
		return nil
	}

	err := eris.Errorf("%s responded %d: %s", method, statusCode, describe(apiResp, decodeErr, body))

	switch {
	// 429 一律可重試，並採用對方指定的等待時間
	case statusCode == http.StatusTooManyRequests:
		if apiResp.Parameters != nil && apiResp.Parameters.RetryAfter > 0 {
			return withRetryAfter(err, apiResp.Parameters.RetryAfter)
		}

		// 對方沒指定就交還給 backoff 自己的指數退避
		return err

	// 其他 4xx 是請求本身的問題（token 錯、chat 不存在、訊息不合法），不重試
	case statusCode >= http.StatusBadRequest && statusCode < http.StatusInternalServerError:
		return backoff.Permanent(err)

	// 5xx 與「200 但 ok 不為真」都是對方的暫時狀態，可重試
	default:
		return err
	}
}

// 取最有資訊量的那一份，Bot API 的 description 優先，解不出 JSON 時退回 body 原文
func describe(apiResp tgbotapi.APIResponse, decodeErr error, body []byte) string {
	if decodeErr == nil && apiResp.Description != "" {
		return apiResp.Description
	}

	return string(body)
}

/*
 * unwrapPermanent 解開 backoff 的 Permanent 包裝，所有 backoff.Retry 的錯誤出口都要經過這裡。
 *
 * backoff.Retry 只有在「還沒用完重試次數」時才會自己解開；不可重試的錯誤若剛好落在最後一次嘗試，
 * 回傳的最外層會是 *backoff.PermanentError，eris 只認得最外層，屆時堆疊整條看不到。
 */
func unwrapPermanent(err error) error {
	if permanent := (*backoff.PermanentError)(nil); errors.As(err, &permanent) {
		return permanent.Unwrap()
	}

	return err
}
