package main

import (
	"testing"
	"time"
)

/*
 * 5s 這個值本身就是驗收條件的一部分，所以在這裡釘住。
 *
 * 「client 真的夾得住每一次對外呼叫」由 handle 那邊的送出測試負責（getMe 與 sendMessage 共用
 * 同一個 client），兩邊合起來才等於「每個對外呼叫有 5 秒上限」。
 */
func TestBotClientBoundsEveryCall(t *testing.T) {
	if got := newBotClient().Timeout; got != 5*time.Second {
		t.Errorf("client.Timeout = %v, 期望 %v", got, 5*time.Second)
	}
}
