package main

import (
	"context"
	"fmt"
	"os"
	"telegram/handle"

	"github.com/leo84927/core/initialize"
)

func init() {
	// 啟動時先清理，防止上次異常結束殘留
	if err := os.Remove("/tmp/ready"); err != nil && !os.IsNotExist(err) {
		panic(err)
	}
}

func main() {
	app, err := initialize.New()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}
	defer app.Close()

	app.Workers = []func(ctx context.Context) error{
		func(ctx context.Context) error {
			return app.Consumer.WaitForConsume(ctx, handle.MessageHandler)
		},
	}
	app.Run()
}
