package main

import (
	"os"
	"telegram/config"

	"github.com/joho/godotenv"
)

func init() {
	// 啟動時先清理，防止上次異常結束殘留
	os.Remove("/tmp/ready")
	godotenv.Load()
	config.LoadRabbitMQ()
}

func main() {

}
