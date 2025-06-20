package main

import (
	"context"
	"fmt"
	"github.com/xhkzeroone/go-rabbitmq/publisher"
	"log"
)

func main() {
	cfg := &publisher.Config{
		URL:     "amqp://guest:guest@localhost:5672/",
		Timeout: 5000, // 5 giây
	}

	pub, err := publisher.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer pub.Close()

	ctx := context.Background()

	// 📨 Gửi tin nhắn và chờ phản hồi
	body := []byte(`{"message": "Hello from Publisher!"}`)
	reply, err := pub.Publish(ctx, "email-queue", body, true)
	if err != nil {
		log.Fatalf("Failed to publish: %v", err)
	}
	fmt.Println("📬 Reply received:", string(reply))

	// Chỉ gửi mà không cần nhận lại
	_, err = pub.Publish(ctx, "email-queue", []byte(`{"event": "fire-and-forget"}`), false)
	if err != nil {
		log.Fatalf("Failed to publish fire-and-forget: %v", err)
	}
	fmt.Println("✅ Message sent without waiting for reply.")
}
