package main

import (
	"context"
	"github.com/spf13/viper"
	"github.com/xhkzeroone/go-rabbitmq/consumer"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type LogHandler struct{}

func (LogHandler) Handle(b []byte) ([]byte, error) {
	log.Printf(string(b))
	return nil, nil
}

type Log2Handler struct{}

func (Log2Handler) Handle(b []byte) ([]byte, error) {
	log.Printf(string(b))
	return nil, nil
}

func main() {
	cfg := consumer.Config{
		RabbitMQ: consumer.RabbitMQConfig{
			URL:      "amqp://guest:guest@localhost:5672/",
			Prefetch: 10,
			Queues: map[string]consumer.QueueConfig{
				"loghandler": { // <= tên struct (lowercase)
					Queue:    "log-queue",
					Exchange: "logs",
					Durable:  true,
				},
				"log2handler": { // <= tên struct (lowercase)
					Queue:    "log2-queue",
					Exchange: "logs",
					Durable:  true,
				},
				"emailhandler": { // <= tên struct (lowercase)
					Queue:    "email-queue",
					Exchange: "email",
					Durable:  true,
				},
				"onboardhandler": { // <= tên struct (lowercase)
					Queue:    "onboard-queue",
					Exchange: "onboard",
					Durable:  true,
				},
			},
		},
	}

	mgr := consumer.New(&cfg)
	mgr.Register(&LogHandler{}, &Log2Handler{}, &EmailHandler{}, &OnboardHandler{}) // hoặc nhiều handler cùng lúc

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := mgr.Listen(ctx); err != nil {
		log.Fatal(err)
	}
}

type EmailHandler struct{}

func (h *EmailHandler) Handle(msg []byte) ([]byte, error) {
	log.Printf("📨 [EmailHandler] Received: %s", string(msg))
	return []byte(`{"status":"ok"}`), nil
}

type OnboardHandler struct{}

func (h *OnboardHandler) Handle(msg []byte) ([]byte, error) {
	log.Printf("📨 [OnboardHandler] Received: %s", string(msg))
	return []byte(`{"status":"ok"}`), nil
}

func loadConfig() (*consumer.Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg consumer.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

//func waitForShutdown(c *consumer.Manager) {
//	sig := make(chan os.Signal, 1)
//	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
//	<-sig
//	log.Println("🛑 Shutting down...")
//	c.Close()
//}
