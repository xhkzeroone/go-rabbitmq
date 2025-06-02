package main

import (
	"github.com/spf13/viper"
	"github.com/xhkzeroone/go-rabbitmq/consumer"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("❌ Load config failed: %v", err)
	}

	consumer, err := consumer.NewWithRetry(cfg)
	if err != nil {
		log.Fatalf("❌ Init consumer failed: %v", err)
	}

	consumer.Register(&EmailHandler{}, &OnboardHandler{})

	if err := consumer.Listen(); err != nil {
		log.Fatalf("❌ Listen failed: %v", err)
	}

	waitForShutdown(consumer)
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

func waitForShutdown(c *consumer.Consumer) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("🛑 Shutting down...")
	c.Close()
}
