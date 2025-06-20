package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/viper"
	"github.com/xhkzeroone/go-rabbitmq/consumer"
)

/* -------------------------------------------------------------------------- */
/* 📨  BUSINESS HANDLERS – tách file riêng nếu muốn                            */
/* -------------------------------------------------------------------------- */

type LogHandler struct{}

func (LogHandler) Handle(b []byte) ([]byte, error) {
	slog.Info("LogHandler", "msg", string(b))
	return nil, nil
}

type Log2Handler struct{}

func (Log2Handler) Handle(b []byte) ([]byte, error) {
	slog.Info("Log2Handler", "msg", string(b))
	return nil, nil
}

type EmailHandler struct{}

func (EmailHandler) Handle(b []byte) ([]byte, error) {
	slog.Info("EmailHandler", "msg", string(b))
	return []byte(`{"status":"ok"}`), nil
}

type OnboardHandler struct{}

func (OnboardHandler) Handle(b []byte) ([]byte, error) {
	slog.Info("OnboardHandler", "msg", string(b))
	return []byte(`{"status":"ok"}`), nil
}

/* -------------------------------------------------------------------------- */
/* 🚀  ENTRYPOINT                                                             */
/* -------------------------------------------------------------------------- */

func main() {
	// --config=./config.yaml (mặc định ./config.yaml)
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	// thiết lập slog (mặc định JSON, thêm timestamp, level,…)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// đọc YAML vào consumer.Config
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		slog.Error("load config failed", "err", err)
		os.Exit(1)
	}

	// khởi tạo Manager & đăng ký handler
	mgr := consumer.New(cfg)
	mgr.Register(
		&LogHandler{},
		&Log2Handler{},
		&EmailHandler{},
		&OnboardHandler{},
	)

	// Context huỷ khi nhận SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	// chạy lắng nghe
	if err := mgr.Listen(ctx); err != nil {
		slog.Error("manager stopped with error", "err", err)
	}
	slog.Info("👋 graceful shutdown complete")
}

/* -------------------------------------------------------------------------- */
/* 🔧  loadConfig – đọc YAML bằng Viper                                        */
/* -------------------------------------------------------------------------- */

func loadConfig(path string) (*consumer.Config, error) {
	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	// Thời gian đọc & parse có timeout nhẹ để tránh treo khởi động
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	type result struct {
		cfg *consumer.Config
		err error
	}
	done := make(chan result, 1)

	go func() {
		var r result
		if err := viper.ReadInConfig(); err != nil {
			r.err = err
		} else {
			var cfg consumer.Config
			if err := viper.Unmarshal(&cfg); err != nil {
				r.err = err
			} else {
				r.cfg = &cfg
			}
		}
		done <- r
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-done:
		return r.cfg, r.err
	}
}
