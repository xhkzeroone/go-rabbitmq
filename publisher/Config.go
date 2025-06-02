package publisher

import (
	"github.com/spf13/viper"
	"strings"
	"time"
)

type Config struct {
	URL     string `yaml:"url"`
	Timeout int    `yaml:"timeout"` // milliseconds
}

// DefaultConfig returns a Config struct with default values.
// It reads from environment variables (with underscores replacing dots),
// or falls back to hardcoded defaults if env vars are not set.
func DefaultConfig() *Config {
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	viper.SetDefault("url", "amqp://guest:guest@localhost:5672/")
	viper.SetDefault("timeout", 5*time.Second)

	return &Config{
		URL:     viper.GetString("url"),
		Timeout: viper.GetInt("timeout"),
	}
}
