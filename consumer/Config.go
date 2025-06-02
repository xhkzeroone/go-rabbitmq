package consumer

import (
	"github.com/spf13/viper"
	"strings"
	"time"
)

type Config struct {
	URL               string            `yaml:"url" mapstructure:"url"`
	UseTLS            bool              `yaml:"useTLS" mapstructure:"useTLS"`
	ClientCert        string            `yaml:"clientCert" mapstructure:"clientCert"`
	ClientKey         string            `yaml:"clientKey" mapstructure:"clientKey"`
	CACert            string            `yaml:"caCert" mapstructure:"caCert"`
	Queues            map[string]string `yaml:"queues" mapstructure:"queues"`
	ConsumerTag       string            `yaml:"consumerTag" mapstructure:"consumerTag"`
	ReconnectInterval time.Duration     `yaml:"reconnectInterval" mapstructure:"reconnectInterval"`
	Heartbeat         time.Duration     `yaml:"heartbeat" mapstructure:"heartbeat"`
	PrefetchCount     int               `yaml:"prefetchCount" mapstructure:"prefetchCount"`
	RetryCount        int               `yaml:"retryCount" mapstructure:"retryCount"`
	RequeueOnFail     bool              `yaml:"requeueOnFail" mapstructure:"requeueOnFail"`
}

// DefaultConfig returns a Config struct with default values.
// It reads from environment variables (with underscores replacing dots),
// or falls back to hardcoded defaults if env vars are not set.
func DefaultConfig() *Config {
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	viper.SetDefault("url", "amqp://guest:guest@localhost:5672/")
	viper.SetDefault("useTLS", false)
	viper.SetDefault("clientCert", "")
	viper.SetDefault("clientKey", "")
	viper.SetDefault("caCert", "")
	viper.SetDefault("queues", map[string]string{"default": "defaultQueue"})
	viper.SetDefault("consumerTag", "myConsumer")
	viper.SetDefault("reconnectInterval", 5*time.Second)
	viper.SetDefault("heartbeat", 10*time.Second)
	viper.SetDefault("prefetchCount", 1)
	viper.SetDefault("retryCount", 3)
	viper.SetDefault("requeueOnFail", true)

	return &Config{
		URL:               viper.GetString("url"),
		UseTLS:            viper.GetBool("useTLS"),
		ClientCert:        viper.GetString("clientCert"),
		ClientKey:         viper.GetString("clientKey"),
		CACert:            viper.GetString("caCert"),
		Queues:            viper.GetStringMapString("queues"),
		ConsumerTag:       viper.GetString("consumerTag"),
		ReconnectInterval: viper.GetDuration("reconnectInterval"),
		Heartbeat:         viper.GetDuration("heartbeat"),
		PrefetchCount:     viper.GetInt("prefetchCount"),
		RetryCount:        viper.GetInt("retryCount"),
		RequeueOnFail:     viper.GetBool("requeueOnFail"),
	}
}
