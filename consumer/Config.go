package consumer

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

// === Configuration ===

type Config struct {
	RabbitMQ RabbitMQConfig `yaml:"rabbitmq" mapstructure:"rabbitmq" json:"rabbitmq"`
}

type RabbitMQConfig struct {
	URL            string                 `yaml:"url" mapstructure:"url" json:"url"`
	TLS            bool                   `yaml:"tls" mapstructure:"tls" json:"tls"`
	ClientCert     string                 `yaml:"clientCert" mapstructure:"clientCert" json:"clientCert"`
	ClientKey      string                 `yaml:"clientKey" mapstructure:"clientKey" json:"clientKey"`
	CACert         string                 `yaml:"caCert" mapstructure:"caCert" json:"caCert"`
	Heartbeat      time.Duration          `yaml:"heartbeat" mapstructure:"heartbeat" json:"heartbeat"`
	ReconnectDelay time.Duration          `yaml:"reconnectDelay" mapstructure:"reconnectDelay" json:"reconnectDelay"`
	Prefetch       int                    `yaml:"prefetch" mapstructure:"prefetch" json:"prefetch"`
	Queues         map[string]QueueConfig `yaml:"queues" mapstructure:"queues" json:"queues"`
}

type QueueConfig struct {
	Queue         string     `yaml:"queue" mapstructure:"queue" json:"queue"`
	BindingKey    string     `yaml:"bindingKey" mapstructure:"bindingKey" json:"bindingKey"`
	Exchange      string     `yaml:"exchange" mapstructure:"exchange" json:"exchange"`
	ConsumerTag   string     `yaml:"consumerTag" mapstructure:"consumerTag" json:"consumerTag"`
	AutoAck       bool       `yaml:"autoAck" mapstructure:"autoAck" json:"autoAck"`
	Durable       bool       `yaml:"durable" mapstructure:"durable" json:"durable"`
	AutoDelete    bool       `yaml:"autoDelete" mapstructure:"autoDelete" json:"autoDelete"`
	Passive       bool       `yaml:"passive" mapstructure:"passive" json:"passive"`
	Prefetch      int        `yaml:"prefetch" mapstructure:"prefetch" json:"prefetch"`
	Arguments     amqp.Table `yaml:"arguments" mapstructure:"arguments" json:"arguments"`
	RequeueOnFail bool       `yaml:"requeueOnFail" mapstructure:"requeueOnFail" json:"requeueOnFail"`
}
