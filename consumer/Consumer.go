package consumer

import (
	"crypto/tls"
	"crypto/x509"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"reflect"
	"strings"
	"time"
)

// Consumer represents a RabbitMQ consumer with connection, channel, configuration,
// and a set of registered handlers to process messages.
type Consumer struct {
	conn    *amqp.Connection       // AMQP connection
	channel *amqp.Channel          // AMQP channel
	config  *Config                // Consumer configuration
	handler map[string]HandlerFunc // Map of queue key to message handler function
}

// NewWithRetry attempts to establish a connection to RabbitMQ with retry logic.
//
// It repeatedly tries to connect using the provided configuration until successful.
//
// Parameters:
//   - cfg: configuration for RabbitMQ connection and consumer settings.
//
// Returns:
//   - a connected Consumer instance if successful,
//   - or an error if connection fails repeatedly.
func NewWithRetry(cfg *Config) (*Consumer, error) {
	var (
		c   *Consumer
		err error
	)

	for {
		c, err = New(cfg)
		if err == nil {
			log.Println("[RabbitMQ] Connected successfully.")
			return c, nil
		}

		log.Printf("[RabbitMQ] Connection failed: %v — retrying in %v", err, cfg.ReconnectInterval)
		time.Sleep(cfg.ReconnectInterval)
	}
}

// New creates a new Consumer by establishing a RabbitMQ connection and channel.
//
// Parameters:
//   - cfg: configuration including URL, TLS settings, heartbeat, prefetch count.
//
// Returns:
//   - a Consumer instance ready for use,
//   - or an error if connection or channel creation fails.
func New(cfg *Config) (*Consumer, error) {
	var conn *amqp.Connection
	var err error

	amqpConfig := amqp.Config{
		Heartbeat: cfg.Heartbeat,
		Locale:    "en_US",
	}

	if cfg.UseTLS {
		tlsConfig, err := loadTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		amqpConfig.TLSClientConfig = tlsConfig
	}

	conn, err = amqp.DialConfig(cfg.URL, amqpConfig)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if cfg.PrefetchCount > 0 {
		if err := ch.Qos(cfg.PrefetchCount, 0, false); err != nil {
			return nil, err
		}
	}

	return &Consumer{
		conn:    conn,
		channel: ch,
		config:  cfg,
		handler: make(map[string]HandlerFunc),
	}, nil
}

// Register registers message handlers for queues based on handler struct names.
//
// Parameters:
//   - h: one or more handlers implementing the Handler interface.
//     Each handler is registered using its struct name (converted to lowercase) as the key.
//
// For example, a handler struct named "Task" will be registered for the queue key "task".
func (c *Consumer) Register(h ...Handler) {
	for _, v := range h {
		c.handler[getStructName(v)] = v.Handle
	}
}

// Listen starts consuming messages from all queues configured.
//
// It declares each queue, verifies a registered handler exists, and spawns a goroutine
// to consume messages for each queue.
//
// Returns:
//   - an error if any queue declaration fails.
func (c *Consumer) Listen() error {
	for key, queueName := range c.config.Queues {
		_, err := c.channel.QueueDeclare(
			queueName, true, false, false, false, nil,
		)
		if err != nil {
			log.Printf("[RabbitMQ] Failed to declare queue %s: %v", queueName, err)
			continue
		}

		handler, exists := c.handler[key]
		if !exists {
			log.Printf("[RabbitMQ] No handler registered for queue: %s", queueName)
			continue
		}

		go c.listenQueue(queueName, handler)
	}
	return nil
}

// Close closes the RabbitMQ channel and connection.
//
// Should be called when the consumer is no longer needed.
func (c *Consumer) Close() {
	_ = c.channel.Close()
	_ = c.conn.Close()
}

// --- Private helper functions ---

// getStructName returns the lowercase struct name of the provided interface,
// dereferencing pointer types if necessary.
func getStructName(i interface{}) string {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return strings.ToLower(t.Name())
}

// loadTLSConfig loads TLS configuration from certificate files specified in Config.
//
// Parameters:
//   - cfg: configuration containing paths to client cert, client key, and CA cert files.
//
// Returns:
//   - a *tls.Config if the certificates are loaded successfully,
//   - or an error otherwise.
func loadTLSConfig(cfg *Config) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
	if err != nil {
		return nil, err
	}

	caCert, err := os.ReadFile(cfg.CACert)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// listenQueue continuously consumes messages from the specified queue and processes them using the handler.
//
// Parameters:
//   - queueName: the name of the queue to consume from.
//   - handler: a function that processes message bodies and returns a response and an error.
func (c *Consumer) listenQueue(queueName string, handler HandlerFunc) {
	for {
		msgs, err := c.channel.Consume(
			queueName,
			c.config.ConsumerTag,
			false,
			false, false, false,
			nil,
		)
		if err != nil {
			log.Printf("[RabbitMQ] Failed to consume from queue %s: %v — reconnecting...", queueName, err)
			c.reconnect()
			continue
		}

		log.Printf("[RabbitMQ] Listening on queue: %s", queueName)

		for msg := range msgs {
			go func(m amqp.Delivery) {
				var err error
				var resp []byte

				for attempt := 1; attempt <= c.config.RetryCount; attempt++ {
					resp, err = handler(m.Body)
					if err == nil {
						break
					}
					log.Printf("[RabbitMQ] Handler error (attempt %d/%d): %v", attempt, c.config.RetryCount, err)
					time.Sleep(300 * time.Millisecond)
				}

				if m.ReplyTo != "" {
					err2 := c.channel.Publish(
						"", m.ReplyTo, false, false,
						amqp.Publishing{
							ContentType:   "application/json",
							CorrelationId: m.CorrelationId,
							Body:          resp,
						},
					)
					if err2 != nil {
						log.Printf("[RabbitMQ] Failed to send reply to %s: %v", m.ReplyTo, err2)
					}
				}

				if err != nil {
					log.Printf("[RabbitMQ] Message processing failed, sending NACK")
					_ = m.Nack(false, c.config.RequeueOnFail)
				} else {
					_ = m.Ack(false)
				}
			}(msg)
		}

		log.Printf("[RabbitMQ] Consumer channel closed for queue %s — retrying consumption", queueName)
		time.Sleep(c.config.ReconnectInterval)
	}
}

// reconnect attempts to re-establish the RabbitMQ connection and channel.
//
// It loops until a successful reconnection is made.
func (c *Consumer) reconnect() {
	for {
		newConsumer, err := New(c.config)
		if err != nil {
			log.Printf("[RabbitMQ] Reconnect failed: %v — retrying in %v", err, c.config.ReconnectInterval)
			time.Sleep(c.config.ReconnectInterval)
			continue
		}

		c.conn = newConsumer.conn
		c.channel = newConsumer.channel

		if c.config.PrefetchCount > 0 {
			_ = c.channel.Qos(c.config.PrefetchCount, 0, false)
		}

		log.Println("[RabbitMQ] Reconnected successfully.")
		break
	}
}
