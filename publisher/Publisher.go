// Package publisher provides methods to interact with RabbitMQ,
// including sending messages and receiving replies.
package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Logger interface cho phép inject logger custom (vd: slog, zap, logrus)
type Logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// logger mặc định dùng log.Printf
type stdLogger struct{}

func (l *stdLogger) Infof(format string, args ...interface{}) { log.Printf("[INFO] "+format, args...) }
func (l *stdLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

// ErrTimeout is returned when an operation exceeds the configured timeout duration.
var ErrTimeout = errors.New("operation timed out")

// Publisher encapsulates the AMQP connection and channel for message publishing.
type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *Config
	logger  Logger
}

// New creates a new Publisher using the provided configuration.
// Cho phép truyền logger custom (tùy chọn)
func New(cfg *Config, logger ...Logger) (*Publisher, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	var l Logger
	if len(logger) > 0 && logger[0] != nil {
		l = logger[0]
	} else {
		l = &stdLogger{}
	}
	return &Publisher{conn: conn, channel: ch, config: cfg, logger: l}, nil
}

// Close gracefully closes the AMQP channel and connection.
func (p *Publisher) Close() error {
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

// PublishAndDecode sends a struct payload to the specified queue in JSON format,
// waits for a reply, then unmarshals the JSON reply into the given response struct.
//
// This is commonly used for RPC-style interactions via RabbitMQ.
//
// Parameters:
//   - ctx: Context to handle timeout and cancellation.
//   - queue: Queue name to publish the message to.
//   - req: Request object (any struct) to be marshaled to JSON.
//   - resp: Pointer to a struct where the reply will be unmarshaled.
//
// Returns an error if the publish fails or the reply cannot be unmarshaled.
func (p *Publisher) PublishAndDecode(ctx context.Context, queue string, req any, resp any) error {
	reply, err := p.PublishJSON(ctx, queue, req, true)
	if err != nil {
		return err
	}
	return json.Unmarshal(reply, resp)
}

// PublishJSON marshals a struct payload to JSON and publishes it to the queue.
// Optionally waits for a reply if waitForReply is true.
//
// Parameters:
//   - ctx: Context to handle timeout and cancellation.
//   - queue: Queue name to publish the message to.
//   - payload: Struct to be marshaled to JSON.
//   - waitForReply: If true, waits for a reply with matching CorrelationId.
//
// Returns the reply body (if waitForReply = true) or nil, and an error if any.
func (p *Publisher) PublishJSON(ctx context.Context, queue string, payload any, waitForReply bool) ([]byte, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("[RabbitMQ] failed to marshal payload: %w", err)
	}
	return p.Publish(ctx, queue, data, waitForReply)
}

// PublishEx cho phép publish vào exchange/routingKey, cấu hình persistent message
func (p *Publisher) PublishEx(ctx context.Context, exchange, routingKey string, body []byte, persistent bool, waitForReply bool) ([]byte, error) {
	if p.channel == nil || p.conn == nil || p.channel.IsClosed() || p.conn.IsClosed() {
		return nil, errors.New("[RabbitMQ] AMQP channel/connection is closed")
	}
	corrID := uuid.New().String()
	mode := amqp.Transient
	if persistent {
		mode = amqp.Persistent
	}
	if !waitForReply {
		err := p.channel.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: mode,
		})
		if err != nil {
			p.logger.Errorf("Publish error (exchange=%s, routingKey=%s): %v", exchange, routingKey, err)
			return nil, err
		}
		p.logger.Infof("Published to %s/%s", exchange, routingKey)
		return nil, nil
	}
	// Declare a temporary reply queue
	replyQueue, err := p.channel.QueueDeclare(
		"", false, true, true, false, nil,
	)
	if err != nil {
		p.logger.Errorf("Declare reply queue error: %v", err)
		return nil, err
	}
	msgs, err := p.channel.Consume(
		replyQueue.Name, "", true, false, false, false, nil,
	)
	if err != nil {
		p.logger.Errorf("Consume reply queue error: %v", err)
		return nil, err
	}
	defer func() {
		_ = p.channel.Cancel("", false)
	}()
	err = p.channel.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		ReplyTo:       replyQueue.Name,
		Body:          body,
		DeliveryMode:  mode,
	})
	if err != nil {
		p.logger.Errorf("Publish error (exchange=%s, routingKey=%s): %v", exchange, routingKey, err)
		return nil, err
	}
	p.logger.Infof("Published with CorrelationId: %s", corrID)
	timeout := time.After(time.Duration(p.config.Timeout) * time.Millisecond)
	for {
		select {
		case msg := <-msgs:
			if msg.CorrelationId == corrID {
				return msg.Body, nil
			}
		case <-timeout:
			return nil, ErrTimeout
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Publish gọi PublishEx với exchange="", routingKey=queue
func (p *Publisher) Publish(ctx context.Context, queue string, body []byte, waitForReply bool) ([]byte, error) {
	return p.PublishEx(ctx, "", queue, body, true, waitForReply)
}
