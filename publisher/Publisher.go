// Package publisher provides methods to interact with RabbitMQ,
// including sending messages and receiving replies.
package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

// ErrTimeout is returned when an operation exceeds the configured timeout duration.
var ErrTimeout = errors.New("operation timed out")

// Publisher encapsulates the AMQP connection and channel for message publishing.
type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *Config
}

// New creates a new Publisher using the provided configuration.
// It establishes a connection and channel to RabbitMQ.
//
// Returns a pointer to Publisher or an error if connection fails.
func New(cfg *Config) (*Publisher, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &Publisher{conn: conn, channel: ch, config: cfg}, nil
}

// Close gracefully closes the AMQP channel and connection.
//
// Returns an error if closing the channel or connection fails.
func (p *Publisher) Close() error {
	if err := p.channel.Close(); err != nil {
		return err
	}
	return p.conn.Close()
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

// Publish sends a raw message (as bytes) to the given queue.
// If waitForReply is true, it waits for a reply with the same CorrelationId.
//
// Parameters:
//   - ctx: Context to handle timeout and cancellation.
//   - queue: Queue name to publish the message to.
//   - body: Message body in bytes.
//   - waitForReply: If true, waits and returns the reply message body.
//
// Returns the reply body if applicable, or nil, and an error if publish or reply fails.
func (p *Publisher) Publish(ctx context.Context, queue string, body []byte, waitForReply bool) ([]byte, error) {
	if p.channel.IsClosed() {
		return nil, errors.New("[RabbitMQ] AMQP channel is closed")
	}

	corrID := uuid.New().String()

	if !waitForReply {
		// Fire-and-forget mode
		err := p.channel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Declare a temporary reply queue
	replyQueue, err := p.channel.QueueDeclare(
		"", false, true, true, false, nil,
	)
	if err != nil {
		return nil, err
	}

	// Start consuming messages from the reply queue
	msgs, err := p.channel.Consume(
		replyQueue.Name, "", true, false, false, false, nil,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = p.channel.Cancel("", false)
	}()

	// Publish request message with CorrelationId and ReplyTo
	err = p.channel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: corrID,
		ReplyTo:       replyQueue.Name,
		Body:          body,
	})
	if err != nil {
		return nil, err
	}
	fmt.Println("[RabbitMQ] Published with CorrelationId:", corrID)

	// Wait for the reply with a matching CorrelationId
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
