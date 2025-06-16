package consumer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math"
	"os"
	"sync"
	"time"
)

// === Consumer & Manager ===

// Consumer represents a logical subscription.
// All fields except Handler come from QueueConfig.
type Consumer struct {
	cfg     QueueConfig
	name    string
	ch      *amqp.Channel
	handler HandlerFunc
}

// RabbitManager owns a single connection and N consumer channels.
type RabbitManager struct {
	cfg         *Config
	conn        *amqp.Connection
	lock        sync.RWMutex
	handlers    map[string]HandlerFunc
	consumers   []*Consumer
	notifyClose chan *amqp.Error
	ctx         context.Context
	cancel      context.CancelFunc
}

// New builds manager with config (URL may be overridden later).
func New(cfg *Config) *RabbitManager {
	return &RabbitManager{
		cfg:      cfg,
		handlers: make(map[string]HandlerFunc),
	}
}

// Register registers one or more struct handlers. The struct name (lowercase)
// must match key in cfg.RabbitMQ.Queues.
func (m *RabbitManager) Register(h ...Handler) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, hd := range h {
		name := getStructName(hd)
		m.handlers[name] = hd.Handle
		log.Printf("[RabbitMQ] registered handler for %s", name)
	}
}

// Listen establishes connection, creates consumers and blocks until ctx done.
func (m *RabbitManager) Listen(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.ctx, m.cancel = context.WithCancel(ctx)

	// Build consumer list from cfg & registered handlers.
	for key, qc := range m.cfg.RabbitMQ.Queues {
		h, ok := m.handlers[key]
		if !ok {
			log.Printf("[RabbitMQ] No handler registered for %s – skip", key)
			continue
		}
		c := &Consumer{cfg: qc, name: key, handler: h}
		m.consumers = append(m.consumers, c)
	}

	if len(m.consumers) == 0 {
		return errors.New("no consumers to start")
	}

	// Keep reconnecting until context cancelled.
	backoff := m.cfg.RabbitMQ.ReconnectDelay
	if backoff == 0 {
		backoff = 5 * time.Second
	}

	for {
		if err := m.connectOnce(); err != nil {
			log.Printf("[RabbitMQ] connect error: %v", err)
		} else {
			// wait for connection close or ctx cancel
			select {
			case <-m.ctx.Done():
				m.closeConn()
				return nil
			case err := <-m.notifyClose:
				if err != nil {
					log.Printf("[RabbitMQ] connection closed: %v", err)
				}
			}
		}
		// exponential backoff w/ cap 1m
		select {
		case <-time.After(backoff):
		case <-m.ctx.Done():
			return nil
		}
		backoff = time.Duration(math.Min(float64(backoff*2), float64(time.Minute)))
	}
}

// Stop cancels internal context (graceful shutdown).
func (m *RabbitManager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}

// connectOnce dials and starts all consumers.
func (m *RabbitManager) connectOnce() error {
	var conn *amqp.Connection
	var err error

	url := m.cfg.RabbitMQ.URL
	if m.cfg.RabbitMQ.TLS {
		tlsCfg, errTLS := loadTLSConfig(&m.cfg.RabbitMQ)
		if errTLS != nil {
			return errTLS
		}
		conn, err = amqp.DialTLS(url, tlsCfg)
	} else {
		conn, err = amqp.Dial(url)
	}
	if err != nil {
		return err
	}

	m.lock.Lock()
	m.conn = conn
	m.notifyClose = conn.NotifyClose(make(chan *amqp.Error, 1))
	m.lock.Unlock()

	// open channel & start each consumer
	for _, c := range m.consumers {
		if err = m.initConsumer(c); err != nil {
			m.closeConn()
			return err
		}
	}
	log.Printf("[RabbitMQ] connection established (%d consumers)", len(m.consumers))
	return nil
}

func (m *RabbitManager) closeConn() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}
}

func (m *RabbitManager) initConsumer(c *Consumer) error {
	ch, err := m.conn.Channel()
	if err != nil {
		return err
	}
	// prefetch
	prefetch := c.cfg.Prefetch
	if prefetch == 0 {
		prefetch = m.cfg.RabbitMQ.Prefetch
	}
	if prefetch > 0 {
		if err = ch.Qos(prefetch, 0, false); err != nil {
			return err
		}
	}
	// declare topology or verify only
	if err = declareTopology(ch, &c.cfg); err != nil {
		return err
	}

	c.ch = ch
	go c.consumeLoop(m.ctx)
	return nil
}

// === Consumer implementation ===

func declareTopology(ch *amqp.Channel, q *QueueConfig) error {
	if q.Passive {
		_, err := ch.QueueDeclarePassive(q.Queue, q.Durable, q.AutoDelete, false, false, q.Arguments)
		return err
	}
	// declare exchange if provided
	if q.Exchange != "" {
		exType := "direct"
		if q.BindingKey == "" {
			exType = "fanout"
		}
		if err := ch.ExchangeDeclare(q.Exchange, exType, q.Durable, q.AutoDelete, false, false, nil); err != nil {
			return err
		}
	}
	_, err := ch.QueueDeclare(q.Queue, q.Durable, q.AutoDelete, false, false, q.Arguments)
	if err != nil {
		return err
	}
	if q.Exchange != "" {
		if err = ch.QueueBind(q.Queue, q.BindingKey, q.Exchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *Consumer) consumeLoop(ctx context.Context) {
	msgs, err := c.ch.Consume(c.cfg.Queue, c.cfg.ConsumerTag, c.cfg.AutoAck, false, false, false, nil)
	if err != nil {
		log.Printf("[%s] consume error: %v", c.name, err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-msgs:
			if !ok {
				return
			}
			c.handleDelivery(&m)
		}
	}
}

func (c *Consumer) handleDelivery(m *amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[%s] panic: %v", c.name, r)
			_ = m.Nack(false, c.cfg.RequeueOnFail)
		}
	}()
	resp, err := c.handler(m.Body)
	if err != nil {
		log.Printf("[%s] handler error: %v", c.name, err)
		_ = m.Nack(false, c.cfg.RequeueOnFail)
		return
	}
	// reply if needed
	if m.ReplyTo != "" && resp != nil {
		if publishErr := c.ch.PublishWithContext(context.TODO(), "", m.ReplyTo, false, false, amqp.Publishing{
			CorrelationId: m.CorrelationId,
			ContentType:   "application/json",
			Body:          resp,
		}); publishErr != nil {
			log.Printf("[%s] reply publish error: %v", c.name, publishErr)
			_ = m.Nack(false, true)
			return
		}
	}
	if !c.cfg.AutoAck {
		_ = m.Ack(false)
	}
}

// === TLS helper ===

func loadTLSConfig(cfg *RabbitMQConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
	if err != nil {
		return nil, err
	}
	caPem, err := os.ReadFile(cfg.CACert)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPem)
	return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}
