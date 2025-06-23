// Package consumer cung cấp các tiện ích (RabbitManager, Consumer, ...) để
// xây dựng hệ thống tiêu thụ RabbitMQ có khả năng tự động reconnect cả
// connection và channel.
//
// Triết lý thiết kế:
//
//  1. *RabbitManager* duy trì **một** kết nối (*amqp.Connection*) và chia sẻ
//     kết nối này cho nhiều *Consumer* dưới dạng các kênh AMQP độc lập.
//  2. Mỗi *Consumer* **tự giám sát** kênh của chính mình (NotifyClose) và chủ
//     động mở lại kênh khi bị đóng, KHÔNG cần tác động tới connection.
//  3. *RabbitManager* giám sát connection gốc và tự động khôi phục lại khi
//     kết nối bị mất; việc reconnect được thực hiện với chiến lược back‑off
//     có jitter để tránh bão reconnect.
//  4. Toàn bộ mã nguồn được bình luận chi tiết bằng tiếng Việt và tuân thủ
//     quy tắc "Go doc":
//     • Mọi tên xuất (exported) đều có chú thích bắt đầu bằng chính tên đó.
//     • Tên private quan trọng cũng có chú thích ngắn gọn khi cần.
//
// Cách sử dụng tối thiểu:
//
//	mgr := consumer.New(cfg)
//	mgr.Register(&MyLogHandler{})
//	ctx := context.Background()
//	if err := mgr.Listen(ctx); err != nil { ... }
//
// Trong đó *MyLogHandler* cần triển khai interface *Handler* với phương thức
// Handle([]byte) ([]byte, error).
//
// Lưu ý: File này KHÔNG chứa định nghĩa của *QueueConfig*, *Config* ... – chúng
// được khai báo ở một module khác (phần placeholder bên dưới chỉ để minh hoạ).
package consumer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"

	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*  --------------------------------------------------------------------------
    🐇  RABBIT MANAGER V2 – TỰ ĐỘNG RECONNECT CONNECTION + CHANNEL
    --------------------------------------------------------------------------
    Khác biệt chính so với V1:
      1. Mỗi Consumer **tự giám sát channel riêng** và tự mở lại (re‑open)
         khi channel bị đóng, KHÔNG cần đóng/mở lại connection.
      2. RabbitManager vẫn chịu trách nhiệm giám sát **connection gốc**
         và tự động reconnect khi connection bị drop.
      3. Dùng RWMutex để **chia sẻ an toàn** connection cho nhiều Consumer.
*/

// ============================================================================
// 👉 Các kiểu dữ liệu placeholder (đã định nghĩa ở module khác)
// ============================================================================
/*
   Những kiểu dưới đây chỉ để trình biên dịch biết; hãy thay thế bằng bản gốc
   trong dự án thực tế.
*/

// type QueueConfig struct {
// 	Queue, Exchange, BindingKey, ConsumerTag string
// 	Durable, AutoDelete, Passive, AutoAck    bool
// 	Prefetch                                 int
// 	RequeueOnFail                            bool
// 	Arguments                                amqp.Table
// }
//
// type Config struct {
// 	RabbitMQ RabbitMQConfig
// }
//
// type RabbitMQConfig struct {
// 	URL            string
// 	TLS            bool
// 	ReconnectDelay time.Duration
// 	Prefetch       int
// 	ClientCert     string
// 	ClientKey      string
// 	CACert         string
// 	Queues         map[string]QueueConfig
// }
//
// type HandlerFunc func([]byte) ([]byte, error)

// Handler định nghĩa interface tối thiểu mà ứng dụng cần implement để xử lý
// message nhận được.
//
// Hàm Handle nhận *payload* dưới dạng []byte, trả về []byte (có thể nil) làm
// response RPC cùng error (nếu có).
//
// Comment này dùng làm ví dụ cho builder *go doc* – trong dự án thật, Handler
// thường đã được định nghĩa ở package khác.
//
//go:generate mockgen -destination=mocks_test.go -package=consumer . Handler
// (ghi chú go:generate chỉ minh hoạ)
// type Handler interface{ Handle([]byte) ([]byte, error) }

// getStructName trả về tên kiểu cụ thể (không bao gồm package) của một giá trị
// triển khai interface Handler. Hàm này thường được định nghĩa ở package utils.
// func getStructName(h Handler) string { return "" /* placeholder */ }

// ============================================================================
// 🌐 STRUCT RabbitManager – quản trị **connection** chia sẻ cho nhiều Consumer
// ============================================================================

// Logger interface cho phép inject logger custom (vd: slog, zap, logrus)
type Logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

// logger mặc định dùng log.Printf
type stdLogger struct{}

func (l *stdLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}
func (l *stdLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}
func (l *stdLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[DEBUG] "+format, args...)
}

// RabbitManager giữ một kết nối duy nhất tới RabbitMQ và phân phát kết nối đó
// cho nhiều Consumer dưới dạng các channel. Nó giám sát connection gốc và tự
// động reconnect khi connection bị đóng.
//
//	╭──────────────╮           ╭──────────────╮
//	│ RabbitManager│──────────▶│  Connection  │ 1 connection, share TCP socket
//	╰──────────────╯           ╰──────────────╯
//	       │                         ▲
//	       ├── channel (Consumer A)  │
//	       ├── channel (Consumer B)  │ multiple AMQP channels (multiplexed)
//	       └── …                     │
//
// Zero value của *RabbitManager* KHÔNG hợp lệ; hãy dùng hàm New để khởi tạo.
// Sau khi tạo, người dùng cần:
//  1. gọi (*RabbitManager).Register(...) để đăng ký các Handler;
//  2. gọi (*RabbitManager).Listen(ctx) để bắt đầu lắng nghe.
//
// Khi không còn nhu cầu, hãy gọi (*RabbitManager).Stop() để dừng toàn bộ goroutine.
type RabbitManager struct {
	cfg *Config

	// tài nguyên AMQP
	conn *amqp.Connection
	lock sync.RWMutex // bảo vệ conn khi truy cập đồng thời

	handlers  map[string]HandlerFunc // map tên struct → handler
	consumers []*Consumer            // danh sách Consumer đã khởi tạo

	ctx    context.Context // context gốc (huỷ để stop)
	cancel context.CancelFunc

	wg     sync.WaitGroup // đợi các consumer dừng
	logger Logger         // logger custom
}

// New tạo mới một RabbitManager với cấu hình cfg nhưng CHƯA mở kết nối tới
// RabbitMQ. Hàm trả về con trỏ quản lý – việc kết nối sẽ được thực hiện trong
// phương thức Listen.
func New(cfg *Config, logger ...Logger) *RabbitManager {
	var l Logger
	if len(logger) > 0 && logger[0] != nil {
		l = logger[0]
	} else {
		l = &stdLogger{}
	}
	return &RabbitManager{
		cfg:      cfg,
		handlers: make(map[string]HandlerFunc),
		logger:   l,
	}
}

// Register đăng ký một hoặc nhiều Handler. Mỗi Handler được ánh xạ bởi tên
// struct cụ thể (lấy thông qua getStructName). Nếu trùng tên, Handler sau sẽ
// ghi đè Handler trước.
func (m *RabbitManager) Register(h ...Handler) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, hd := range h {
		n := getStructName(hd)
		m.handlers[n] = hd.Handle
	}
}

// Listen khởi tạo các Consumer (theo cấu hình QueueConfig) và chạy vòng lặp
// giám sát connection. Phương thức BLOCKS tới khi:
//   - Context bị huỷ (ctx.Done())
//   - hoặc có lỗi nghiêm trọng xảy ra khi khởi tạo Consumer.
func (m *RabbitManager) Listen(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.ctx, m.cancel = context.WithCancel(ctx)

	// 1) Thử kết nối một lần ngay đầu
	if err := m.connect(); err != nil {
		m.logger.Errorf("[RabbitMQ] initial dial failed: %v (will retry in loop)", err)
	}

	// 2) Tạo & chạy consumer SAU khi (đã hoặc sẽ) có connection
	for key, qc := range m.cfg.RabbitMQ.Queues {
		hd, ok := m.handlers[key]
		if !ok { // không có handler tương ứng
			continue
		}
		c := &Consumer{cfg: qc, name: key, handler: hd, logger: m.logger}
		c.setConn(m.conn)
		m.consumers = append(m.consumers, c)
		m.wg.Add(1)
		go c.run(m.ctx, &m.wg) // truyền WaitGroup
	}
	if len(m.consumers) == 0 {
		return errors.New("no consumers to start")
	}

	// 3) Tiếp tục vòng lặp reconnect như cũ
	return m.connectionLoop()
}

// ---------------------------------------------------------------------------
// connectionLoop – tự động Dial + NotifyClose + back‑off
// ---------------------------------------------------------------------------
//
// Hàm BLOKING: chạy tới khi ctx.Done().
func (m *RabbitManager) connectionLoop() error {
	backoff := m.cfg.RabbitMQ.ReconnectDelay
	if backoff == 0 {
		backoff = 5 * time.Second
	}
	max := time.Minute

	for {
		// thử kết nối
		if err := m.connect(); err != nil {
			m.logger.Errorf("[RabbitMQ] dial error: %v", err)
		} else {
			m.logger.Infof("[RabbitMQ] connection established (%d consumers)", len(m.consumers))
			connClosed := make(chan *amqp.Error, 1)
			m.conn.NotifyClose(connClosed)
			select {
			case <-m.ctx.Done():
				m.closeConn()
				return nil
			case err := <-connClosed:
				m.logger.Errorf("[RabbitMQ] connection closed: %v", err)
			}
		}
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		delay := backoff + jitter/2
		select {
		case <-time.After(delay):
		case <-m.ctx.Done():
			return nil
		}
		if backoff < max {
			backoff *= 2
			if backoff > max {
				backoff = max
			}
		}
	}
}

// ---------------------------------------------------------------------------
// connect – dial RabbitMQ, cập nhật connection cho các Consumer
// ---------------------------------------------------------------------------
func (m *RabbitManager) connect() error {
	var (
		conn *amqp.Connection
		err  error
	)
	url := m.cfg.RabbitMQ.URL
	if m.cfg.RabbitMQ.TLS {
		tlsCfg, tlsErr := loadTLSConfig(&m.cfg.RabbitMQ)
		if tlsErr != nil {
			return tlsErr
		}
		conn, err = amqp.DialTLS(url, tlsCfg)
	} else {
		conn, err = amqp.Dial(url)
	}
	if err != nil {
		return err
	}

	// lưu connection
	m.lock.Lock()
	m.conn = conn
	m.lock.Unlock()

	// cập nhật connection cho tất cả Consumer (channel sẽ tự mở)
	for _, c := range m.consumers {
		c.setConn(conn)
	}
	return nil
}

// closeConn đóng connection hiện tại nếu đang mở.
func (m *RabbitManager) closeConn() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}
}

// Stop huỷ context gốc, từ đó kết thúc mọi goroutine được sinh ra bởi
// RabbitManager và Consumer.
func (m *RabbitManager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait() // đợi các consumer dừng
}

// ============================================================================
// 🧩 STRUCT Consumer – 1 consumer tương ứng 1 queue + handler
// ============================================================================

// Trạng thái consumer

type ConsumerStatus int

const (
	ConsumerStopped ConsumerStatus = iota
	ConsumerWaiting
	ConsumerRunning
	ConsumerError
)

func (s ConsumerStatus) String() string {
	switch s {
	case ConsumerStopped:
		return "stopped"
	case ConsumerWaiting:
		return "waiting"
	case ConsumerRunning:
		return "running"
	case ConsumerError:
		return "error"
	default:
		return "unknown"
	}
}

// Consumer đại diện cho một worker tiêu thụ một hàng đợi cụ thể. Mỗi Consumer
// có channel AMQP riêng (multiplexed trong cùng connection) và tự động khôi
// phục channel khi gặp sự cố. Việc khởi tạo Consumer được thực hiện bởi
// RabbitManager.
type Consumer struct {
	// cấu hình hàng đợi
	cfg     QueueConfig
	name    string      // tên (khóa) của consumer – thường khớp với struct handler
	handler HandlerFunc // hàm xử lý message

	// tài nguyên AMQP
	ch     *amqp.Channel    // channel hiện tại
	notify chan *amqp.Error // nhận tín hiệu NotifyClose của channel

	// bảo vệ truy cập connection (được RabbitManager share cho nhiều Consumer)
	mu   sync.RWMutex
	conn *amqp.Connection

	status   ConsumerStatus
	statusMu sync.RWMutex
	logger   Logger
}

// ===== helper set/get connection an toàn =====
func (c *Consumer) setConn(conn *amqp.Connection) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
}
func (c *Consumer) getConn() *amqp.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

// ---------------------------------------------------------------------------
// run – vòng lặp FI x X tự giám sát channel
// ---------------------------------------------------------------------------
func (c *Consumer) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	base, max := 5*time.Second, time.Minute
	backoff := time.Duration(0)
	for {
		c.setStatus(ConsumerWaiting)
		conn := c.getConn()
		if conn == nil || conn.IsClosed() {
			select {
			case <-time.After(base):
				continue
			case <-ctx.Done():
				c.setStatus(ConsumerStopped)
				return
			}
		}
		if err := c.openChannel(); err != nil {
			if backoff == 0 {
				c.logger.Debugf("[%s] waiting for connection…", c.name)
			} else {
				c.logger.Errorf("[%s] openChannel error: %v", c.name, err)
			}
			c.setStatus(ConsumerError)
		} else {
			backoff = base
			c.setStatus(ConsumerRunning)
			select {
			case <-ctx.Done():
				c.closeChannel()
				c.setStatus(ConsumerStopped)
				return
			case err := <-c.notify:
				if err != nil {
					c.logger.Errorf("[%s] channel closed: %v", c.name, err)
				}
				c.setStatus(ConsumerError)
			}
		}
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		delay := backoff + jitter/2
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			c.setStatus(ConsumerStopped)
			return
		}
		if backoff < max {
			backoff *= 2
			if backoff > max {
				backoff = max
			}
		}
	}
}

// ---------------------------------------------------------------------------
// openChannel – mở channel mới, khai báo topology và khởi chạy consumeLoop
// ---------------------------------------------------------------------------
func (c *Consumer) openChannel() error {
	conn := c.getConn()
	if conn == nil || conn.IsClosed() {
		return errors.New("connection not ready")
	}

	// mở channel
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	// cài đặt QoS (prefetch)
	prefetch := c.cfg.Prefetch
	if prefetch > 0 {
		if err = ch.Qos(prefetch, 0, false); err != nil {
			return err
		}
	}

	// khai báo queue + exchange (idempotent, nên gọi mỗi lần reconnect)
	if err = declareTopology(ch, &c.cfg); err != nil {
		return err
	}

	// cập nhật trạng thái
	c.ch = ch
	c.notify = ch.NotifyClose(make(chan *amqp.Error, 1))

	// chạy goroutine tiêu thụ message
	go c.consumeLoop()
	c.logger.Infof("[%s] channel (re)started", c.name)
	return nil
}

// closeChannel đóng channel hiện tại nếu có.
func (c *Consumer) closeChannel() {
	if c.ch != nil {
		_ = c.ch.Close()
		c.ch = nil
	}
}

// ---------------------------------------------------------------------------
// consumeLoop – đăng ký Consume và đọc message liên tục
// ---------------------------------------------------------------------------
func (c *Consumer) consumeLoop() {
	msgs, err := c.ch.Consume(
		c.cfg.Queue,
		c.cfg.ConsumerTag,
		c.cfg.AutoAck,
		false,
		false, false, nil,
	)
	if err != nil {
		c.logger.Errorf("[%s] consume error: %v", c.name, err)
		return
	}
	for m := range msgs { // vòng lặp vô hạn tới khi channel đóng
		c.handleDelivery(&m)
	}
}

// ---------------------------------------------------------------------------
// handleDelivery – gọi business handler, trả lời RPC (nếu có) & ack/nack
// ---------------------------------------------------------------------------
func (c *Consumer) handleDelivery(m *amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Errorf("[%s] panic: %v", c.name, r)
			_ = m.Nack(false, c.cfg.RequeueOnFail)
		}
	}()
	if c.handler == nil {
		c.logger.Errorf("[%s] handler is nil, message dropped", c.name)
		_ = m.Nack(false, c.cfg.RequeueOnFail)
		return
	}
	resp, err := c.handler(m.Body)
	if err != nil {
		c.logger.Errorf("[%s] handler error: %v", c.name, err)
		_ = m.Nack(false, c.cfg.RequeueOnFail)
		return
	}

	// nếu đây là RPC request (ReplyTo != ""), publish trả lời
	if m.ReplyTo != "" && resp != nil {
		timeout := 5 * time.Second // Giá trị mặc định
		if c.cfg.RPCTimeout > 0 {
			timeout = c.cfg.RPCTimeout
		}
		publishCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := c.ch.PublishWithContext(publishCtx, "", m.ReplyTo, false, false, amqp.Publishing{CorrelationId: m.CorrelationId, ContentType: "application/json", Body: resp}); err != nil {
			c.logger.Errorf("[%s] reply publish error: %v", c.name, err)
		}
	}
	if !c.cfg.AutoAck {
		_ = m.Ack(false)
	}
}

func (c *Consumer) setStatus(s ConsumerStatus) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.status = s
}
func (c *Consumer) GetStatus() ConsumerStatus {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status
}

// ============================================================================
// 🛠️ Helper: khai báo topology (exchange / queue / binding) idempotent
// ============================================================================
func declareTopology(ch *amqp.Channel, q *QueueConfig) error {
	if q.Passive {
		_, err := ch.QueueDeclarePassive(
			q.Queue, q.Durable, q.AutoDelete, false, false, q.Arguments,
		)
		return err
	}

	// 1. Exchange (nếu có)
	if q.Exchange != "" {
		exType := "direct"
		if q.BindingKey == "" { // không có routing key ⇒ fanout
			exType = "fanout"
		}
		if err := ch.ExchangeDeclare(
			q.Exchange, exType, q.Durable, q.AutoDelete,
			false, false, nil,
		); err != nil {
			return err
		}
	}

	// 2. Queue
	if _, err := ch.QueueDeclare(
		q.Queue, q.Durable, q.AutoDelete, false, false, q.Arguments,
	); err != nil {
		return err
	}

	// 3. Bind queue ↔ exchange (nếu có)
	if q.Exchange != "" {
		if err := ch.QueueBind(
			q.Queue, q.BindingKey, q.Exchange, false, nil,
		); err != nil {
			return err
		}
	}
	return nil
}

// ============================================================================
// 🔐 Helper: loadTLSConfig – đọc file PEM & tạo *tls.Config
// ============================================================================
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

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}
