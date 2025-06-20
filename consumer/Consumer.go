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
//     quy tắc "Go doc":
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

	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
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
// Comment này dùng làm ví dụ cho builder *go doc* – trong dự án thật, Handler
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
}

// New tạo mới một RabbitManager với cấu hình cfg nhưng CHƯA mở kết nối tới
// RabbitMQ. Hàm trả về con trỏ quản lý – việc kết nối sẽ được thực hiện trong
// phương thức Listen.
func New(cfg *Config) *RabbitManager {
	return &RabbitManager{
		cfg:      cfg,
		handlers: make(map[string]HandlerFunc),
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
		log.Printf("[RabbitMQ] initial dial failed: %v (will retry in loop)", err)
	}

	// 2) Tạo & chạy consumer SAU khi (đã hoặc sẽ) có connection
	for key, qc := range m.cfg.RabbitMQ.Queues {
		hd, ok := m.handlers[key]
		if !ok { // không có handler tương ứng
			continue
		}
		c := &Consumer{cfg: qc, name: key, handler: hd}
		// gán conn hiện tại (có thể nil); sau này connectionLoop sẽ update
		c.setConn(m.conn)
		m.consumers = append(m.consumers, c)
		go c.run(m.ctx) // mỗi consumer tự giám sát channel
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
			log.Printf("[RabbitMQ] dial error: %v", err)
		} else {
			log.Printf("[RabbitMQ] connection established (%d consumers)", len(m.consumers))
			// chờ tới khi có sự cố
			connClosed := make(chan *amqp.Error, 1)
			m.conn.NotifyClose(connClosed)
			select {
			case <-m.ctx.Done(): // ứng dụng dừng
				m.closeConn()
				return nil
			case err := <-connClosed: // connection bị drop
				log.Printf("[RabbitMQ] connection closed: %v", err)
			}
		}

		// exponential back‑off
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
}

// ============================================================================
// 🧩 STRUCT Consumer – 1 consumer tương ứng 1 queue + handler
// ============================================================================

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
func (c *Consumer) run(ctx context.Context) {
	base, max := 5*time.Second, time.Minute
	backoff := time.Duration(0) // =0 để lần đầu không log lỗi

	for {
		// Chờ connection
		conn := c.getConn()
		if conn == nil || conn.IsClosed() {
			select {
			case <-time.After(base):
				continue // chờ tiếp
			case <-ctx.Done():
				return
			}
		}

		// Đã có connection → thử mở channel
		if err := c.openChannel(); err != nil {
			if backoff == 0 { // lần đầu, ghi log dạng Debug thay vì Error
				log.Printf("[%s] waiting for connection…", c.name)
			} else {
				log.Printf("[%s] openChannel error: %v", c.name, err)
			}
		} else {
			backoff = base // reset sau khi thành công
			// 2️⃣ chờ tới khi channel đóng hoặc context bị huỷ
			select {
			case <-ctx.Done():
				c.closeChannel()
				return
			case err := <-c.notify: // channel NotifyClose
				if err != nil {
					log.Printf("[%s] channel closed: %v", c.name, err)
				}
			}
		}

		// 3️⃣ back‑off có jitter trước khi thử lại
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		delay := backoff + jitter/2
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}

		// 4️⃣ nhân đôi back‑off tới ngưỡng max
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

	log.Printf("[%s] channel (re)started", c.name)
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
		c.cfg.Queue,       // queue
		c.cfg.ConsumerTag, // consumer tag
		c.cfg.AutoAck,     // auto‑ack?
		false,             // exclusive
		false, false, nil, // no‑local, no‑wait, args
	)
	if err != nil {
		log.Printf("[%s] consume error: %v", c.name, err)
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
	// chặn panic để không giết goroutine
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[%s] panic: %v", c.name, r)
			_ = m.Nack(false, c.cfg.RequeueOnFail)
		}
	}()

	// gọi hàm xử lý chính
	resp, err := c.handler(m.Body)
	if err != nil {
		log.Printf("[%s] handler error: %v", c.name, err)
		_ = m.Nack(false, c.cfg.RequeueOnFail)
		return
	}

	// nếu đây là RPC request (ReplyTo != ""), publish trả lời
	if m.ReplyTo != "" && resp != nil {
		publishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.ch.PublishWithContext(publishCtx,
			"", m.ReplyTo, false, false,
			amqp.Publishing{
				CorrelationId: m.CorrelationId,
				ContentType:   "application/json",
				Body:          resp,
			}); err != nil {
			log.Printf("[%s] reply publish error: %v", c.name, err)
			_ = m.Nack(false, true) // requeue để thử lại
			return
		}
	}

	// cuối cùng, ack nếu không AutoAck
	if !c.cfg.AutoAck {
		_ = m.Ack(false)
	}
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
