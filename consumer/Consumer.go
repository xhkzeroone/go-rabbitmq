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

// ======================= Phần Định nghĩa Consumer & Manager =======================

// Consumer đại diện cho một subscription logic tới hàng đợi (queue) cụ thể.
// Mỗi Consumer tương ứng đúng 1 queue hoặc 1 cặp exchange/queue/binding.
//
// ➡️  Field giải thích:
//   - cfg     : Cấu hình hàng đợi được đọc từ file cấu hình bên ngoài.
//   - name    : Tên ngắn gọn của consumer (thường trùng với key trong file config).
//   - ch      : Channel AMQP dành riêng cho consumer này (mỗi consumer 1 channel).
//   - handler : Hàm xử lý message, được inject thông qua phương thức Register().
//
// Lý do KHÔNG lưu ctx vào Consumer: Context chung được cấp từ RabbitManager,
// và Consumer chỉ đọc <-ctx.Done() bên trong consumeLoop.
// Tách riêng tránh circular reference & đơn giản hoá unit‑test.
type Consumer struct {
	cfg     QueueConfig
	name    string
	ch      *amqp.Channel
	handler HandlerFunc
}

// RabbitManager sở hữu một kết nối (connection) duy nhất tới RabbitMQ
// và quản lý N channel (mỗi channel gắn với một Consumer).
//
//	╭──────────────╮           ╭──────────────╮
//	│ RabbitManager│──────────▶│  Connection  │ 1 connection, share TCP socket
//	╰──────────────╯           ╰──────────────╯
//	       │                         ▲
//	       ├── channel (Consumer A)  │
//	       ├── channel (Consumer B)  │ multiple AMQP channels (multiplexed)
//	       └── …                     │
//
// Việc dùng 1 connection giúp tiết kiệm tài nguyên (TCP, TLS handshake).
// Khi connection bị drop, Manager sẽ tự reconnect & khởi tạo lại toàn bộ channel.
type RabbitManager struct {
	cfg         *Config                // Cấu hình gốc (chứa thông tin RabbitMQ & toàn app)
	conn        *amqp.Connection       // Kết nối thực tới broker
	lock        sync.RWMutex           // Bảo vệ conn + map handler khỏi race‑condition
	handlers    map[string]HandlerFunc // Map tên → hàm xử lý đăng ký bởi Register()
	consumers   []*Consumer            // Danh sách consumer đã build từ config
	notifyClose chan *amqp.Error       // Channel nhận tín hiệu đóng kết nối của AMQP driver
	ctx         context.Context        // Context gốc được truyền khi Listen()
	cancel      context.CancelFunc     // Hàm huỷ context, dùng cho Stop()
}

// New khởi tạo RabbitManager với cấu hình ban đầu.
// (URL có thể được override ở runtime nếu cần.)
func New(cfg *Config) *RabbitManager {
	return &RabbitManager{
		cfg:      cfg,
		handlers: make(map[string]HandlerFunc),
	}
}

// ======================= Đăng ký handler =======================

// Register nhận 1..N struct implement giao diện Handler.
// Tên struct (viết thường) phải khớp key cấu hình để ánh xạ đúng queue.
func (m *RabbitManager) Register(h ...Handler) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, hd := range h {
		name := getStructName(hd)
		m.handlers[name] = hd.Handle
		log.Printf("[RabbitMQ] registered handler for %s", name)
	}
}

// ======================= Vòng đời chính: Listen & Stop =======================

// Listen thiết lập kết nối, khởi tạo consumer và block tới khi ctx bị cancel.
// Tự động reconnect với backoff lũy tiến.
func (m *RabbitManager) Listen(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.ctx, m.cancel = context.WithCancel(ctx)

	// 1️⃣ Build danh sách consumer từ file cấu hình & các handler đã đăng ký.
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

	return m.reconnectLoop()
}

func (m *RabbitManager) reconnectLoop() error {
	// 2️⃣ Tính backoff (time.Sleep) khi reconnect, mặc định 5s.
	baseBackoff := m.cfg.RabbitMQ.ReconnectDelay
	if baseBackoff == 0 {
		baseBackoff = 5 * time.Second
	}
	maxBackoff := time.Minute
	backoff := baseBackoff

	// 3️⃣ Vòng lặp chính: connect → wait → reconnect nếu lỗi.
	for {
		// Chờ đến khi connection đóng hoặc context hủy.
		if err := m.connectOnce(); err != nil {
			log.Printf("[RabbitMQ] connect error: %v", err)
		} else {
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

		jitter := time.Duration(rand.Int63n(int64(backoff)))
		delay := backoff + jitter/2

		// Exponential backoff, tối đa 1 phút.
		select {
		case <-time.After(delay):
		case <-m.ctx.Done():
			return nil
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// Stop huỷ context nội bộ → các consumer & Listen() sẽ kết thúc gracefully.
func (m *RabbitManager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
}

// ======================= Thiết lập 1 lần kết nối & channel =======================

// connectOnce dial tới RabbitMQ (TLS hoặc TCP), khởi tạo tất cả consumer.
// Nếu lỗi ở bất kỳ bước nào → đóng connection & trả lỗi để Listen() reconnect.
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

	// Lưu connection & notifyClose (phải dưới lock để thread‑safe)
	m.lock.Lock()
	m.conn = conn
	m.notifyClose = conn.NotifyClose(make(chan *amqp.Error, 1))
	m.lock.Unlock()

	// Mở riêng 1 channel cho từng consumer
	for _, c := range m.consumers {
		if err = m.initConsumer(c); err != nil {
			m.closeConn() // Đóng connection nếu init thất bại
			return err
		}
	}
	log.Printf("[RabbitMQ] connection established (%d consumers)", len(m.consumers))
	return nil
}

// closeConn đóng connection an toàn (idempotent).
func (m *RabbitManager) closeConn() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.conn != nil {
		_ = m.conn.Close()
		m.conn = nil
	}
}

// initConsumer mở channel, thiết lập QoS, khai báo topology (queue, exchange).
func (m *RabbitManager) initConsumer(c *Consumer) error {
	ch, err := m.conn.Channel()
	if err != nil {
		return err
	}
	// 1️⃣ Thiết lập prefetch (QoS) – ưu tiên giá trị riêng trong QueueConfig.
	prefetch := c.cfg.Prefetch
	if prefetch == 0 {
		prefetch = m.cfg.RabbitMQ.Prefetch
	}
	if prefetch > 0 {
		if err = ch.Qos(prefetch, 0, false); err != nil {
			return err
		}
	}
	// 2️⃣ Khai báo topology (queue/exchange) hoặc verify passive.
	if err = declareTopology(ch, &c.cfg); err != nil {
		return err
	}

	c.ch = ch
	go c.consumeLoop(m.ctx) // Chạy goroutine nhận message.
	return nil
}

// ======================= Phần khai báo Queue / Exchange =======================

// declareTopology đảm bảo queue/exchange tồn tại trước khi Consume.
// Nếu q.Passive = true ➔ chỉ kiểm tra tồn tại, không tạo mới.
func declareTopology(ch *amqp.Channel, q *QueueConfig) error {
	if q.Passive {
		_, err := ch.QueueDeclarePassive(q.Queue, q.Durable, q.AutoDelete, false, false, q.Arguments)
		return err
	}
	// Khai báo exchange (nếu cấu hình)
	if q.Exchange != "" {
		exType := "direct"
		if q.BindingKey == "" {
			exType = "fanout" // Nếu không có bindingKey → dùng fanout.
		}
		if err := ch.ExchangeDeclare(q.Exchange, exType, q.Durable, q.AutoDelete, false, false, nil); err != nil {
			return err
		}
	}
	// Khai báo queue
	_, err := ch.QueueDeclare(q.Queue, q.Durable, q.AutoDelete, false, false, q.Arguments)
	if err != nil {
		return err
	}
	// Bind queue với exchange (nếu có)
	if q.Exchange != "" {
		if err = ch.QueueBind(q.Queue, q.BindingKey, q.Exchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}

// ======================= Vòng lặp tiêu thụ message =======================

func (c *Consumer) consumeLoop(ctx context.Context) {
	// Khởi tạo luồng Consume. AutoAck quyết định driver tự ACK hay thủ công.
	msgs, err := c.ch.Consume(c.cfg.Queue, c.cfg.ConsumerTag, c.cfg.AutoAck, false, false, false, nil)
	if err != nil {
		log.Printf("[%s] consume error: %v", c.name, err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			_ = c.ch.Cancel(c.cfg.ConsumerTag, false)
			return // Dừng graceful khi manager Stop()
		case m, ok := <-msgs:
			if !ok {
				return // Channel bị đóng
			}
			c.handleDelivery(ctx, &m)
		}
	}
}

// handleDelivery gọi business handler, quản lý ACK/NACK & reply (RPC pattern).
func (c *Consumer) handleDelivery(ctx context.Context, m *amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[%s] panic: %v", c.name, r)
			_ = m.Nack(false, c.cfg.RequeueOnFail)
		}
	}()

	// Gọi hàm xử lý từ user (HandlerFunc).
	resp, err := c.handler(m.Body)
	if err != nil {
		log.Printf("[%s] handler error: %v", c.name, err)
		_ = m.Nack(false, c.cfg.RequeueOnFail)
		return
	}

	if m.ReplyTo != "" && resp != nil {
		replyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if publishErr := c.ch.PublishWithContext(replyCtx, "", m.ReplyTo, false, false, amqp.Publishing{
			CorrelationId: m.CorrelationId,
			ContentType:   "application/json",
			Body:          resp,
		}); publishErr != nil {
			log.Printf("[%s] reply publish error: %v", c.name, publishErr)
			_ = m.Nack(false, true) // NACK & requeue để retry gửi reply
			return
		}
	}

	// Thủ công ACK khi xử lý thành công (nếu AutoAck = false).
	if !c.cfg.AutoAck {
		_ = m.Ack(false)
	}
}

// ======================= Helper tải cấu hình TLS =======================

// loadTLSConfig đọc chứng chỉ client + CA, trả về *tls.Config đủ dùng cho DialTLS.
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
