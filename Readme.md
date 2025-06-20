# RabbitMQ Consumer Framework

Hệ thống tiêu thụ RabbitMQ nâng cao với khả năng tự động reconnect cả **connection** và **channel**, hỗ trợ chạy nhiều consumer đồng thời chia sẻ một kết nối TCP duy nhất.

---

## 📦 Tính năng nổi bật

* **Tự động reconnect connection:** Khi kết nối bị gián đoạn (ví dụ: RabbitMQ restart), hệ thống sẽ tự động kết nối lại.
* **Tự động reconnect channel:** Mỗi `Consumer` tự giám sát kênh riêng và tự mở lại nếu channel bị đóng.
* **Phân tách rõ ràng:** `RabbitManager` quản lý kết nối và chia sẻ cho nhiều `Consumer`. Mỗi `Consumer` tự xử lý message của mình.
* **TLS hỗ trợ đầy đủ:** Kết nối có thể sử dụng TLS mutual authentication.
* **Khai báo topology idempotent:** Queue, Exchange và Binding được khai báo lại an toàn mỗi lần reconnect.

---

## 📁 Cấu trúc chính

```go
RabbitManager  // Quản lý connection duy nhất tới RabbitMQ
├── Register()  // Đăng ký các handler xử lý message
├── Listen()    // Khởi động tất cả consumer và giám sát connection
└── Stop()      // Dừng toàn bộ goroutine và đóng kết nối

Consumer       // Tương ứng với một hàng đợi và một handler
├── run()       // Vòng lặp giám sát và reconnect channel
├── openChannel() // Mở mới channel, setup topology và bắt đầu consume
└── handleDelivery() // Gọi handler, xử lý RPC response và ack/nack
```

---

## ⚙️ Cấu hình

### Config

```go
type Config struct {
  RabbitMQ RabbitMQConfig
}

type RabbitMQConfig struct {
  URL            string        // amqp(s)://user:pass@host:port/vhost
  TLS            bool
  ReconnectDelay time.Duration // delay ban đầu giữa các lần reconnect
  Prefetch       int           // fallback prefetch nếu queue không có cấu hình riêng
  ClientCert     string        // path tới client.crt (nếu TLS)
  ClientKey      string        // path tới client.key (nếu TLS)
  CACert         string        // path tới ca.crt (nếu TLS)
  Queues         map[string]QueueConfig
}

type QueueConfig struct {
  Queue, Exchange, BindingKey, ConsumerTag string
  Durable, AutoDelete, Passive, AutoAck    bool
  Prefetch                                 int
  RequeueOnFail                            bool
  Arguments                                amqp.Table
}
```

---

## 🚀 Cách sử dụng

### 1. Cài đặt

```bash
go get github.com/xhkzeroone/consumer
```

### 2. Tạo handler

```go
type LogHandler struct{}

func (LogHandler) Handle(body []byte) ([]byte, error) {
  log.Printf("Received: %s", string(body))
  return nil, nil
}
```

### 3. Khởi động consumer

```go
cfg := &consumer.Config{
  RabbitMQ: consumer.RabbitMQConfig{
    URL: "amqp://guest:guest@localhost:5672/",
    ReconnectDelay: 5 * time.Second,
    Queues: map[string]consumer.QueueConfig{
      "LogHandler": {
        Queue:       "logs",
        Exchange:    "",
        ConsumerTag: "log-consumer",
        AutoAck:     false,
        Durable:     true,
        RequeueOnFail: true,
      },
    },
  },
}

mgr := consumer.New(cfg)
mgr.Register(&LogHandler{})

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

if err := mgr.Listen(ctx); err != nil {
  log.Fatal(err)
}
```

---

## 📌 Ghi chú triển khai

* **Tên handler** (`getStructName`) cần khớp với key trong `cfg.RabbitMQ.Queues`.
* **Connection và channel** đều sử dụng `NotifyClose` để phát hiện gián đoạn.
* **Backoff reconnect** có jitter ngẫu nhiên nhằm giảm khả năng reconnect đồng loạt.
* **Handler panic-safe:** Dù panic trong business logic, hệ thống vẫn tiếp tục hoạt động.

---

## 🛡️ Giấy phép

MIT License. Bản quyền (c) 2025 - Open Source Maintainers.
