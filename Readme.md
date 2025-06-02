
# RabbitMQ Publisher - Hướng Dẫn Sử Dụng

## Giới Thiệu

Hệ thống `Publisher` giúp bạn gửi tin nhắn tới RabbitMQ với khả năng chờ phản hồi hoặc không chờ phản hồi. Cấu trúc này hỗ trợ việc gửi tin nhắn theo yêu cầu trong các hệ thống phân tán.

## Cài Đặt

Để sử dụng `Publisher`, bạn cần cài đặt các thư viện phụ thuộc sau:

```bash
go get github.com/rabbitmq/amqp091-go
go get github.com/google/uuid
```

## Cấu Hình và Khởi Tạo `Publisher`

Để tạo một `Publisher` mới, bạn cần cung cấp cấu hình kết nối với RabbitMQ. Cấu hình này bao gồm URL kết nối và thời gian timeout.

### Khởi Tạo `Publisher`

```go
cfg := &client.Config{
    URL:     "amqp://guest:guest@localhost:5672/",
    Timeout: 5000,  // Timeout 5 giây
}

publisher, err := client.NewPublisher(cfg)
if err != nil {
    log.Fatalf("Error creating publisher: %v", err)
}
```

### Cấu Hình `Config`

```go
type Config struct {
    URL     string // URL kết nối RabbitMQ
    Timeout int    // Thời gian timeout (ms)
}
```

## Gửi Tin Nhắn mà không Chờ Phản Hồi

Để gửi tin nhắn vào RabbitMQ mà không cần chờ phản hồi, bạn sử dụng hàm `Publish` với `waitForReply = false`.

```go
ctx := context.Background()  // Sử dụng context cơ bản hoặc tạo context mới với timeout

// Nội dung tin nhắn
message := []byte(`{"message": "Hello, RabbitMQ!"}`)

// Gửi tin nhắn mà không cần chờ phản hồi
_, err := publisher.Publish(ctx, "testQueue", message, false)
if err != nil {
    log.Fatalf("Error publishing message: %v", err)
}

log.Println("Message sent successfully!")
```

## Gửi Tin Nhắn và Chờ Phản Hồi

Để gửi tin nhắn và chờ phản hồi từ RabbitMQ, bạn sử dụng hàm `Publish` với `waitForReply = true`.

```go
ctx := context.Background()  // Sử dụng context cơ bản hoặc tạo context mới với timeout

// Nội dung tin nhắn
message := []byte(`{"message": "Hello, RabbitMQ!"}`)

// Gửi tin nhắn và chờ phản hồi
reply, err := publisher.Publish(ctx, "testQueue", message, true)
if err != nil {
    log.Fatalf("Error publishing message: %v", err)
}

// Xử lý phản hồi nếu có
log.Printf("Received reply: %s", reply)
```

## Quản Lý `context` và `timeout`

Phương thức `Publish` sử dụng `context` để giúp bạn quản lý thời gian chờ và hủy bỏ các yêu cầu. Bạn có thể tạo một `context` mới với thời gian timeout như sau:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Gửi tin nhắn và chờ phản hồi
reply, err := publisher.Publish(ctx, "testQueue", message, true)
```

Nếu bạn không cần chờ phản hồi và chỉ muốn gửi tin nhắn, bạn có thể bỏ qua việc tạo `context` hoặc sử dụng `context.Background()`.

## Các Phương Thức Của `Publisher`

### `NewPublisher`

```go
func NewPublisher(cfg *Config) (*Publisher, error)
```

- **Mô tả**: Khởi tạo một đối tượng `Publisher` mới với cấu hình kết nối RabbitMQ.
- **Tham số**:
    - `cfg`: Cấu hình kết nối RabbitMQ.
- **Trả về**:
    - `*Publisher`: Đối tượng Publisher mới.
    - `error`: Lỗi nếu có trong quá trình kết nối.

### `Publish`

```go
func (p *Publisher) Publish(ctx context.Context, queue string, body []byte, waitForReply bool) ([]byte, error)
```

- **Mô tả**: Gửi tin nhắn tới RabbitMQ và có thể chờ phản hồi từ server.
- **Tham số**:
    - `ctx`: Context để quản lý thời gian chờ và hủy bỏ.
    - `queue`: Tên của queue để gửi tin nhắn.
    - `body`: Nội dung tin nhắn gửi đi.
    - `waitForReply`: Nếu là `true`, phương thức sẽ chờ phản hồi từ RabbitMQ, nếu `false` sẽ không chờ phản hồi.
- **Trả về**:
    - `[]byte`: Nội dung phản hồi từ RabbitMQ (nếu có).
    - `error`: Lỗi nếu có trong quá trình gửi tin nhắn hoặc nhận phản hồi.

## Lỗi Thường Gặp

- **ErrTimeout**: Lỗi xảy ra khi quá trình chờ phản hồi vượt quá thời gian timeout đã được cấu hình.

```go
var ErrTimeout = errors.New("operation timed out")
```

---

## Ví Dụ Đầy Đủ

Dưới đây là ví dụ đầy đủ về cách sử dụng `Publisher` trong ứng dụng:

```go
package main

import (
    "context"
    "log"
    "time"
    "your_module/client" // Import module của bạn
)

func main() {
    // Khởi tạo Publisher
    cfg := &client.Config{
        URL:     "amqp://guest:guest@localhost:5672/",
        Timeout: 5000, // Timeout 5 giây
    }

    publisher, err := client.NewPublisher(cfg)
    if err != nil {
        log.Fatalf("Error creating publisher: %v", err)
    }

    // Gửi tin nhắn và chờ phản hồi
    message := []byte(`{"message": "Hello, RabbitMQ!"}`)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    reply, err := publisher.Publish(ctx, "testQueue", message, true)
    if err != nil {
        log.Fatalf("Error publishing message: %v", err)
    }

    // In ra phản hồi
    log.Printf("Received reply: %s", reply)
}
```

---

Với hướng dẫn trên, bạn có thể dễ dàng tích hợp `Publisher` vào hệ thống của mình để giao tiếp với RabbitMQ một cách hiệu quả.




# RabbitMQ Consumer - Hướng dẫn sử dụng

## Giới thiệu

Đây là hướng dẫn sử dụng `Consumer` trong hệ thống của bạn để lắng nghe và xử lý các thông điệp từ các hàng đợi (queues) RabbitMQ. `Consumer` sẽ tự động đăng ký các hàng đợi (nếu chưa tồn tại) và xử lý các thông điệp nhận được từ chúng. Mỗi hàng đợi có thể có một hàm xử lý (handler) riêng biệt.

## Cài đặt

Để sử dụng `Consumer`, bạn cần có một cấu hình cho RabbitMQ và một số phụ thuộc. Đảm bảo rằng bạn đã cài đặt và cấu hình `RabbitMQ` đúng cách.

### Cấu hình

Đầu tiên, bạn cần tạo một cấu hình cho `Consumer`. Cấu hình này bao gồm URL kết nối đến RabbitMQ và danh sách các hàng đợi mà bạn muốn tiêu thụ.

```go
type Config struct {
    URL     string   // URL kết nối đến RabbitMQ
    Queues  []string // Danh sách các hàng đợi để tiêu thụ
}
```

Ví dụ:

```go
cfg := &Config{
    URL:    "amqp://guest:guest@localhost:5672/",
    Queues: []string{"queue1", "queue2"},
}
```

## Khởi tạo Consumer

Sau khi cấu hình, bạn có thể khởi tạo một `Consumer` bằng cách gọi hàm `NewConsumer`.

```go
consumer, err := NewConsumer(cfg)
if err != nil {
    log.Fatalf("Error initializing consumer: %v", err)
}
```

## Lắng nghe các hàng đợi

Sau khi khởi tạo `Consumer`, bạn có thể gọi phương thức `Listen` để bắt đầu lắng nghe các hàng đợi. Phương thức này sẽ tự động đăng ký các hàng đợi chưa tồn tại và bắt đầu tiêu thụ các thông điệp.

```go
err := consumer.Listen()
if err != nil {
    log.Fatalf("Error listening to queues: %v", err)
}
```

Lưu ý: Mỗi hàng đợi cần có một handler (hàm xử lý) riêng. Các handler này phải được đăng ký trước khi gọi `Listen`.

## Đăng ký Handler

Trước khi bắt đầu lắng nghe, bạn cần đăng ký các handler cho các hàng đợi mà bạn muốn tiêu thụ. Mỗi hàng đợi sẽ có một hàm xử lý riêng biệt.

Ví dụ:

```go
func handleQueue1(msg []byte) error {
    log.Printf("Processing message for queue1: %s", msg)
    return nil
}

func handleQueue2(msg []byte) error {
    log.Printf("Processing message for queue2: %s", msg)
    return nil
}

// Đăng ký handler cho các hàng đợi
handlerRegistry := map[string]HandlerFunc{
    "queue1": handleQueue1,
    "queue2": handleQueue2,
}
```

## Đóng Consumer

Khi bạn không còn cần phải lắng nghe nữa, đừng quên gọi phương thức `Close` để đóng kết nối và kênh RabbitMQ.

```go
consumer.Close()
```

## Ví dụ hoàn chỉnh

Dưới đây là ví dụ hoàn chỉnh về cách sử dụng `Consumer`:

```go
package main

import (
    "log"
    "server" // import package chứa Consumer
)

func handleQueue1(msg []byte) error {
    log.Printf("Processing message for queue1: %s", msg)
    return nil
}

func handleQueue2(msg []byte) error {
    log.Printf("Processing message for queue2: %s", msg)
    return nil
}

func main() {
    // Khởi tạo cấu hình
    cfg := &server.Config{
        URL:    "amqp://guest:guest@localhost:5672/",
        Queues: []string{"queue1", "queue2"},
    }

    // Đăng ký handler cho các hàng đợi
    server.handlerRegistry = map[string]server.HandlerFunc{
        "queue1": handleQueue1,
        "queue2": handleQueue2,
    }

    // Khởi tạo consumer
    consumer, err := server.NewConsumer(cfg)
    if err != nil {
        log.Fatalf("Error initializing consumer: %v", err)
    }

    // Bắt đầu lắng nghe
    err = consumer.Listen()
    if err != nil {
        log.Fatalf("Error listening to queues: %v", err)
    }

    // Đóng consumer khi không còn sử dụng
    defer consumer.Close()
}
```

## Kết luận

Với `Consumer`, bạn có thể dễ dàng quản lý các hàng đợi RabbitMQ trong ứng dụng của mình. Chỉ cần cấu hình các hàng đợi và đăng ký các hàm xử lý, và `Consumer` sẽ lo phần còn lại cho bạn. Đừng quên xử lý lỗi và đóng kết nối khi không cần thiết nữa!
