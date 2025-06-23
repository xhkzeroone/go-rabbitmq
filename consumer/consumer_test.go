package consumer

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/mock"
)

// Mock Logger để không in ra console khi test
type mockLogger struct{}

func (m *mockLogger) Infof(format string, args ...any)  {}
func (m *mockLogger) Errorf(format string, args ...any) {}
func (m *mockLogger) Debugf(format string, args ...any) {}

// Mock Delivery để sử dụng trong test
type mockDelivery struct {
	mock.Mock
	amqp.Delivery
}

func (m *mockDelivery) Ack(multiple bool) error {
	args := m.Called(multiple)
	return args.Error(0)
}

func (m *mockDelivery) Nack(multiple, requeue bool) error {
	args := m.Called(multiple, requeue)
	return args.Error(0)
}

func TestHandleDelivery(t *testing.T) {
	tests := []struct {
		name          string
		handler       HandlerFunc
		delivery      amqp.Delivery
		requeueOnFail bool
		autoAck       bool
		setupMock     func(delivery *mockDelivery)
	}{
		{
			name:     "Success - Ack message",
			handler:  func(b []byte) ([]byte, error) { return nil, nil },
			delivery: amqp.Delivery{Body: []byte("ok")},
			autoAck:  false,
			setupMock: func(d *mockDelivery) {
				d.On("Ack", false).Return(nil).Once()
			},
		},
		{
			name:          "Handler Error - Nack without requeue",
			handler:       func(b []byte) ([]byte, error) { return nil, errors.New("handler failed") },
			delivery:      amqp.Delivery{Body: []byte("fail")},
			requeueOnFail: false,
			autoAck:       false,
			setupMock: func(d *mockDelivery) {
				d.On("Nack", false, false).Return(nil).Once()
			},
		},
		{
			name:          "Handler Error - Nack with requeue",
			handler:       func(b []byte) ([]byte, error) { return nil, errors.New("handler failed") },
			delivery:      amqp.Delivery{Body: []byte("fail")},
			requeueOnFail: true,
			autoAck:       false,
			setupMock: func(d *mockDelivery) {
				d.On("Nack", false, true).Return(nil).Once()
			},
		},
		{
			name:          "Panic in Handler - Nack with requeue",
			handler:       func(b []byte) ([]byte, error) { panic("something went very wrong") },
			delivery:      amqp.Delivery{Body: []byte("panic")},
			requeueOnFail: true,
			autoAck:       false,
			setupMock: func(d *mockDelivery) {
				d.On("Nack", false, true).Return(nil).Once()
			},
		},
		{
			name:     "Success with AutoAck - No Ack/Nack called",
			handler:  func(b []byte) ([]byte, error) { return nil, nil },
			delivery: amqp.Delivery{Body: []byte("ok")},
			autoAck:  true,
			setupMock: func(d *mockDelivery) {
				// Assert that Ack/Nack are not called
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer := &Consumer{
				cfg: QueueConfig{
					RequeueOnFail: tt.requeueOnFail,
					AutoAck:       tt.autoAck,
					RPCTimeout:    1 * time.Second,
				},
				name:    "test-consumer",
				handler: tt.handler,
				logger:  &mockLogger{},
			}

			// Tạo mock delivery và thiết lập expectation
			mockedDelivery := &mockDelivery{Delivery: tt.delivery}
			if tt.setupMock != nil {
				tt.setupMock(mockedDelivery)
			}

			// Gọi hàm cần test
			consumer.handleDelivery(&mockedDelivery.Delivery)

			// Kiểm tra xem các expectation có được đáp ứng không
			mockedDelivery.AssertExpectations(t)
		})
	}
}
