package consumer

import (
	"reflect"
	"strings"
)

// Handler defines user code that processes a single message body and optionally
// returns a response body.
// Returning an error causes the delivery to be *Nack*‑ed (optionally requeued).
// Returning a non‑nil []byte sends a reply when ReplyTo is provided.
// The consumer will *Ack* the message automatically on success.
// Implementations should be thread‑safe.
//
// Example:
//
//	type LogHandler struct{}
//	func (LogHandler) Handle(body []byte) ([]byte, error) {
//	    log.Printf("[LOG] %s", body)
//	    return nil, nil
//	}
type Handler interface{ Handle([]byte) ([]byte, error) }

type HandlerFunc func([]byte) ([]byte, error)

// getStructName returns lowercase type name used as key in queue map.
func getStructName(i interface{}) string {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return strings.ToLower(t.Name())
}
