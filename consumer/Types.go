package consumer

type HandlerFunc func(message []byte) ([]byte, error)

type Handler interface {
	Handle(message []byte) ([]byte, error)
}
