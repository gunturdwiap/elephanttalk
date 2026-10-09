package ws

type filterFunc func(*Client) bool

type event struct {
	messageType int
	message     []byte
	filter      filterFunc
}
