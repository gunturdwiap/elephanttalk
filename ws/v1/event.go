package ws

type filterFunc func(*Client) bool

type event struct {
	t      int
	data   []byte
	filter filterFunc
}
