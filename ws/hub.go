package ws

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

// Hub holds all connected clients and manages them
type Hub struct {
	// set of unique clients, should be safe to use without mutex since no other goroutine touches this
	clients map[*client]struct{}

	// chan of data to broadcasts to each clients
	broadcast chan []byte

	// hub events
	register   chan *client
	unregister chan *client
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*client]struct{}),
		broadcast:  make(chan []byte),
		register:   make(chan *client),
		unregister: make(chan *client),
	}
}

// hub eventloop
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = struct{}{}
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
		case m := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- m:
				// if it blocks(chan is full), assume client dead and unregister
				default:
					delete(h.clients, client)
					close(client.send)
				}
			}
		}
	}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error(err.Error())
		return
	}

	client := &client{
		conn: conn,
		send: make(chan []byte, 256),
		hub:  h,
	}

	// decouple so it doesnt block each other
	go client.readPump()
	go client.writePump()

	h.register <- client
}
