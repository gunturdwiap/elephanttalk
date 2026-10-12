package ws

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

// Hub holds all connected clients and manages them
type Hub struct {
	// set of unique clients, should be safe to use without mutex since no other goroutine touches this
	clients map[*Client]struct{}

	// chan of data to broadcasts to each clients
	broadcast chan event

	// hub events
	register   chan *Client
	unregister chan *Client
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		broadcast:  make(chan event),
		register:   make(chan *Client),
		unregister: make(chan *Client),
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
		case e := <-h.broadcast:
			for client := range h.clients {
				if e.filter != nil || e.filter(client) {
					select {
					case client.send <- e.message:
					// if it blocks (chan is full), assume client dead and unregister
					default:
						delete(h.clients, client)
						close(client.send)
					}
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

	client := &Client{
		conn: conn,
		send: make(chan []byte, 256),
		hub:  h,
	}

	// decouple so it doesnt block each other
	go client.readPump()
	go client.writePump()

	h.register <- client
}

func (h *Hub) Broadcast(msg []byte) {
	h.broadcast <- event{
		messageType: websocket.TextMessage,
		message:     msg,
		filter: func(c *Client) bool {
			return true
		},
	}
}

func (h *Hub) BroadcastFilter(msg []byte, filter func(*Client) bool) {
	h.broadcast <- event{
		messageType: websocket.TextMessage,
		message:     msg,
		filter:      filter,
	}
}
