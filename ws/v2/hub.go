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

	// hooks
	connectHandler    func(*Client)
	disconnectHandler func(*Client)
	messageHandler    func(*Client, int, []byte)
	errorHandler      func(*Client, error)
}

func NewHub() *Hub {
	h := &Hub{
		clients:    make(map[*Client]struct{}),
		broadcast:  make(chan event),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}

	go h.run()
	return h
}

// hooks setters
func (h *Hub) HandleConnect(f func(*Client))              { h.connectHandler = f }
func (h *Hub) HandleDisconnect(f func(*Client))           { h.disconnectHandler = f }
func (h *Hub) HandleMessage(f func(*Client, int, []byte)) { h.messageHandler = f }
func (h *Hub) HandleError(f func(*Client, error))         { h.errorHandler = f }

// hub eventloop
func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = struct{}{}
			if h.connectHandler != nil {
				h.connectHandler(client)
			}
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				if h.disconnectHandler != nil {
					h.disconnectHandler(client)
				}
				delete(h.clients, client)
				close(client.send)
			}
		case e := <-h.broadcast:
			for client := range h.clients {
				if e.filter == nil || e.filter(client) {
					select {
					case client.send <- e:
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
		conn:    conn,
		send:    make(chan event, 256),
		hub:     h,
		meta:    make(map[string]string),
		request: r,
	}

	h.register <- client

	// decouple so it doesnt block each other
	go client.readPump()
	go client.writePump()
}

func (h *Hub) Broadcast(messageType int, msg []byte) {
	h.broadcast <- event{
		messageType: messageType,
		message:     msg,
	}
}

func (h *Hub) BroadcastFilter(messageType int, msg []byte, filter func(*Client) bool) {
	h.broadcast <- event{
		messageType: messageType,
		message:     msg,
		filter:      filter,
	}
}
