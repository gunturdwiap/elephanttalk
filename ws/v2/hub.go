// package ws plagiarize olahol/melody but worse
package ws

import (
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

type filterFunc func(*Client) bool

type event struct {
	messageType int
	message     []byte
	filter      filterFunc
}

// note to self, flow:
// connect:    http upgrade -> client -> register chan -> hub event loop -> added to h.clients
// broadcast:  client readpump -> broadcast chan -> hub event loop -> all client send chans -> writepumps

// Hub holds all connected clients and manages them
type Hub struct {
	// set of unique clients, should be safe to use without mutex since no other goroutine touches this
	clients map[*Client]struct{}

	// chan of data to broadcasts to each clients
	broadcast chan event

	register   chan *Client
	unregister chan *Client
	shutdown   chan struct{}

	// hooks
	connectHandler    func(*Client)
	disconnectHandler func(*Client)
	messageHandler    func(*Client, []byte)
	errorHandler      func(*Client, error)

	// avoid shutting down mult times
	isShutdown atomic.Bool
}

// NewHub creates a new hub and runs its eventloop in a new goroutine
func NewHub() *Hub {
	h := &Hub{
		clients:    make(map[*Client]struct{}),
		broadcast:  make(chan event),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		shutdown:   make(chan struct{}),
	}

	go h.run()
	return h
}

// hooks setters
func (h *Hub) HandleConnect(f func(*Client))         { h.connectHandler = f }
func (h *Hub) HandleDisconnect(f func(*Client))      { h.disconnectHandler = f }
func (h *Hub) HandleMessage(f func(*Client, []byte)) { h.messageHandler = f }
func (h *Hub) HandleError(f func(*Client, error))    { h.errorHandler = f }

func (h *Hub) Broadcast(msg []byte) {
	h.broadcast <- event{
		messageType: websocket.TextMessage,
		message:     msg,
	}
}

func (h *Hub) BroadcastFilter(msg []byte, filter func(*Client) bool) {
	h.broadcast <- event{
		messageType: websocket.TextMessage,
		message:     msg,
		filter:      filter,
	}
}

// hub eventloop
func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = struct{}{}
		case client := <-h.unregister:
			h.removeClient(client)
		case e := <-h.broadcast:
			for client := range h.clients {
				if e.filter == nil || e.filter(client) {
					client.sendEvent(e)
				}
			}
		case <-h.shutdown:
			for client := range h.clients {
				h.removeClient(client)
			}
			return
		}
	}
}

func (h *Hub) removeClient(client *Client) {
	if _, ok := h.clients[client]; ok {
		delete(h.clients, client)
		close(client.send)
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

	if h.connectHandler != nil {
		h.connectHandler(client)
	}

	// decouple rw so it doesnt block each other
	go client.writePump()
	client.readPump()

	if h.disconnectHandler != nil {
		h.disconnectHandler(client)
	}
}

func (h *Hub) Shutdown() {
	if !h.isShutdown.Swap(true) {
		close(h.shutdown)
	}
}
