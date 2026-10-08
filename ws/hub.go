package ws

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Hub holds all connected clients and manages them
type Hub struct {
	// set of unique clients, should be safe to use without mutex since no other goroutine touches this
	clients map[*client]struct{}

	// chan of data to broadcasts to each clients
	broadcast chan event

	// hub events
	register   chan *client
	unregister chan *client
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*client]struct{}),
		broadcast:  make(chan event),
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
		case e := <-h.broadcast:
			switch e.Action {
			case chat:
				if err := h.chatHandler(e); err != nil {
					slog.Error(err.Error())
					continue
				}
			case typing:
				//TODO handle typing
				out, err := json.Marshal(e.Payload)
				if err != nil {
					slog.Error(err.Error())
					continue
				}

				h.broadcastToAll(out)
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

func (h *Hub) chatHandler(e event) error {
	var input struct {
		Message string `json:"message"`
	}

	err := json.Unmarshal(e.Payload, &input)
	if err != nil {
		return err
	}

	output := struct {
		Sender  string    `json:"sender"`
		SentAt  time.Time `json:"sent_at"`
		Message string    `json:"message"`
	}{
		Sender:  fmt.Sprintf("client-%p", e.Sender),
		SentAt:  time.Now(),
		Message: input.Message,
	}

	out, err := json.Marshal(output)
	if err != nil {
		return err
	}

	slog.Info("broadcasting", slog.Any("event", out))
	h.broadcastToAll(out)

	return nil
}

func (h *Hub) broadcastToAll(out []byte) {
	for client := range h.clients {
		select {
		case client.send <- out:
		// if it blocks (chan is full), assume client dead and unregister
		default:
			delete(h.clients, client)
			close(client.send)
		}
	}
}
