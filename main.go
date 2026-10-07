package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

var counter atomic.Int64

// flow:
// connect:    http upgrade -> client -> register chan -> hub event loop -> added to h.clients
// broadcast:  client readpump -> broadcast chan -> hub event loop -> all client send chans -> writepumps
func main() {
	hub := newHub()

	go hub.run()

	http.Handle("GET /", http.FileServer(http.Dir("./public")))
	http.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Error(err.Error())
			return
		}

		client := &Client{
			conn: conn,
			send: make(chan []byte, 256),
			hub:  hub,
			id:   counter.Add(1),
		}

		// decouple so it doesnt block each other
		go client.readPump()
		go client.writePump()

		hub.register <- client
	})

	slog.Info("server run 8080")
	http.ListenAndServe(":8080", nil)
}

const (
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10 // 90% of pongWait
)

// Client represents each WS connection
type Client struct {
	conn *websocket.Conn

	// ensure write is only done by one goroutine, gorrila shenanigans
	send chan []byte

	// reference to central Hub, needed for event comms
	hub *Hub

	// dummy id
	id int64
}

func (c *Client) readPump() {
	defer func() {
		c.conn.Close()

		// send unregister, which then delete the client from the hub and close the send chan
		c.hub.unregister <- c
	}()

	c.conn.SetReadLimit(500 * 1024)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(appData string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, p, err := c.conn.ReadMessage()
		if err != nil {
			slog.Error(err.Error())
			return
		}

		msg := fmt.Appendf(nil, "client-%d: %s", c.id, p)
		slog.Info(string(msg))

		c.hub.broadcast <- msg
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)

	// for push demonstration purpose
	meowTicker := time.NewTicker(5 * time.Second)

	defer func() {
		c.conn.Close()
		ticker.Stop()
		meowTicker.Stop()
	}()

	for {
		select {
		case b, ok := <-c.send:
			// if send chan is closed, that means client disconnected, send close message
			if !ok {
				if err := c.conn.WriteMessage(websocket.CloseMessage, nil); err != nil {
					slog.Error(err.Error())
				}
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, b); err != nil {
				slog.Error(err.Error())
				return
			}

		// periodically send ping
		case <-ticker.C:
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				slog.Error(err.Error())
				return
			}
		case <-meowTicker.C:
			if rand.Intn(100) == 0 {
				continue
			}

			msg := fmt.Appendf(nil, "client-%d: %s", c.id, "meow")
			slog.Info(string(msg))

			c.hub.broadcast <- msg
		}
	}
}

// Hub holds all connected clients and manages them
type Hub struct {
	// set of unique clients, should be safe to use without mutex since no other goroutine touches this
	clients map[*Client]struct{}

	// chan of data to broadcasts to each clients
	broadcast chan []byte

	// hub events
	register   chan *Client
	unregister chan *Client
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// hub eventloop
func (h *Hub) run() {
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
