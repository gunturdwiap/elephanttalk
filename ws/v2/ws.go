package ws

// import (
// 	"errors"
// 	"net/http"
// 	"sync"

// 	"github.com/gorilla/websocket"
// )

// // Client represents an active websocket connection.
// type Client struct {
// 	hub     *Hub
// 	conn    *websocket.Conn
// 	send    chan event
// 	request *http.Request
// 	metaMu  sync.RWMutex
// 	meta    map[string]any
// }

// // Request returns the http.Request associated with the handshake.
// func (c *Client) Request() *http.Request { return c.request }

// // Set safely stores metadata on the client connection.
// func (c *Client) Set(key string, value any) {
// 	c.metaMu.Lock()
// 	c.meta[key] = value
// 	c.metaMu.Unlock()
// }

// // Get safely retrieves metadata from the client connection.
// func (c *Client) Get(key string) (any, bool) {
// 	c.metaMu.RLock()
// 	val, ok := c.meta[key]
// 	c.metaMu.RUnlock()
// 	return val, ok
// }

// // Get safely retrieves metadata with native type safety using Go 1.27 generic methods.
// // func (c *Client) GetValue[T any](key string) (T, bool) {
// // 	c.metaMu.RLock()
// // 	val, ok := c.meta[key]
// // 	c.metaMu.RUnlock()
// // 	if !ok {
// // 		var zero T
// // 		return zero, false
// // 	}
// // 	typedVal, ok := val.(T)
// // 	return typedVal, ok
// // }

// func (c *Client) writeEvent(ev event) {
// 	select {
// 	case c.send <- ev:
// 	default:
// 		if c.hub.onError != nil {
// 			c.hub.onError(c, errors.New("event buffer full"))
// 		}
// 	}
// }

// func (c *Client) readPump() {
// 	defer func() {
// 		c.hub.unregister <- c
// 		c.conn.Close()
// 	}()
// 	for {
// 		mt, msg, err := c.conn.ReadMessage()
// 		if err != nil {
// 			if c.hub.onError != nil {
// 				c.hub.onError(c, err)
// 			}
// 			break
// 		}
// 		if c.hub.onMessage != nil {
// 			c.hub.onMessage(c, mt, msg)
// 		}
// 	}
// }

// func (c *Client) writePump() {
// 	defer c.conn.Close()
// 	for {
// 		ev, ok := <-c.send
// 		if !ok {
// 			c.conn.WriteMessage(websocket.CloseMessage, []byte{})
// 			return
// 		}
// 		if err := c.conn.WriteMessage(ev.messageType, ev.data); err != nil {
// 			break
// 		}
// 	}
// }

// // internal event packet moved through the hub channel
// type event struct {
// 	messageType int
// 	data        []byte
// 	filter      func(*Client) bool
// }

// // Hub manages connection pools and event looping.
// type Hub struct {
// 	upgrader     websocket.Upgrader
// 	clients      map[*Client]bool
// 	register     chan *Client
// 	unregister   chan *Client
// 	broadcast    chan event
// 	onConnect    func(*Client)
// 	onDisconnect func(*Client)
// 	onMessage    func(*Client, int, []byte)
// 	onError      func(*Client, error)
// }

// // New initializes and starts the background event loop.
// func New() *Hub {
// 	h := &Hub{
// 		upgrader:   websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
// 		clients:    make(map[*Client]bool),
// 		register:   make(chan *Client),
// 		unregister: make(chan *Client),
// 		broadcast:  make(chan event),
// 	}
// 	go h.run()
// 	return h
// }

// // Shy Setters for lifecycle hooks
// func (h *Hub) HandleConnect(f func(*Client))              { h.onConnect = f }
// func (h *Hub) HandleDisconnect(f func(*Client))           { h.onDisconnect = f }
// func (h *Hub) HandleMessage(f func(*Client, int, []byte)) { h.onMessage = f }
// func (h *Hub) HandleError(f func(*Client, error))         { h.onError = f }

// // Broadcast sends a message to all connected clients.
// func (h *Hub) Broadcast(messageType int, data []byte) {
// 	h.broadcast <- event{messageType: messageType, data: data}
// }

// // BroadcastFilter sends a message only to clients where the filter returns true (great for DMs or rooms).
// func (h *Hub) BroadcastFilter(messageType int, data []byte, filter func(*Client) bool) {
// 	h.broadcast <- event{messageType: messageType, data: data, filter: filter}
// }

// func (h *Hub) run() {
// 	for {
// 		select {
// 		case c := <-h.register:
// 			h.clients[c] = true
// 			if h.onConnect != nil {
// 				h.onConnect(c)
// 			}
// 		case c := <-h.unregister:
// 			if _, ok := h.clients[c]; ok {
// 				delete(h.clients, c)
// 				close(c.send)
// 				if h.onDisconnect != nil {
// 					h.onDisconnect(c)
// 				}
// 			}
// 		case ev := <-h.broadcast:
// 			for c := range h.clients {
// 				if ev.filter == nil || ev.filter(c) {
// 					c.writeEvent(ev)
// 				}
// 			}
// 		}
// 	}
// }

// // ServeHTTP handles websocket upgrade requests.
// func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
// 	conn, err := h.upgrader.Upgrade(w, r, nil)
// 	if err != nil {
// 		return
// 	}
// 	c := &Client{
// 		hub:     h,
// 		conn:    conn,
// 		send:    make(chan event, 256),
// 		request: r,
// 		meta:    make(map[string]any),
// 	}
// 	h.register <- c
// 	go c.writePump()
// 	go c.readPump()
// }
