package ws

import (
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10 // 90% of pongWait
	readLimit  = 500 * 1024
	writeWait  = 10 * time.Second
)

// Client represents a ws connection
type Client struct {
	conn *websocket.Conn

	// ensure write is only done by one goroutine, gorrila shenanigans
	send chan event

	// reference to central Hub, needed for event comms
	hub *Hub

	request *http.Request

	metaMu sync.RWMutex
	meta   map[string]string
}

func (c *Client) Request() *http.Request {
	return c.request
}

func (c *Client) Get(key string) (string, bool) {
	c.metaMu.RLock()
	v, ok := c.meta[key]
	c.metaMu.RUnlock()

	return v, ok
}

func (c *Client) Set(key, val string) {
	c.metaMu.Lock()
	c.meta[key] = val
	c.metaMu.Unlock()
}

func (c *Client) Send(b []byte) {
	c.sendEvent(event{
		messageType: websocket.TextMessage,
		message:     b,
	})
}

func (c *Client) sendEvent(e event) {
	select {
	case c.send <- e:
	default:
		// if it blocks (chan is full), assume client dead and unregister
		c.hub.unregister <- c
	}
}

func (c *Client) readPump() {
	defer func() {
		// send unregister, which then delete the Client from the hub and close the send chan
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(readLimit)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(appData string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		mt, p, err := c.conn.ReadMessage()
		if err != nil {
			if c.hub.errorHandler != nil {
				c.hub.errorHandler(c, err)
			}
			return
		}

		if mt == websocket.TextMessage && c.hub.messageHandler != nil {
			c.hub.messageHandler(c, p)
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		c.conn.Close()
		ticker.Stop()
	}()

	for {
		select {
		case e, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			// if send chan is closed, that means Client disconnected, send close message
			if !ok {
				if err := c.conn.WriteMessage(websocket.CloseMessage, nil); err != nil {
					if c.hub.errorHandler != nil {
						c.hub.errorHandler(c, err)
					}
				}
				return
			}

			if err := c.conn.WriteMessage(e.messageType, e.message); err != nil {
				if c.hub.errorHandler != nil {
					c.hub.errorHandler(c, err)
				}
				return
			}

		// periodically send ping
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				if c.hub.errorHandler != nil {
					c.hub.errorHandler(c, err)
				}
				return
			}
		}
	}
}
