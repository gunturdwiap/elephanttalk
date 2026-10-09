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

func (c *Client) Get(key string) string {
	c.metaMu.RLock()
	defer c.metaMu.RUnlock()
	return c.meta[key]
}

func (c *Client) Set(key, val string) {
	c.metaMu.Lock()
	c.meta[key] = val
	c.metaMu.Unlock()
}

func (c *Client) readPump() {
	defer func() {
		c.conn.Close()
		// send unregister, which then delete the Client from the hub and close the send chan
		c.hub.unregister <- c
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

		if c.hub.messageHandler != nil {
			c.hub.messageHandler(c, mt, p)
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
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				if c.hub.errorHandler != nil {
					c.hub.errorHandler(c, err)
				}
				return
			}
		}
	}
}
