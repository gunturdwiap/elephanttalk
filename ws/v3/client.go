package ws

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	pingPeriod = 54 * time.Second
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

	// initial handshake req
	request *http.Request

	// metadata
	meta   map[string]string
	metaMu sync.RWMutex
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

func (c *Client) Close() {
	c.hub.unregister <- c
}

func (c *Client) Send(b []byte) {
	c.sendEvent(event{
		messageType: websocket.MessageText,
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
		c.conn.Close(websocket.StatusNormalClosure, "")
	}()

	c.conn.SetReadLimit(readLimit)

	for {
		mt, p, err := c.conn.Read(context.Background())

		if err != nil {
			if c.hub.errorHandler != nil {
				c.hub.errorHandler(c, err)
			}
			return
		}

		if mt == websocket.MessageText && c.hub.messageHandler != nil {
			c.hub.messageHandler(c, p)
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		// coder conn.Close already handle closing conn and sending close frame
		c.conn.Close(websocket.StatusNormalClosure, "")
		ticker.Stop()
	}()

	for {
		select {
		case e, ok := <-c.send:
			// if send chan is closed, that means conn closed
			if !ok {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), writeWait)
			err := c.conn.Write(ctx, e.messageType, e.message)
			cancel()

			if err != nil {
				if c.hub.errorHandler != nil {
					c.hub.errorHandler(c, err)
				}
				return
			}

		// Periodically send ping
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), writeWait)
			err := c.conn.Ping(ctx)
			cancel()

			if err != nil {
				if c.hub.errorHandler != nil {
					c.hub.errorHandler(c, err)
				}
				return
			}
		}
	}
}
