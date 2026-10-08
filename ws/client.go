package ws

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10 // 90% of pongWait
)

// Client represents each WS connection
type client struct {
	conn *websocket.Conn

	// ensure write is only done by one goroutine, gorrila shenanigans
	send chan []byte

	// reference to central Hub, needed for event comms
	hub *Hub
}

func (c *client) readPump() {
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

		var e event
		err = json.Unmarshal(p, &e)
		if err != nil {
			slog.Error(err.Error())
			continue
		}

		e.Sender = c

		c.hub.broadcast <- e
	}
}

func (c *client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		c.conn.Close()
		ticker.Stop()
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
		}
	}
}
