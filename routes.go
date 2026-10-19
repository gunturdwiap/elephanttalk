package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/gunturdwiap/elephanttalk/public"
	"github.com/gunturdwiap/elephanttalk/ws/v3"
)

// TODO
var counter atomic.Int64

func (app *application) routes() http.Handler {
	app.hub.HandleError(func(c *ws.Client, err error) {
		app.logger.Error(err.Error())
	})

	app.hub.HandleConnect(func(c *ws.Client) {
		c.Set("username", fmt.Sprintf("user-%d", counter.Add(1)))
	})

	app.hub.HandleDisconnect(func(c *ws.Client) {
		username, _ := c.Get("username")
		app.logger.Info("client disconnected", slog.String("username", username))
	})

	app.hub.HandleMessage(func(c *ws.Client, b []byte) {
		var request action
		if err := json.Unmarshal(b, &request); err != nil {
			app.logger.Error(err.Error())
			c.Close()
			return
		}

		switch request.Type {
		case chatAction:
			app.chatActionHandler(c, request.Payload)
		case typingAction:
			app.typingActionHandler(c, request.Payload)
		default:
			app.logger.Error("unknown action type", slog.String("type", string(request.Type)))
			c.Close()
			return
		}
	})

	indexHTML, err := public.Files.ReadFile("index.html")
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /public/", http.StripPrefix("/public/", http.FileServerFS(public.Files)))
	mux.Handle("/{$}", http.RedirectHandler("/room/general", http.StatusPermanentRedirect))
	mux.HandleFunc("GET /room/{room}", func(w http.ResponseWriter, r *http.Request) {
		w.Write(indexHTML)
	})
	mux.Handle("GET /room/{room}/ws", app.hub)

	return mux
}
