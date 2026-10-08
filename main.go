package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gunturdwiap/elephanttalk/ws"
)

// flow:
// connect:    http upgrade -> client -> register chan -> hub event loop -> added to h.clients
// broadcast:  client readpump -> broadcast chan -> hub event loop -> all client send chans -> writepumps
func main() {
	var port int
	flag.IntVar(&port, "port", 8080, "server port")
	flag.Parse()

	hub := ws.NewHub()
	go hub.Run()

	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServer(http.Dir("./public")))
	mux.Handle("GET /ws", hub)

	slog.Info("server run", slog.Int("port", port))
	http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
}
