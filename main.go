package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gunturdwiap/elephanttalk/ws/v2"
)

// TODO: rooms, typings, presence? history?

// flow:
// connect:    http upgrade -> client -> register chan -> hub event loop -> added to h.clients
// broadcast:  client readpump -> broadcast chan -> hub event loop -> all client send chans -> writepumps
func main() {
	var port int
	flag.IntVar(&port, "port", 8080, "server port")
	flag.Parse()

	hub := ws.NewHub()

	hub.HandleMessage(func(c *ws.Client, i int, b []byte) {
		var request action
		if err := json.Unmarshal(b, &request); err != nil {
			slog.Error(err.Error())
			return
		}

		switch request.Type {
		case chat:
			var input struct {
				Message string `json:"message"`
			}
			if err := json.Unmarshal(request.Payload, &input); err != nil {
				slog.Error(err.Error())
				return
			}

			// TODO do something validate persist idk

			output := struct {
				Sender  string    `json:"sender"`
				SentAt  time.Time `json:"sent_at"`
				Message string    `json:"message"`
			}{
				// dummy username
				Sender:  fmt.Sprintf("user-%p", c),
				SentAt:  time.Now(),
				Message: input.Message,
			}

			out, err := json.Marshal(output)
			if err != nil {
				slog.Error(err.Error())
				return
			}

			hub.BroadcastFilter(i, out, func(tc *ws.Client) bool {
				return c.Request().URL.Path == tc.Request().URL.Path
			})

		// TODO
		case typing:
			return
		default:
			slog.Error("unknown type")
			return
		}
	})

	mux := http.NewServeMux()
	mux.Handle("GET /public/", http.StripPrefix("/public/", http.FileServer(http.Dir("./public"))))
	mux.Handle("/{$}", http.RedirectHandler("/room/general", http.StatusPermanentRedirect))
	mux.HandleFunc("GET /room/{room}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./public/index.html")
	})

	mux.Handle("GET /room/{room}/ws", hub)

	slog.Info("server run", slog.Int("port", port))
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	if err := server.ListenAndServe(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

type action struct {
	Type    actionType      `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type actionType string

const (
	chat   actionType = "chat"
	typing actionType = "typing"
)

func parseActionType(raw string) (actionType, error) {
	switch at := actionType(raw); at {
	case chat, typing:
		return at, nil
	default:
		return "", fmt.Errorf("invalid action type: %q", raw)
	}
}

func (at *actionType) UnmarshalJSON(raw []byte) error {
	unquoted, err := strconv.Unquote(string(raw))
	if err != nil {
		return errors.New("invalid action type")
	}

	parsed, err := parseActionType(unquoted)
	if err != nil {
		return err
	}

	*at = parsed

	return nil
}
