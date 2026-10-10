package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gunturdwiap/elephanttalk/ws/v2"
)

var counter atomic.Int64

// TODO: rooms, typings, presence? history?
func main() {
	var port int
	flag.IntVar(&port, "port", 8080, "server port")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	hub := ws.NewHub()
	hub.HandleError(func(c *ws.Client, err error) {
		logger.Error(err.Error())
	})
	hub.HandleConnect(func(c *ws.Client) {
		c.Set("username", fmt.Sprintf("user-%d", counter.Add(1)))
	})
	hub.HandleMessage(func(c *ws.Client, i int, b []byte) {
		var request action
		if err := json.Unmarshal(b, &request); err != nil {
			logger.Error(err.Error())
			return
		}

		username, ok := c.Get("username")
		if !ok {
			logger.Error("no username")
			return
		}

		switch request.Type {
		case chat:
			var input struct {
				Message string `json:"message"`
			}
			if err := json.Unmarshal(request.Payload, &input); err != nil {
				logger.Error(err.Error())
				return
			}

			// TODO do something validate persist idk

			payload := struct {
				Sender  string    `json:"sender"`
				SentAt  time.Time `json:"sent_at"`
				Message string    `json:"message"`
			}{
				Sender:  username,
				SentAt:  time.Now(),
				Message: html.EscapeString(input.Message),
			}

			output, err := marshalAction(chat, payload)
			if err != nil {
				logger.Error(err.Error())
				return
			}

			hub.BroadcastFilter(i, output, func(tc *ws.Client) bool {
				return c.Request().URL.Path == tc.Request().URL.Path
			})

		case typing:
			var input struct {
				IsTyping bool `json:"is_typing"`
			}
			if err := json.Unmarshal(request.Payload, &input); err != nil {
				logger.Error(err.Error())
				return
			}

			payload := struct {
				Sender   string `json:"sender"`
				IsTyping bool   `json:"is_typing"`
			}{
				Sender:   username,
				IsTyping: input.IsTyping,
			}

			output, err := marshalAction(typing, payload)
			if err != nil {
				logger.Error(err.Error())
				return
			}

			hub.BroadcastFilter(i, output, func(tc *ws.Client) bool {
				targetUsername, _ := tc.Get("username")
				return c.Request().URL.Path == tc.Request().URL.Path && username != targetUsername
			})
		default:
			logger.Error("unknown type")
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

	logger.Info("server run", slog.Int("port", port))
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	if err := server.ListenAndServe(); err != nil {
		logger.Error(err.Error())
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
func marshalAction(at actionType, v any) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	return json.Marshal(action{
		Type:    at,
		Payload: payload,
	})
}
