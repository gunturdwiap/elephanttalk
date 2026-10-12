package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"strconv"
	"time"

	"github.com/gunturdwiap/elephanttalk/ws/v2"
)

type actionType string

const (
	chatAction   actionType = "chat"
	typingAction actionType = "typing"
	errorAction  actionType = "error"
)

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

func parseActionType(raw string) (actionType, error) {
	switch at := actionType(raw); at {
	case chatAction, typingAction:
		return at, nil
	default:
		return "", fmt.Errorf("invalid action type: %q", raw)
	}
}

type action struct {
	Type    actionType      `json:"type"`
	Payload json.RawMessage `json:"payload"`
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

func (app *application) chatActionHandler(c *ws.Client, payloadBytes json.RawMessage) {
	username, ok := c.Get("username")
	if !ok {
		app.sendErrorAction(c, chatAction, "no username found for client")
		return
	}

	var input struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payloadBytes, &input); err != nil {
		app.sendErrorAction(c, chatAction, "bad req")
		return
	}

	payload := struct {
		Sender  string    `json:"sender"`
		SentAt  time.Time `json:"sent_at"`
		Message string    `json:"message"`
	}{
		Sender:  username,
		SentAt:  time.Now(),
		Message: html.EscapeString(input.Message),
	}

	output, err := marshalAction(chatAction, payload)
	if err != nil {
		app.sendInternalErrorAction(c, chatAction, err)
		return
	}

	app.hub.BroadcastFilter(output, func(tc *ws.Client) bool {
		return c.Request().URL.Path == tc.Request().URL.Path
	})
}

func (app *application) typingActionHandler(c *ws.Client, payloadBytes json.RawMessage) {
	username, ok := c.Get("username")
	if !ok {
		app.sendErrorAction(c, typingAction, "no username found for client")
		return
	}

	var input struct {
		IsTyping bool `json:"is_typing"`
	}
	if err := json.Unmarshal(payloadBytes, &input); err != nil {
		app.sendErrorAction(c, typingAction, "bad req")
		return
	}

	payload := struct {
		Sender   string `json:"sender"`
		IsTyping bool   `json:"is_typing"`
	}{
		Sender:   username,
		IsTyping: input.IsTyping,
	}

	output, err := marshalAction(typingAction, payload)
	if err != nil {
		app.sendInternalErrorAction(c, typingAction, err)
		return
	}

	app.hub.BroadcastFilter(output, func(tc *ws.Client) bool {
		targetUsername, _ := tc.Get("username")
		return c.Request().URL.Path == tc.Request().URL.Path && username != targetUsername
	})
}

func (app *application) sendErrorAction(c *ws.Client, intendedAction actionType, msg string) {
	type payload struct {
		Type    actionType `json:"type"`
		Message string     `json:"message"`
	}
	res, err := marshalAction(errorAction, payload{
		Type:    intendedAction,
		Message: msg,
	})
	if err != nil {
		app.logger.Error("failed to marshal error action payload", slog.String("err", err.Error()))
		return
	}

	c.Send(res)
}

func (app *application) sendInternalErrorAction(c *ws.Client, intendedAction actionType, err error) {
	app.logger.Error("internal server error",
		slog.String("action", string(intendedAction)),
		slog.String("err", err.Error()),
	)

	app.sendErrorAction(c, intendedAction, "An internal server error occurred")
}
