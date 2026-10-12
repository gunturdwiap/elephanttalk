package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"strconv"
	"time"

	"github.com/gunturdwiap/elephanttalk/ws/v2"
)

type actionType string

const (
	chatAction   actionType = "chat"
	typingAction actionType = "typing"
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
		app.logger.Error("no username found for client")
		return
	}

	var input struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payloadBytes, &input); err != nil {
		app.logger.Error(err.Error())
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
		app.logger.Error(err.Error())
		return
	}

	app.hub.BroadcastFilter(output, func(tc *ws.Client) bool {
		return c.Request().URL.Path == tc.Request().URL.Path
	})
}

func (app *application) typingActionHandler(c *ws.Client, payloadBytes json.RawMessage) {
	username, ok := c.Get("username")
	if !ok {
		app.logger.Error("no username found for client")
		return
	}

	var input struct {
		IsTyping bool `json:"is_typing"`
	}
	if err := json.Unmarshal(payloadBytes, &input); err != nil {
		app.logger.Error(err.Error())
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
		app.logger.Error(err.Error())
		return
	}

	app.hub.BroadcastFilter(output, func(tc *ws.Client) bool {
		targetUsername, _ := tc.Get("username")
		return c.Request().URL.Path == tc.Request().URL.Path && username != targetUsername
	})
}
