package ws

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

type event struct {
	Sender  *client         `json:"-"`
	Action  actionType      `json:"action"`
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
