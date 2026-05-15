package apperr

import (
	"encoding/json"
	"io"
)

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"`
}

func (e *Error) Error() string {
	return e.Message
}

func New(code, message, hint string) *Error {
	return &Error{Code: code, Message: message, Hint: hint}
}

func WriteJSON(w io.Writer, err error) {
	if w == nil || err == nil {
		return
	}
	appErr, ok := err.(*Error)
	if !ok {
		appErr = New("INTERNAL", err.Error(), "")
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"error": appErr})
}
