package apperr

import (
	"encoding/json"
	"io"
)

type Error struct {
	SchemaVersion string         `json:"schema_version"`
	Code          string         `json:"code"`
	Message       string         `json:"message"`
	Hint          string         `json:"hint,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string {
	return e.Message
}

func New(code, message, hint string) *Error {
	return &Error{SchemaVersion: "v1", Code: code, Message: message, Hint: hint}
}

// WithDetails attaches a structured payload so AI consumers can branch on
// machine-readable fields (available ids, http status, body prefix, retriable
// flags) instead of grep-ing the free-text message.
func (e *Error) WithDetails(details map[string]any) *Error {
	if len(details) == 0 {
		return e
	}
	if e.Details == nil {
		e.Details = map[string]any{}
	}
	for k, v := range details {
		e.Details[k] = v
	}
	return e
}

func WriteJSON(w io.Writer, err error) {
	if w == nil || err == nil {
		return
	}
	appErr, ok := err.(*Error)
	if !ok {
		appErr = New("INTERNAL", err.Error(), "")
	}
	if appErr.SchemaVersion == "" {
		appErr.SchemaVersion = "v1"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"error": appErr})
}
