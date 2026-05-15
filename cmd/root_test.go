package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDiagnoseCommandEmitsJSONEnvelope(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"orders","state":"RUNNING","tasks":{"total":1,"running":1}}]}`))
	})
	mux.HandleFunc("/jobs/job-1", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jid":"job-1","name":"orders","state":"RUNNING","vertices":[{"id":"v1","name":"source","status":"RUNNING","tasks":{"total":1,"running":1}}]}`))
	})
	mux.HandleFunc("/jobs/job-1/exceptions", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"root-exception":"","all-exceptions":[]}`))
	})
	mux.HandleFunc("/jobs/job-1/checkpoints", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"counts":{"total":0,"completed":0,"failed":0}}`))
	})
	mux.HandleFunc("/jobmanager/config", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", server.URL}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if got, want := env["scenario"], "diagnose"; got != want {
		t.Fatalf("scenario = %v, want %v", got, want)
	}
	if _, ok := env["findings"].([]any); !ok {
		t.Fatalf("findings missing from envelope: %s", stdout.String())
	}
}

func TestDiagnoseCommandReturnsUserErrorForInvalidURL(t *testing.T) {
	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", "localhost:8081"}, &stdout, &stderr)
	if rc != 2 {
		t.Fatalf("RunWith rc = %d, want 2; stderr = %s", rc, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	var env map[string]map[string]any
	if err := json.Unmarshal(stderr.Bytes(), &env); err != nil {
		t.Fatalf("stderr is not JSON: %v\n%s", err, stderr.String())
	}
	if got, want := env["error"]["code"], "URL_INVALID"; got != want {
		t.Fatalf("error code = %v, want %v", got, want)
	}
}
