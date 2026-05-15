package flink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNormalizeBaseURLPreservesGatewayPath(t *testing.T) {
	base, err := NormalizeBaseURL("http://gateway.example.com/proxy/application_1/")
	if err != nil {
		t.Fatalf("NormalizeBaseURL returned error: %v", err)
	}
	if got, want := base.String(), "http://gateway.example.com/proxy/application_1"; got != want {
		t.Fatalf("base URL = %q, want %q", got, want)
	}
	if got, want := base.Endpoint("/jobs/overview"), "http://gateway.example.com/proxy/application_1/jobs/overview"; got != want {
		t.Fatalf("endpoint = %q, want %q", got, want)
	}
}

func TestNormalizeBaseURLDefaultsHTTPForHostPort(t *testing.T) {
	base, err := NormalizeBaseURL("localhost:8081")
	if err != nil {
		t.Fatalf("NormalizeBaseURL returned error: %v", err)
	}
	if got, want := base.String(), "http://localhost:8081"; got != want {
		t.Fatalf("base URL = %q, want %q", got, want)
	}
}

func TestClientCollectsJobSnapshot(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/flink/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"orders","state":"RUNNING","start-time":1000,"duration":300000,"tasks":{"total":2,"running":2}}]}`))
	})
	mux.HandleFunc("/flink/jobs/job-1", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jid":"job-1","name":"orders","state":"RUNNING","start-time":1000,"duration":300000,"vertices":[{"id":"v1","name":"source","parallelism":2,"status":"RUNNING","duration":300000,"tasks":{"total":2,"running":2}}]}`))
	})
	mux.HandleFunc("/flink/jobs/job-1/jobmanager/config", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"key":"execution.checkpointing.interval","value":"60000"}]`))
	})
	mux.HandleFunc("/flink/jobs/job-1/vertices/v1/backpressure", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","backpressure-level":"high","subtasks":[{"subtask":0,"backpressure-level":"high","ratio":0.9}]}`))
	})
	mux.HandleFunc("/flink/jobs/job-1/exceptions", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"root-exception":"","all-exceptions":[]}`))
	})
	mux.HandleFunc("/flink/jobs/job-1/checkpoints", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"counts":{"restored":0,"total":3,"in_progress":0,"completed":3,"failed":0},"latest":{"completed":{"id":3,"status":"COMPLETED","end_to_end_duration":1200}}}`))
	})
	mux.HandleFunc("/flink/jobmanager/config", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"key":"parallelism.default","value":"2"}]`))
	})
	mux.HandleFunc("/flink/config", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"flink-version":"1.18.1","flink-revision":"abc123"}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL + "/flink/")
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	snapshot, err := client.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect returned error: %v", err)
	}
	if got, want := len(snapshot.Jobs), 1; got != want {
		t.Fatalf("jobs length = %d, want %d", got, want)
	}
	if got, want := snapshot.Jobs[0].Detail.Vertices[0].Name, "source"; got != want {
		t.Fatalf("vertex name = %q, want %q", got, want)
	}
	if got, want := snapshot.FlinkVersion, "1.18.1"; got != want {
		t.Fatalf("flink version = %q, want %q", got, want)
	}
	if snapshot.Jobs[0].Detail.Vertices[0].Backpressure == nil {
		t.Fatalf("vertex backpressure was not collected")
	}
	if got, want := snapshot.Jobs[0].Detail.Vertices[0].Backpressure.Level(), "high"; got != want {
		t.Fatalf("backpressure level = %q, want %q", got, want)
	}
	if got, want := snapshot.Jobs[0].JobManagerConfig["execution.checkpointing.interval"], "60000"; got != want {
		t.Fatalf("job config = %q, want %q", got, want)
	}
	if got, want := snapshot.JobManagerConfig["parallelism.default"], "2"; got != want {
		t.Fatalf("jobmanager config = %q, want %q", got, want)
	}
}

func TestClientCollectsOnlyRequestedJobID(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"skip","state":"RUNNING"},{"jid":"job-2","name":"target","state":"FAILED"}]}`))
	})
	mux.HandleFunc("/jobs/job-2", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jid":"job-2","name":"target","state":"FAILED","vertices":[]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	snapshot, err := client.CollectWithOptions(context.Background(), CollectOptions{JobID: "job-2"})
	if err != nil {
		t.Fatalf("CollectWithOptions returned error: %v", err)
	}
	if got, want := len(snapshot.Jobs), 1; got != want {
		t.Fatalf("jobs length = %d, want %d", got, want)
	}
	if got, want := snapshot.Jobs[0].Overview.JID, "job-2"; got != want {
		t.Fatalf("job id = %q, want %q", got, want)
	}
}

func TestClientReturnsJobNotFoundForRequestedJobID(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"orders","state":"RUNNING"}]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	_, err = client.CollectWithOptions(context.Background(), CollectOptions{JobID: "missing"})
	if err == nil {
		t.Fatalf("expected error for missing job id")
	}
	var nf *JobNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T %[1]v, want *JobNotFoundError", err)
	}
}

func TestClientListJobsOnlyFetchesOverview(t *testing.T) {
	detailCalled := false
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"orders","state":"RUNNING"}]}`))
	})
	mux.HandleFunc("/jobs/job-1", func(w http.ResponseWriter, r *http.Request) {
		detailCalled = true
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	jobs, err := client.ListJobs(context.Background())
	if err != nil {
		t.Fatalf("ListJobs returned error: %v", err)
	}
	if detailCalled {
		t.Fatalf("ListJobs should not fetch job details")
	}
	if got, want := len(jobs), 1; got != want {
		t.Fatalf("jobs length = %d, want %d", got, want)
	}
}

func TestClientLimitsBackpressureCollectionByMaxVertices(t *testing.T) {
	var backpressureCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"large","state":"RUNNING"}]}`))
	})
	mux.HandleFunc("/jobs/job-1", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jid":"job-1","name":"large","state":"RUNNING","vertices":[{"id":"v1","name":"a","status":"RUNNING"},{"id":"v2","name":"b","status":"RUNNING"},{"id":"v3","name":"c","status":"RUNNING"}]}`))
	})
	mux.HandleFunc("/jobs/job-1/vertices/v1/backpressure", func(w http.ResponseWriter, r *http.Request) {
		backpressureCalls++
		_, _ = w.Write([]byte(`{"status":"ok","backpressure-level":"ok"}`))
	})
	mux.HandleFunc("/jobs/job-1/vertices/v2/backpressure", func(w http.ResponseWriter, r *http.Request) {
		backpressureCalls++
		_, _ = w.Write([]byte(`{"status":"ok","backpressure-level":"ok"}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	snapshot, err := client.CollectWithOptions(context.Background(), CollectOptions{MaxVertices: 2})
	if err != nil {
		t.Fatalf("CollectWithOptions returned error: %v", err)
	}
	if got, want := backpressureCalls, 2; got != want {
		t.Fatalf("backpressure calls = %d, want %d", got, want)
	}
	if len(snapshot.Warnings) == 0 {
		t.Fatalf("expected warning about skipped vertices")
	}
}
