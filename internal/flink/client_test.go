package flink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestExtractJobIDFromWebURLFragment(t *testing.T) {
	raw := "https://gateway/proxy/application_1/#/job/running/29dff7365a0c1823af622b38eeb2bd96/overview"
	if got, want := ExtractJobIDFromWebURL(raw), "29dff7365a0c1823af622b38eeb2bd96"; got != want {
		t.Fatalf("ExtractJobIDFromWebURL = %q, want %q", got, want)
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

func TestClientReturnsHelpfulErrorForHTMLResponse(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><body>application expired</body></html>`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	_, err = client.Collect(context.Background())
	if err == nil {
		t.Fatalf("expected error for HTML response")
	}
	var nonJSON *NonJSONResponseError
	if !errors.As(err, &nonJSON) {
		t.Fatalf("error = %T %[1]v, want *NonJSONResponseError", err)
	}
	if nonJSON.Prefix == "" {
		t.Fatalf("expected response prefix in error")
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

func TestClientSamplesDorisSinkMetrics(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"sync","state":"RUNNING"}]}`))
	})
	mux.HandleFunc("/jobs/job-1", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jid":"job-1","name":"sync","state":"RUNNING","vertices":[{"id":"sink","name":"dorisSink: Writer","parallelism":4,"status":"RUNNING"}]}`))
	})
	mux.HandleFunc("/jobs/job-1/vertices/sink/backpressure", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok","backpressure-level":"ok"}`))
	})
	handleMetrics := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("get") == "" {
			_, _ = w.Write([]byte(`[
				{"id":"dorisSink__Writer.table_totalFlushSucceededNumber"},
				{"id":"dorisSink__Writer.table_totalFlushLoadedRows"},
				{"id":"dorisSink__Writer.table_totalFlushLoadBytes"},
				{"id":"dorisSink__Writer.table_loadTimeMs_mean"},
				{"id":"dorisSink__Writer.table_loadTimeMs_max"},
				{"id":"dorisSink__Writer.table_writeDataTimeMs_mean"},
				{"id":"dorisSink__Writer.table_writeDataTimeMs_max"},
				{"id":"dorisSink__Writer.table_commitAndPublishTimeMs_mean"}
			]`))
			return
		}
		if strings.Contains(r.URL.Path, "/subtasks/3/") {
			_, _ = w.Write([]byte(`[
				{"id":"dorisSink__Writer.table_totalFlushSucceededNumber","value":"2"},
				{"id":"dorisSink__Writer.table_totalFlushLoadedRows","value":"44000"},
				{"id":"dorisSink__Writer.table_totalFlushLoadBytes","value":"1200000000"},
				{"id":"dorisSink__Writer.table_loadTimeMs_mean","value":"95000"},
				{"id":"dorisSink__Writer.table_loadTimeMs_max","value":"110000"},
				{"id":"dorisSink__Writer.table_writeDataTimeMs_mean","value":"94000"},
				{"id":"dorisSink__Writer.table_writeDataTimeMs_max","value":"109000"}
			]`))
			return
		}
		_, _ = w.Write([]byte(`[
			{"id":"dorisSink__Writer.table_totalFlushSucceededNumber","value":"2"},
			{"id":"dorisSink__Writer.table_totalFlushLoadedRows","value":"40000"},
			{"id":"dorisSink__Writer.table_totalFlushLoadBytes","value":"1000000000"},
			{"id":"dorisSink__Writer.table_loadTimeMs_mean","value":"90000"},
			{"id":"dorisSink__Writer.table_loadTimeMs_max","value":"100000"},
			{"id":"dorisSink__Writer.table_writeDataTimeMs_mean","value":"89000"},
			{"id":"dorisSink__Writer.table_writeDataTimeMs_max","value":"99000"},
			{"id":"dorisSink__Writer.table_commitAndPublishTimeMs_mean","value":"40"}
		]`))
	}
	mux.HandleFunc("/jobs/job-1/vertices/sink/subtasks/0/metrics", handleMetrics)
	mux.HandleFunc("/jobs/job-1/vertices/sink/subtasks/1/metrics", handleMetrics)
	mux.HandleFunc("/jobs/job-1/vertices/sink/subtasks/2/metrics", handleMetrics)
	mux.HandleFunc("/jobs/job-1/vertices/sink/subtasks/3/metrics", handleMetrics)
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	snapshot, err := client.CollectWithOptions(context.Background(), CollectOptions{MaxVertices: 0})
	if err != nil {
		t.Fatalf("CollectWithOptions returned error: %v", err)
	}
	got := snapshot.Jobs[0].Detail.Vertices[0].DorisMetrics
	if got == nil {
		t.Fatalf("DorisMetrics was not collected")
	}
	if got.Summary.PerFlushBytesMean != 525000000 {
		t.Fatalf("PerFlushBytesMean = %v, want 525000000", got.Summary.PerFlushBytesMean)
	}
	if got.Summary.LoadTimeMsMean != 91250 {
		t.Fatalf("LoadTimeMsMean = %v, want 91250", got.Summary.LoadTimeMsMean)
	}
	if got.Summary.WriteDataTimeMsMax != 109000 {
		t.Fatalf("WriteDataTimeMsMax = %v, want 109000", got.Summary.WriteDataTimeMsMax)
	}
	if got.Summary.PerFlushMiBMean != 500.679 {
		t.Fatalf("PerFlushMiBMean = %v, want 500.679", got.Summary.PerFlushMiBMean)
	}
	if got.Summary.LoadTimeSecMean != 91.25 {
		t.Fatalf("LoadTimeSecMean = %v, want 91.25", got.Summary.LoadTimeSecMean)
	}
	if got.Summary.LoadMiBPerSecPerSubtask != 5.487 {
		t.Fatalf("LoadMiBPerSecPerSubtask = %v, want 5.487", got.Summary.LoadMiBPerSecPerSubtask)
	}
}
