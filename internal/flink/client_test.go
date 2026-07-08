package flink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

func TestExtractTaskManagerIDFromWebURLFragment(t *testing.T) {
	raw := "https://gateway/proxy/application_1/#/task-manager/container_e57_1777980975440_136612_01_000009/thread-dump"
	if got, want := ExtractTaskManagerIDFromWebURL(raw), "container_e57_1777980975440_136612_01_000009"; got != want {
		t.Fatalf("ExtractTaskManagerIDFromWebURL = %q, want %q", got, want)
	}
}

func TestExtractVertexIDFromWebURLFragment(t *testing.T) {
	raw := "https://gateway/proxy/application_1/#/job/running/29dff7365a0c1823af622b38eeb2bd96/vertices/9f4e2c3d1b0a9876543210fedcba1234/flamegraph"
	if got, want := ExtractVertexIDFromWebURL(raw), "9f4e2c3d1b0a9876543210fedcba1234"; got != want {
		t.Fatalf("ExtractVertexIDFromWebURL = %q, want %q", got, want)
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
		_, _ = w.Write([]byte(`{"counts":{"restored":0,"total":3,"in_progress":0,"completed":3,"failed":0},"summary":{"end_to_end_duration":{"avg":1200,"max":1500},"alignment_buffered":{"avg":0,"max":0},"state_size":{"avg":1024,"max":2048}},"latest":{"completed":{"id":3,"status":"COMPLETED","end_to_end_duration":1200,"alignment_buffered":0,"state_size":1024}}}`))
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
	if got, want := snapshot.Jobs[0].Checkpoints.Summary.EndToEndDuration.Avg, float64(1200); got != want {
		t.Fatalf("checkpoint duration avg = %v, want %v", got, want)
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
	if !nonJSON.IsHTML {
		t.Fatalf("expected IsHTML=true for HTML body")
	}
}

// YARN proxy expiry / login redirect often returns a non-2xx status WITH an
// HTML body. That must be detected as an HTML/proxy-expired error, not an
// opaque HTTP_STATUS_ERROR that is indistinguishable from a real Flink 5xx.
func TestClientDetectsHTMLOnNon2xx(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusFound)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><body>login required</body></html>`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	_, err = client.Collect(context.Background())
	if err == nil {
		t.Fatalf("expected error for non-2xx HTML response")
	}
	var nonJSON *NonJSONResponseError
	if !errors.As(err, &nonJSON) {
		t.Fatalf("error = %T %[1]v, want *NonJSONResponseError (not HTTPError)", err)
	}
	if !nonJSON.IsHTML {
		t.Fatalf("expected IsHTML=true for non-2xx HTML body")
	}
	class := ClassifyAPIError(err)
	if class.Code != "NON_JSON_HTML_RESPONSE" {
		t.Fatalf("classified code = %q, want NON_JSON_HTML_RESPONSE", class.Code)
	}
}

// A genuine non-2xx WITHOUT HTML body must still classify as HTTP_STATUS_ERROR.
func TestClientNon2xxWithoutHTMLStaysHTTPError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"errors":["boom"]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	_, err = client.Collect(context.Background())
	if err == nil {
		t.Fatalf("expected error for HTTP 500")
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("error = %T %[1]v, want *HTTPError", err)
	}
	if httpErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", httpErr.StatusCode)
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

func TestClientGetsThreadDump(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/taskmanagers/container_1/thread-dump", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"threadInfos":[{"threadName":"stream-load-upload-1-table-thread-1","stringifiedThreadInfo":"\"stream-load-upload-1-table-thread-1\" Id=1 RUNNABLE\n\tat java.net.SocketOutputStream.socketWrite0(Native Method)\n\tat org.apache.doris.flink.sink.writer.DorisWriter.write(DorisWriter.java:1)\n\n"}]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	dump, err := client.GetThreadDump(context.Background(), "container_1")
	if err != nil {
		t.Fatalf("GetThreadDump returned error: %v", err)
	}
	if got, want := len(dump.ThreadInfos), 1; got != want {
		t.Fatalf("thread count = %d, want %d", got, want)
	}
	summary := SummarizeThreadDump(dump, 10)
	if got, want := summary.States["RUNNABLE"], 1; got != want {
		t.Fatalf("RUNNABLE threads = %d, want %d", got, want)
	}
	if got, want := summary.InterestingThreads[0].Reason, "doris_stream_load_socket_write"; got != want {
		t.Fatalf("reason = %q, want %q", got, want)
	}
	if !strings.Contains(summary.Interpretation, "Doris Stream Load") {
		t.Fatalf("interpretation should mention Doris Stream Load: %q", summary.Interpretation)
	}
}

func TestSummarizeThreadDumpExplainsNoInterestingThreads(t *testing.T) {
	summary := SummarizeThreadDump(ThreadDump{ThreadInfos: []ThreadInfo{{
		ThreadName:            "flink-akka.actor.default-dispatcher",
		StringifiedThreadInfo: "\"flink-akka.actor.default-dispatcher\" Id=1 WAITING\n\tat java.lang.Object.wait(Native Method)\n\n",
	}}}, 10)
	if got, want := summary.InterestingCount, 0; got != want {
		t.Fatalf("interesting count = %d, want %d", got, want)
	}
	if !strings.Contains(summary.Interpretation, "未发现") {
		t.Fatalf("interpretation should explain empty interesting threads: %q", summary.Interpretation)
	}
}

// A thread whose stack merely mentions the word "blocked" (e.g. in a method or
// log line) but is RUNNABLE must NOT be classified as blocked. Only real
// BLOCKED-state threads should be.
func TestSummarizeThreadDumpBlockedUsesThreadState(t *testing.T) {
	dump := ThreadDump{ThreadInfos: []ThreadInfo{
		{
			ThreadName:            "worker-runnable",
			StringifiedThreadInfo: "\"worker-runnable\" Id=10 RUNNABLE\n\tat com.example.QueueBlockedChecker.poll(QueueBlockedChecker.java:20)\n\n",
		},
		{
			ThreadName:            "worker-really-blocked",
			StringifiedThreadInfo: "\"worker-really-blocked\" Id=11 BLOCKED on java.lang.Object@abc\n\tat com.example.Lock.acquire(Lock.java:5)\n\n",
		},
	}}
	summary := SummarizeThreadDump(dump, 10)
	if summary.Reasons["blocked"] != 1 {
		t.Fatalf("blocked reason count = %d, want 1 (only the real BLOCKED thread)", summary.Reasons["blocked"])
	}
	for _, it := range summary.InterestingThreads {
		if it.ThreadName == "worker-runnable" {
			t.Fatalf("RUNNABLE thread with 'blocked' in stack must not be flagged blocked")
		}
	}
}

func TestSummarizeDorisSinkMetricsRoundsAndDerivesReadableFields(t *testing.T) {
	summary := summarizeDorisSinkMetrics([]DorisSinkMetricsSample{
		{
			Subtask:                    0,
			FlushSucceeded:             3,
			FlushLoadedRows:            82000,
			FlushLoadBytes:             2652796129,
			LoadTimeMsMean:             118565.51530612246,
			LoadTimeMsMax:              148271,
			WriteDataTimeMsMean:        118466.97448979593,
			WriteDataTimeMsMax:         148122,
			BeginTxnTimeMsMean:         20.591478696741856,
			CommitAndPublishTimeMsMean: 46.320802005012524,
		},
	})
	if got, want := summary.PerFlushMiBMean, 843.301; got != want {
		t.Fatalf("per flush MiB mean = %v, want %v", got, want)
	}
	if got, want := summary.WriteDataShareOfLoad, 0.999; got != want {
		t.Fatalf("write data share = %v, want %v", got, want)
	}
	if got, want := summary.LoadMiBPerSecPerSubtask, 7.113; got != want {
		t.Fatalf("load MiB/s = %v, want %v", got, want)
	}
	if got, want := summary.CommitAndPublishTimeSecMean, 0.046; got != want {
		t.Fatalf("commit seconds = %v, want %v", got, want)
	}
}

func TestClientGetsFlameGraphWithTypeAndSubtask(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/job-1/vertices/v1/flamegraph", func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("type"), "ON_CPU"; got != want {
			t.Fatalf("flamegraph type = %q, want %q", got, want)
		}
		if got, want := r.URL.Query().Get("subtaskindex"), "3"; got != want {
			t.Fatalf("subtaskindex = %q, want %q", got, want)
		}
		_, _ = w.Write([]byte(`{"endTimestamp":1234,"data":{"name":"root","value":10,"children":[{"name":"hot.Frame","value":7},{"name":"cold.Frame","value":3}]}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	graph, err := client.GetFlameGraph(context.Background(), FlameGraphRequest{
		JobID:           "job-1",
		VertexID:        "v1",
		Type:            "ON_CPU",
		SubtaskIndex:    3,
		HasSubtaskIndex: true,
	})
	if err != nil {
		t.Fatalf("GetFlameGraph returned error: %v", err)
	}
	summary := SummarizeFlameGraph(graph, 5)
	if got, want := summary.TotalSamples, int64(10); got != want {
		t.Fatalf("total samples = %d, want %d", got, want)
	}
	if got, want := summary.TopFrames[0].Name, "hot.Frame"; got != want {
		t.Fatalf("top frame = %q, want %q", got, want)
	}
	if got, want := summary.TopFrames[0].Share, 0.7; got != want {
		t.Fatalf("top frame share = %v, want %v", got, want)
	}
}

// Flink flamegraph sampling is lazy: the first request returns total=0 and
// only triggers a sampling round. GetFlameGraphWaiting must poll until samples
// appear so the AI gets data from one invocation.
func TestGetFlameGraphWaitingPollsUntilSamplesAppear(t *testing.T) {
	var calls int
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/job-1/vertices/v1/flamegraph", func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls < 3 {
			// First two calls: sampling still in progress, empty tree.
			_, _ = w.Write([]byte(`{"endTimestamp":-3,"data":{"name":"root","value":0}}`))
			return
		}
		_, _ = w.Write([]byte(`{"endTimestamp":100,"data":{"name":"root","value":10,"children":[{"name":"hot.Frame","value":10}]}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	graph, err := client.GetFlameGraphWaiting(context.Background(), FlameGraphRequest{JobID: "job-1", VertexID: "v1"}, 2*time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("GetFlameGraphWaiting error: %v", err)
	}
	if graph.Data.Value != 10 {
		t.Fatalf("expected non-empty samples after polling, got value=%d (calls=%d)", graph.Data.Value, calls)
	}
	if calls < 3 {
		t.Fatalf("expected at least 3 calls (2 empty + 1 populated), got %d", calls)
	}
}

// With waiting disabled (maxWait=0), it must do exactly one request and return
// whatever it got, even if empty.
func TestGetFlameGraphWaitingDisabledDoesSingleCall(t *testing.T) {
	var calls int
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/job-1/vertices/v1/flamegraph", func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"endTimestamp":-3,"data":{"name":"root","value":0}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	graph, err := client.GetFlameGraphWaiting(context.Background(), FlameGraphRequest{JobID: "job-1", VertexID: "v1"}, 0, 0)
	if err != nil {
		t.Fatalf("GetFlameGraphWaiting error: %v", err)
	}
	if graph.Data.Value != 0 {
		t.Fatalf("expected empty graph, got value=%d", graph.Data.Value)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 call with waiting disabled, got %d", calls)
	}
}

func TestClientListTaskManagers(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/taskmanagers", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"taskmanagers":[{"id":"container_1","slotsNumber":6,"freeSlots":0}]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	taskManagers, err := client.ListTaskManagers(context.Background())
	if err != nil {
		t.Fatalf("ListTaskManagers returned error: %v", err)
	}
	if got, want := taskManagers[0].ID, "container_1"; got != want {
		t.Fatalf("TaskManager id = %q, want %q", got, want)
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
