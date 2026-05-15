package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	if _, ok := env["snapshot"]; ok {
		t.Fatalf("snapshot should be omitted by default for compact agent output: %s", stdout.String())
	}
	if _, ok := env["next_actions"].([]any); !ok {
		t.Fatalf("next_actions missing from envelope: %s", stdout.String())
	}
	if _, ok := env["source_endpoints"].([]any); !ok {
		t.Fatalf("source_endpoints missing from envelope: %s", stdout.String())
	}
	if _, ok := env["primary_finding"].(map[string]any); !ok {
		t.Fatalf("primary_finding missing from envelope: %s", stdout.String())
	}
	if got, ok := env["diagnosis"].(string); !ok || got == "" {
		t.Fatalf("diagnosis missing from envelope: %s", stdout.String())
	}
}

func TestDiagnoseCommandIncludesSnapshotWhenRequested(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[]}`))
	})
	mux.HandleFunc("/jobmanager/config", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", "--include-snapshot", server.URL}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if _, ok := env["snapshot"].(map[string]any); !ok {
		t.Fatalf("snapshot missing when requested: %s", stdout.String())
	}
}

func TestDiagnoseCommandAcceptsHostPortURL(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[]}`))
	})
	mux.HandleFunc("/jobmanager/config", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", server.Listener.Addr().String()}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if got, want := env["ui_url"], "http://"+server.Listener.Addr().String(); got != want {
		t.Fatalf("ui_url = %v, want %v", got, want)
	}
}

func TestDiagnoseCommandSupportsInsecureSkipVerify(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[]}`))
	})
	mux.HandleFunc("/jobmanager/config", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	})
	server := httptest.NewTLSServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", "--insecure-skip-verify", server.URL}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if got, want := env["ui_url"], server.URL; got != want {
		t.Fatalf("ui_url = %v, want %v", got, want)
	}
}

func TestDiagnoseCommandSuggestsInsecureSkipVerifyForCertificateErrors(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[]}`))
	})
	server := httptest.NewTLSServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", server.URL}, &stdout, &stderr)
	if rc != 3 {
		t.Fatalf("RunWith rc = %d, want 3; stderr = %s", rc, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--insecure-skip-verify") {
		t.Fatalf("stderr should suggest --insecure-skip-verify: %s", stderr.String())
	}
}

func TestDiagnoseCommandPassesJobIDFilter(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"skip","state":"RUNNING"},{"jid":"job-2","name":"target","state":"FAILED"}]}`))
	})
	mux.HandleFunc("/jobs/job-2", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jid":"job-2","name":"target","state":"FAILED","vertices":[]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", "--job-id", "job-2", server.URL}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	summary := env["summary"].(map[string]any)
	if got, want := summary["total_jobs"], float64(1); got != want {
		t.Fatalf("total_jobs = %v, want %v", got, want)
	}
}

func TestDiagnoseCommandInfersJobIDFromWebUIFragment(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/proxy/application_1/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"skip","state":"RUNNING"},{"jid":"29dff7365a0c1823af622b38eeb2bd96","name":"target","state":"RUNNING"}]}`))
	})
	mux.HandleFunc("/proxy/application_1/jobs/29dff7365a0c1823af622b38eeb2bd96", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jid":"29dff7365a0c1823af622b38eeb2bd96","name":"target","state":"RUNNING","vertices":[]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	rawURL := server.URL + "/proxy/application_1/#/job/running/29dff7365a0c1823af622b38eeb2bd96/overview"
	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", rawURL}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	summary := env["summary"].(map[string]any)
	if got, want := summary["total_jobs"], float64(1); got != want {
		t.Fatalf("total_jobs = %v, want %v", got, want)
	}
}

func TestDiagnoseCommandReturnsUserErrorWhenJobIDMissing(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"orders","state":"RUNNING"}]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", "--job-id", "missing", server.URL}, &stdout, &stderr)
	if rc != 2 {
		t.Fatalf("RunWith rc = %d, want 2; stderr = %s", rc, stderr.String())
	}
	var env map[string]map[string]any
	if err := json.Unmarshal(stderr.Bytes(), &env); err != nil {
		t.Fatalf("stderr is not JSON: %v\n%s", err, stderr.String())
	}
	if got, want := env["error"]["code"], "JOB_NOT_FOUND"; got != want {
		t.Fatalf("error code = %v, want %v", got, want)
	}
}

func TestDiagnoseCommandListJobs(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"orders","state":"RUNNING"}]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", "--list-jobs", server.URL}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if got, want := env["scenario"], "list-jobs"; got != want {
		t.Fatalf("scenario = %v, want %v", got, want)
	}
	if _, ok := env["jobs"].([]any); !ok {
		t.Fatalf("jobs missing from envelope: %s", stdout.String())
	}
}

func TestDiagnoseCommandListJobsReturnsHelpfulHTMLHint(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><body>attempts page</body></html>`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", "--list-jobs", server.URL}, &stdout, &stderr)
	if rc != 3 {
		t.Fatalf("RunWith rc = %d, want 3; stderr = %s", rc, stderr.String())
	}
	if !strings.Contains(stderr.String(), "返回 HTML") || !strings.Contains(stderr.String(), "YARN application/proxy") {
		t.Fatalf("stderr should explain HTML/YARN proxy issue: %s", stderr.String())
	}
}

func TestThreadDumpCommandInfersTaskManagerIDFromWebUIFragment(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/proxy/application_1/taskmanagers/container_1/thread-dump", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"threadInfos":[{"threadName":"doris-writer","stringifiedThreadInfo":"\"doris-writer\" Id=1 RUNNABLE\n\tat java.net.SocketOutputStream.socketWrite0(Native Method)\n\tat org.apache.doris.flink.sink.writer.DorisWriter.write(DorisWriter.java:1)\n\n"}]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	rawURL := server.URL + "/proxy/application_1/#/task-manager/container_1/thread-dump"
	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"thread-dump", rawURL}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if got, want := env["scenario"], "thread-dump"; got != want {
		t.Fatalf("scenario = %v, want %v", got, want)
	}
	summary := env["summary"].(map[string]any)
	if got, want := summary["interesting_count"], float64(1); got != want {
		t.Fatalf("interesting_count = %v, want %v", got, want)
	}
}

func TestThreadDumpCommandReturnsHelpfulHTMLHint(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/taskmanagers/container_1/thread-dump", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><body>login</body></html>`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"thread-dump", "--taskmanager-id", "container_1", server.URL}, &stdout, &stderr)
	if rc != 3 {
		t.Fatalf("RunWith rc = %d, want 3; stderr = %s", rc, stderr.String())
	}
	if !strings.Contains(stderr.String(), "返回 HTML") || !strings.Contains(stderr.String(), "attempts") {
		t.Fatalf("stderr should explain HTML/YARN proxy issue: %s", stderr.String())
	}
}

func TestThreadDumpCommandListsTaskManagersWhenIDMissing(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/taskmanagers", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"taskmanagers":[{"id":"container_1"}]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"thread-dump", server.URL}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if got, want := env["scenario"], "thread-dump-list-taskmanagers"; got != want {
		t.Fatalf("scenario = %v, want %v", got, want)
	}
}

func TestDiagnoseCommandQuietWarningsSummarizesWarnings(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/overview", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jobs":[{"jid":"job-1","name":"orders","state":"RUNNING","tasks":{"total":1,"running":1}}]}`))
	})
	mux.HandleFunc("/jobs/job-1", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jid":"job-1","name":"orders","state":"RUNNING","vertices":[]}`))
	})
	mux.HandleFunc("/jobs/job-1/exceptions", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/jobs/job-1/checkpoints", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", "--quiet-warnings", server.URL}, &stdout, &stderr)
	if rc != 0 {
		t.Fatalf("RunWith rc = %d, stderr = %s", rc, stderr.String())
	}
	var env map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &env); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if _, ok := env["warnings"]; ok {
		t.Fatalf("warnings should be omitted with --quiet-warnings: %s", stdout.String())
	}
	if got, ok := env["warnings_count"].(float64); !ok || got == 0 {
		t.Fatalf("warnings_count missing: %s", stdout.String())
	}
	if got, ok := env["warnings_hint"].(string); !ok || got == "" {
		t.Fatalf("warnings_hint missing: %s", stdout.String())
	}
}

func TestDiagnoseCommandReturnsUserErrorForInvalidURL(t *testing.T) {
	var stdout, stderr bytes.Buffer
	rc := RunWith(context.Background(), []string{"diagnose", "://bad"}, &stdout, &stderr)
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
