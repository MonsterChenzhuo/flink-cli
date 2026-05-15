package flink

import "testing"

func TestDiagnoseReportsFailuresExceptionsAndCheckpointIssues(t *testing.T) {
	snapshot := Snapshot{
		UIURL: "http://flink.example.com",
		Jobs: []JobSnapshot{{
			Overview: JobOverview{
				JID:   "job-1",
				Name:  "orders",
				State: "FAILED",
				Tasks: TaskCounts{Total: 10, Failed: 1},
			},
			Detail: JobDetail{
				Vertices: []Vertex{{
					ID:     "v1",
					Name:   "sink",
					Status: "FAILED",
					Tasks:  TaskCounts{Total: 10, Failed: 1},
					Backpressure: &BackpressureInfo{
						BackpressureLevel: "high",
					},
				}},
			},
			Exceptions: ExceptionReport{RootException: "java.lang.RuntimeException: checkpoint aborted"},
			Checkpoints: CheckpointStats{
				Counts: CheckpointCounts{Total: 5, Completed: 1, Failed: 4},
				Latest: LatestCheckpoints{
					Failed: &Checkpoint{ID: 5, Status: "FAILED", FailureMessage: "timeout"},
				},
			},
		}},
	}

	report := Diagnose(snapshot)
	if report.Summary.Critical == 0 {
		t.Fatalf("expected at least one critical finding, got %+v", report.Summary)
	}
	if !hasFinding(report.Findings, "job_failed") {
		t.Fatalf("missing job_failed finding: %+v", report.Findings)
	}
	if !hasFinding(report.Findings, "root_exception") {
		t.Fatalf("missing root_exception finding: %+v", report.Findings)
	}
	if !hasFinding(report.Findings, "checkpoint_failure_rate") {
		t.Fatalf("missing checkpoint_failure_rate finding: %+v", report.Findings)
	}
	if !hasFinding(report.Findings, "backpressure_high") {
		t.Fatalf("missing backpressure_high finding: %+v", report.Findings)
	}
	if got, want := report.Summary.JobsByState["FAILED"], 1; got != want {
		t.Fatalf("FAILED jobs = %d, want %d", got, want)
	}
}

func hasFinding(findings []Finding, ruleID string) bool {
	for _, f := range findings {
		if f.RuleID == ruleID {
			return true
		}
	}
	return false
}
