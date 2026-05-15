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

func TestDiagnoseReportsBusySinkWithUpstreamBackpressure(t *testing.T) {
	snapshot := Snapshot{
		UIURL: "http://flink.example.com",
		Jobs: []JobSnapshot{{
			Overview: JobOverview{
				JID:   "job-1",
				Name:  "sync-to-doris",
				State: "RUNNING",
			},
			Checkpoints: CheckpointStats{
				Counts: CheckpointCounts{Total: 54, Completed: 54, Failed: 0},
				Summary: CheckpointStatsSummary{
					EndToEndDuration:  CheckpointMetricStats{Avg: 24549, Max: 35158},
					AlignmentBuffered: CheckpointMetricStats{Avg: 0, Max: 0},
				},
				Latest: LatestCheckpoints{
					Completed: &Checkpoint{ID: 434039, EndToEndDuration: 23423, StateSize: 88272, AlignmentBuffered: 0},
				},
			},
			Detail: JobDetail{
				Vertices: []Vertex{
					{
						ID:     "source",
						Name:   "Kafka Source",
						Status: "RUNNING",
						Metrics: VertexMetrics{
							AccumulatedBackpressuredMS: 120000,
							AccumulatedBusyMS:          20000,
							AccumulatedIdleMS:          1000,
						},
					},
					{
						ID:          "sink",
						Name:        "dorisSink: Writer",
						Parallelism: 96,
						Status:      "RUNNING",
						Metrics: VertexMetrics{
							ReadBytes:                  377424408187,
							ReadRecords:                13361140,
							WriteRecords:               480,
							AccumulatedBackpressuredMS: 0,
							AccumulatedBusyMS:          24882980,
							AccumulatedIdleMS:          36574902,
						},
						DorisMetrics: &DorisSinkMetrics{
							Summary: DorisSinkMetricsSummary{
								PerFlushBytesMean:          536870912,
								LoadTimeMsMean:             90000,
								WriteDataTimeMsMean:        89000,
								CommitAndPublishTimeMsMean: 40,
							},
						},
						Backpressure: &BackpressureInfo{BackpressureLevel2: "ok"},
					},
				},
			},
		}},
	}

	report := Diagnose(snapshot)
	if !hasFinding(report.Findings, "sink_busy_upstream_backpressure") {
		t.Fatalf("missing sink busy finding: %+v", report.Findings)
	}
	finding := findFinding(report.Findings, "sink_busy_upstream_backpressure")
	if _, ok := finding.Evidence["doris_sink_metrics"]; !ok {
		t.Fatalf("missing Doris sink metrics in evidence: %+v", finding.Evidence)
	}
	if _, ok := finding.Evidence["checkpoint_summary"]; !ok {
		t.Fatalf("missing checkpoint summary in evidence: %+v", finding.Evidence)
	}
}

func hasFinding(findings []Finding, ruleID string) bool {
	return findFinding(findings, ruleID).RuleID != ""
}

func findFinding(findings []Finding, ruleID string) Finding {
	for _, f := range findings {
		if f.RuleID == ruleID {
			return f
		}
	}
	return Finding{}
}
