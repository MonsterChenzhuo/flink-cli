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
								PerFlushBytesMean:           536870912,
								PerFlushMiBMean:             512,
								LoadTimeMsMean:              90000,
								WriteDataTimeMsMean:         89000,
								WriteDataShareOfLoad:        0.989,
								CommitAndPublishTimeMsMean:  40,
								CommitAndPublishTimeSecMean: 0.04,
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
	interpretation, ok := finding.Evidence["interpretation"].(map[string]any)
	if !ok {
		t.Fatalf("missing interpretation in evidence: %+v", finding.Evidence)
	}
	if got, want := interpretation["primary_bottleneck"], "doris_stream_load_write_data"; got != want {
		t.Fatalf("primary_bottleneck = %v, want %v", got, want)
	}
	if got, want := interpretation["checkpoint_likely_bottleneck"], false; got != want {
		t.Fatalf("checkpoint_likely_bottleneck = %v, want %v", got, want)
	}
}

// When backpressure propagates from the source down the chain and terminates
// at a vertex (that vertex is busy but not itself backpressured, and everything
// downstream is idle), diagnose must identify that termination vertex as the
// bottleneck instead of just flagging the source. This mirrors the real
// finderX_data_base_pre case: source=high -> flatmap=low -> process-pre
// (terminus) -> downstream idle.
func TestDiagnoseIdentifiesBackpressureChainBottleneck(t *testing.T) {
	high := &BackpressureInfo{BackpressureLevel: "high"}
	low := &BackpressureInfo{BackpressureLevel: "low"}
	ok := &BackpressureInfo{BackpressureLevel: "ok"}
	snapshot := Snapshot{
		UIURL: "http://flink.example.com",
		Jobs: []JobSnapshot{{
			Overview: JobOverview{JID: "job-1", Name: "finderX", State: "RUNNING"},
			Detail: JobDetail{
				Vertices: []Vertex{
					{ID: "source", Name: "Source: client-source", Status: "RUNNING", Backpressure: high,
						Metrics: VertexMetrics{AccumulatedBackpressuredMS: 60000, AccumulatedBusyMS: 20000}},
					{ID: "flatmap", Name: "flat-map", Status: "RUNNING", Backpressure: low,
						Metrics: VertexMetrics{AccumulatedBackpressuredMS: 30000, AccumulatedBusyMS: 24000}},
					{ID: "process-pre", Name: "process-pre", Status: "RUNNING", Backpressure: ok,
						Metrics: VertexMetrics{AccumulatedBackpressuredMS: 0, AccumulatedBusyMS: 29000, AccumulatedIdleMS: 71000}},
					{ID: "asyn", Name: "asyn-process", Status: "RUNNING", Backpressure: ok,
						Metrics: VertexMetrics{AccumulatedBackpressuredMS: 0, AccumulatedBusyMS: 4000, AccumulatedIdleMS: 96000}},
				},
			},
		}},
	}

	report := Diagnose(snapshot)
	finding := findFinding(report.Findings, "backpressure_chain")
	if finding.RuleID == "" {
		t.Fatalf("missing backpressure_chain finding: %+v", report.Findings)
	}
	if got, want := finding.Evidence["bottleneck_vertex_name"], "process-pre"; got != want {
		t.Fatalf("bottleneck vertex = %v, want %v", got, want)
	}
	if got, want := finding.Evidence["bottleneck_vertex_id"], "process-pre"; got != want {
		t.Fatalf("bottleneck vertex id = %v, want %v", got, want)
	}
}

// A chain with no backpressure at all must not emit a chain finding.
func TestDiagnoseNoBackpressureChainWhenHealthy(t *testing.T) {
	ok := &BackpressureInfo{BackpressureLevel: "ok"}
	snapshot := Snapshot{
		Jobs: []JobSnapshot{{
			Overview: JobOverview{JID: "job-1", Name: "healthy", State: "RUNNING"},
			Detail: JobDetail{
				Vertices: []Vertex{
					{ID: "a", Name: "source", Status: "RUNNING", Backpressure: ok},
					{ID: "b", Name: "sink", Status: "RUNNING", Backpressure: ok},
				},
			},
		}},
	}
	report := Diagnose(snapshot)
	if hasFinding(report.Findings, "backpressure_chain") {
		t.Fatalf("should not emit backpressure_chain when healthy: %+v", report.Findings)
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
