package flink

import (
	"encoding/json"
	"strings"
	"testing"
)

// failed:0 and other zero counts are health signals and must survive JSON
// serialization so AI consumers can distinguish "0 failures" from "not
// collected".
func TestTaskCountsKeepZeroCounts(t *testing.T) {
	b, err := json.Marshal(TaskCounts{Total: 30, Running: 30})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	for _, key := range []string{`"failed":0`, `"canceling":0`, `"running":30`, `"total":30`} {
		if !strings.Contains(s, key) {
			t.Fatalf("TaskCounts JSON missing %q: %s", key, s)
		}
	}
}

// BackpressureInfo must unmarshal either Flink naming but emit a single
// normalized snake_case key (no backpressure-level + backpressureLevel dual).
func TestBackpressureInfoNormalizesOutputKey(t *testing.T) {
	// camelCase input (newer Flink)
	var bp BackpressureInfo
	if err := json.Unmarshal([]byte(`{"backpressureLevel":"high","subtasks":[{"subtask":0,"backpressureLevel":"high","busyRatio":0.9}]}`), &bp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	out, err := json.Marshal(bp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"backpressure_level":"high"`) {
		t.Fatalf("expected normalized backpressure_level, got: %s", s)
	}
	for _, bad := range []string{"backpressure-level", "backpressureLevel", "busyRatio"} {
		if strings.Contains(s, bad) {
			t.Fatalf("output must not contain raw Flink key %q: %s", bad, s)
		}
	}
	if !strings.Contains(s, `"busy_ratio":0.9`) {
		t.Fatalf("expected snake_case busy_ratio: %s", s)
	}
}

func TestThreadInfoAndTaskManagerSnakeCaseOutput(t *testing.T) {
	ti, _ := json.Marshal(ThreadInfo{ThreadName: "t1", StringifiedThreadInfo: "stack"})
	if !strings.Contains(string(ti), `"thread_name"`) || strings.Contains(string(ti), "threadName") {
		t.Fatalf("ThreadInfo not snake_case: %s", ti)
	}
	tm, _ := json.Marshal(TaskManagerOverview{ID: "c1", DataPort: 1, Slots: 2, FreeSlots: 1})
	for _, bad := range []string{"dataPort", "slotsNumber", "freeSlots"} {
		if strings.Contains(string(tm), bad) {
			t.Fatalf("TaskManagerOverview leaked camelCase %q: %s", bad, tm)
		}
	}
	if !strings.Contains(string(tm), `"data_port"`) {
		t.Fatalf("expected data_port: %s", tm)
	}
}

// Each command's top-level `summary` is a different type; the `kind`
// discriminator lets AI consumers branch without guessing from field presence.
func TestSummaryKindDiscriminators(t *testing.T) {
	if got := Diagnose(Snapshot{}).Summary.Kind; got != "diagnose" {
		t.Fatalf("diagnose summary kind = %q, want diagnose", got)
	}
	fg := SummarizeFlameGraph(FlameGraph{Data: FlameGraphNode{Name: "root", Value: 1, Children: []FlameGraphNode{{Name: "a", Value: 1}}}}, 5)
	if fg.Kind != "flamegraph" {
		t.Fatalf("flamegraph summary kind = %q, want flamegraph", fg.Kind)
	}
	td := SummarizeThreadDump(ThreadDump{}, 5)
	if td.Kind != "thread_dump" {
		t.Fatalf("thread dump summary kind = %q, want thread_dump", td.Kind)
	}
}

func TestCheckpointCountsKeepZeroCounts(t *testing.T) {
	b, err := json.Marshal(CheckpointCounts{Total: 5, Completed: 5})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	for _, key := range []string{`"failed":0`, `"in_progress":0`, `"completed":5`} {
		if !strings.Contains(s, key) {
			t.Fatalf("CheckpointCounts JSON missing %q: %s", key, s)
		}
	}
}
