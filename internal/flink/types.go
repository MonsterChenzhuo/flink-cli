package flink

import "encoding/json"

type Snapshot struct {
	UIURL            string            `json:"ui_url"`
	FlinkVersion     string            `json:"flink_version,omitempty"`
	FlinkRevision    string            `json:"flink_revision,omitempty"`
	SourceEndpoints  []string          `json:"source_endpoints,omitempty"`
	Jobs             []JobSnapshot     `json:"jobs"`
	JobManagerConfig map[string]string `json:"jobmanager_config,omitempty"`
	Warnings         []string          `json:"warnings,omitempty"`
}

type JobSnapshot struct {
	Overview         JobOverview       `json:"overview"`
	Detail           JobDetail         `json:"detail"`
	JobManagerConfig map[string]string `json:"jobmanager_config,omitempty"`
	Exceptions       ExceptionReport   `json:"exceptions"`
	Checkpoints      CheckpointStats   `json:"checkpoints"`
}

type JobOverview struct {
	JID              string     `json:"jid"`
	Name             string     `json:"name"`
	State            string     `json:"state"`
	StartTime        int64      `json:"start-time,omitempty"`
	EndTime          int64      `json:"end-time,omitempty"`
	Duration         int64      `json:"duration,omitempty"`
	LastModification int64      `json:"last-modification,omitempty"`
	Tasks            TaskCounts `json:"tasks,omitempty"`
}

type JobDetail struct {
	JID              string   `json:"jid"`
	Name             string   `json:"name"`
	State            string   `json:"state"`
	StartTime        int64    `json:"start-time,omitempty"`
	EndTime          int64    `json:"end-time,omitempty"`
	Duration         int64    `json:"duration,omitempty"`
	LastModification int64    `json:"last-modification,omitempty"`
	Vertices         []Vertex `json:"vertices,omitempty"`
}

type Vertex struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Parallelism  int               `json:"parallelism,omitempty"`
	Status       string            `json:"status"`
	StartTime    int64             `json:"start-time,omitempty"`
	EndTime      int64             `json:"end-time,omitempty"`
	Duration     int64             `json:"duration,omitempty"`
	Tasks        TaskCounts        `json:"tasks,omitempty"`
	Metrics      VertexMetrics     `json:"metrics,omitempty"`
	DorisMetrics *DorisSinkMetrics `json:"doris_metrics,omitempty"`
	Backpressure *BackpressureInfo `json:"backpressure,omitempty"`
}

type FlameGraphRequest struct {
	JobID           string
	VertexID        string
	Type            string
	SubtaskIndex    int
	HasSubtaskIndex bool
}

type FlameGraph struct {
	EndTimestamp int64          `json:"endTimestamp,omitempty"`
	Data         FlameGraphNode `json:"data"`
}

type FlameGraphNode struct {
	Name     string           `json:"name"`
	Value    int64            `json:"value"`
	Children []FlameGraphNode `json:"children,omitempty"`
}

type FlameGraphSummary struct {
	Kind           string            `json:"kind"`
	TotalSamples   int64             `json:"total_samples"`
	EndTimestamp   int64             `json:"end_timestamp,omitempty"`
	TopSelfFrames  []FlameGraphFrame `json:"top_self_frames,omitempty"`
	TopFrames      []FlameGraphFrame `json:"top_frames,omitempty"`
	TopLeafPaths   []FlameGraphPath  `json:"top_leaf_paths,omitempty"`
	Interpretation string            `json:"interpretation,omitempty"`
}

type FlameGraphFrame struct {
	Name  string  `json:"name"`
	Value int64   `json:"value"`
	Share float64 `json:"share"`
}

type FlameGraphPath struct {
	Path  []string `json:"path"`
	Value int64    `json:"value"`
	Share float64  `json:"share"`
}

type VertexMetrics struct {
	ReadBytes                  float64 `json:"read-bytes,omitempty"`
	WriteBytes                 float64 `json:"write-bytes,omitempty"`
	ReadRecords                float64 `json:"read-records,omitempty"`
	WriteRecords               float64 `json:"write-records,omitempty"`
	AccumulatedBackpressuredMS float64 `json:"accumulated-backpressured-time,omitempty"`
	AccumulatedBusyMS          float64 `json:"accumulated-busy-time,omitempty"`
	AccumulatedIdleMS          float64 `json:"accumulated-idle-time,omitempty"`
}

type DorisSinkMetrics struct {
	Summary DorisSinkMetricsSummary  `json:"summary,omitempty"`
	Samples []DorisSinkMetricsSample `json:"samples,omitempty"`
}

type DorisSinkMetricsSummary struct {
	SampledSubtasks             []int   `json:"sampled_subtasks,omitempty"`
	FlushSucceededTotal         float64 `json:"flush_succeeded_total,omitempty"`
	FlushFailedTotal            float64 `json:"flush_failed_total,omitempty"`
	PerFlushRowsMean            float64 `json:"per_flush_rows_mean,omitempty"`
	PerFlushBytesMean           float64 `json:"per_flush_bytes_mean,omitempty"`
	PerFlushMiBMean             float64 `json:"per_flush_mib_mean,omitempty"`
	PerFlushGiBMean             float64 `json:"per_flush_gib_mean,omitempty"`
	LoadTimeMsMean              float64 `json:"load_time_ms_mean,omitempty"`
	LoadTimeMsMax               float64 `json:"load_time_ms_max,omitempty"`
	LoadTimeSecMean             float64 `json:"load_time_sec_mean,omitempty"`
	LoadTimeSecMax              float64 `json:"load_time_sec_max,omitempty"`
	WriteDataTimeMsMean         float64 `json:"write_data_time_ms_mean,omitempty"`
	WriteDataTimeMsMax          float64 `json:"write_data_time_ms_max,omitempty"`
	WriteDataTimeSecMean        float64 `json:"write_data_time_sec_mean,omitempty"`
	WriteDataTimeSecMax         float64 `json:"write_data_time_sec_max,omitempty"`
	WriteDataShareOfLoad        float64 `json:"write_data_share_of_load,omitempty"`
	LoadMiBPerSecPerSubtask     float64 `json:"load_mib_per_sec_per_subtask,omitempty"`
	LoadGiBPerSecPerSubtask     float64 `json:"load_gib_per_sec_per_subtask,omitempty"`
	BeginTxnTimeMsMean          float64 `json:"begin_txn_time_ms_mean,omitempty"`
	CommitAndPublishTimeMsMean  float64 `json:"commit_and_publish_time_ms_mean,omitempty"`
	CommitAndPublishTimeSecMean float64 `json:"commit_and_publish_time_sec_mean,omitempty"`
}

type DorisSinkMetricsSample struct {
	Subtask                    int     `json:"subtask"`
	FlushSucceeded             float64 `json:"flush_succeeded,omitempty"`
	FlushFailed                float64 `json:"flush_failed,omitempty"`
	FlushLoadedRows            float64 `json:"flush_loaded_rows,omitempty"`
	FlushLoadBytes             float64 `json:"flush_load_bytes,omitempty"`
	FlushTimeMs                float64 `json:"flush_time_ms,omitempty"`
	LoadTimeMsMean             float64 `json:"load_time_ms_mean,omitempty"`
	LoadTimeMsMax              float64 `json:"load_time_ms_max,omitempty"`
	WriteDataTimeMsMean        float64 `json:"write_data_time_ms_mean,omitempty"`
	WriteDataTimeMsMax         float64 `json:"write_data_time_ms_max,omitempty"`
	BeginTxnTimeMsMean         float64 `json:"begin_txn_time_ms_mean,omitempty"`
	CommitAndPublishTimeMsMean float64 `json:"commit_and_publish_time_ms_mean,omitempty"`
	PutDataTimeMsMean          float64 `json:"put_data_time_ms_mean,omitempty"`
}

// Counts are emitted without omitempty: a 0 is a meaningful health signal
// (e.g. failed:0) and AI consumers must be able to distinguish "0 tasks" from
// "field absent / not collected".
type TaskCounts struct {
	Total       int `json:"total"`
	Created     int `json:"created"`
	Scheduled   int `json:"scheduled"`
	Deploying   int `json:"deploying"`
	Running     int `json:"running"`
	Finished    int `json:"finished"`
	Canceling   int `json:"canceling"`
	Canceled    int `json:"canceled"`
	Failed      int `json:"failed"`
	Reconciling int `json:"reconciling"`
}

type ExceptionReport struct {
	RootException string         `json:"root-exception"`
	AllExceptions []JobException `json:"all-exceptions,omitempty"`
	Truncated     bool           `json:"truncated,omitempty"`
}

type JobException struct {
	Exception string `json:"exception"`
	Task      string `json:"task,omitempty"`
	Location  string `json:"location,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
}

type CheckpointStats struct {
	Counts  CheckpointCounts       `json:"counts"`
	Summary CheckpointStatsSummary `json:"summary,omitempty"`
	Latest  LatestCheckpoints      `json:"latest,omitempty"`
}

// Counts are emitted without omitempty so failed:0 / completed:0 stay visible
// as health signals rather than silently disappearing.
type CheckpointCounts struct {
	Restored   int `json:"restored"`
	Total      int `json:"total"`
	InProgress int `json:"in_progress"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
}

type LatestCheckpoints struct {
	Completed *Checkpoint `json:"completed,omitempty"`
	Failed    *Checkpoint `json:"failed,omitempty"`
	Restored  *Checkpoint `json:"restored,omitempty"`
}

type CheckpointStatsSummary struct {
	StateSize         CheckpointMetricStats `json:"state_size,omitempty"`
	EndToEndDuration  CheckpointMetricStats `json:"end_to_end_duration,omitempty"`
	AlignmentBuffered CheckpointMetricStats `json:"alignment_buffered,omitempty"`
	ProcessedData     CheckpointMetricStats `json:"processed_data,omitempty"`
	PersistedData     CheckpointMetricStats `json:"persisted_data,omitempty"`
}

type CheckpointMetricStats struct {
	Min  float64 `json:"min,omitempty"`
	Max  float64 `json:"max,omitempty"`
	Avg  float64 `json:"avg,omitempty"`
	P50  float64 `json:"p50,omitempty"`
	P90  float64 `json:"p90,omitempty"`
	P95  float64 `json:"p95,omitempty"`
	P99  float64 `json:"p99,omitempty"`
	P999 float64 `json:"p999,omitempty"`
}

type Checkpoint struct {
	ID                 int64  `json:"id,omitempty"`
	Status             string `json:"status,omitempty"`
	IsSavepoint        bool   `json:"is_savepoint,omitempty"`
	TriggerTimestamp   int64  `json:"trigger_timestamp,omitempty"`
	LatestAckTimestamp int64  `json:"latest_ack_timestamp,omitempty"`
	EndToEndDuration   int64  `json:"end_to_end_duration,omitempty"`
	StateSize          int64  `json:"state_size,omitempty"`
	AlignmentBuffered  int64  `json:"alignment_buffered,omitempty"`
	ProcessedData      int64  `json:"processed_data,omitempty"`
	PersistedData      int64  `json:"persisted_data,omitempty"`
	FailureMessage     string `json:"failure_message,omitempty"`
}

type configEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DashboardConfig struct {
	FlinkVersion  string `json:"flink-version"`
	FlinkRevision string `json:"flink-revision"`
}

type TaskManagersResponse struct {
	TaskManagers []TaskManagerOverview `json:"taskmanagers"`
}

// TaskManagerOverview parses Flink's camelCase fields but re-tags them to
// snake_case for our own output via MarshalJSON, so AI consumers see a
// consistent naming style. The struct tags below are the *input* (Flink) names.
type TaskManagerOverview struct {
	ID        string `json:"id"`
	Path      string `json:"path,omitempty"`
	DataPort  int    `json:"dataPort,omitempty"`
	JMXPort   int    `json:"jmxPort,omitempty"`
	Slots     int    `json:"slotsNumber,omitempty"`
	FreeSlots int    `json:"freeSlots,omitempty"`
}

func (t TaskManagerOverview) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID        string `json:"id"`
		Path      string `json:"path,omitempty"`
		DataPort  int    `json:"data_port,omitempty"`
		JMXPort   int    `json:"jmx_port,omitempty"`
		Slots     int    `json:"slots_number,omitempty"`
		FreeSlots int    `json:"free_slots,omitempty"`
	}{t.ID, t.Path, t.DataPort, t.JMXPort, t.Slots, t.FreeSlots})
}

type ThreadDump struct {
	ThreadInfos []ThreadInfo `json:"threadInfos"`
}

// ThreadInfo parses Flink's camelCase fields (threadName /
// stringifiedThreadInfo) but re-emits snake_case for our output.
type ThreadInfo struct {
	ThreadName            string `json:"threadName"`
	StringifiedThreadInfo string `json:"stringifiedThreadInfo"`
}

func (t ThreadInfo) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ThreadName            string `json:"thread_name"`
		StringifiedThreadInfo string `json:"stringified_thread_info"`
	}{t.ThreadName, t.StringifiedThreadInfo})
}

type ThreadDumpSummary struct {
	Kind               string          `json:"kind"`
	TotalThreads       int             `json:"total_threads"`
	States             map[string]int  `json:"states"`
	Reasons            map[string]int  `json:"reasons,omitempty"`
	InterestingCount   int             `json:"interesting_count"`
	Interpretation     string          `json:"interpretation,omitempty"`
	InterestingThreads []ThreadSummary `json:"interesting_threads,omitempty"`
}

type ThreadSummary struct {
	ThreadName string   `json:"thread_name"`
	State      string   `json:"state,omitempty"`
	Reason     string   `json:"reason,omitempty"`
	TopFrames  []string `json:"top_frames,omitempty"`
}

// BackpressureInfo keeps both the kebab-case and camelCase level fields so it
// can unmarshal either Flink version's response, but MarshalJSON below emits a
// single normalized snake_case `backpressure_level` so AI consumers never see
// two keys for the same concept.
type BackpressureInfo struct {
	Status             string                `json:"status,omitempty"`
	BackpressureLevel  string                `json:"backpressure-level,omitempty"`
	BackpressureLevel2 string                `json:"backpressureLevel,omitempty"`
	Subtasks           []SubtaskBackpressure `json:"subtasks,omitempty"`
}

func (b BackpressureInfo) Level() string {
	if b.BackpressureLevel != "" {
		return b.BackpressureLevel
	}
	return b.BackpressureLevel2
}

func (b BackpressureInfo) MarshalJSON() ([]byte, error) {
	out := map[string]any{}
	if b.Status != "" {
		out["status"] = b.Status
	}
	if level := b.Level(); level != "" {
		out["backpressure_level"] = level
	}
	if len(b.Subtasks) > 0 {
		out["subtasks"] = b.Subtasks
	}
	return json.Marshal(out)
}

type SubtaskBackpressure struct {
	Subtask            int     `json:"subtask,omitempty"`
	BackpressureLevel  string  `json:"backpressure-level,omitempty"`
	BackpressureLevel2 string  `json:"backpressureLevel,omitempty"`
	Ratio              float64 `json:"ratio,omitempty"`
	BusyRatio          float64 `json:"busyRatio,omitempty"`
	IdleRatio          float64 `json:"idleRatio,omitempty"`
}

func (s SubtaskBackpressure) MarshalJSON() ([]byte, error) {
	out := map[string]any{
		"subtask":    s.Subtask,
		"ratio":      s.Ratio,
		"busy_ratio": s.BusyRatio,
		"idle_ratio": s.IdleRatio,
	}
	if level := s.Level(); level != "" {
		out["backpressure_level"] = level
	}
	return json.Marshal(out)
}

func (s SubtaskBackpressure) Level() string {
	if s.BackpressureLevel != "" {
		return s.BackpressureLevel
	}
	return s.BackpressureLevel2
}
