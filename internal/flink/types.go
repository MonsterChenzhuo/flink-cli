package flink

type Snapshot struct {
	UIURL            string            `json:"ui_url"`
	FlinkVersion     string            `json:"flink_version,omitempty"`
	FlinkRevision    string            `json:"flink_revision,omitempty"`
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
	Backpressure *BackpressureInfo `json:"backpressure,omitempty"`
}

type TaskCounts struct {
	Total       int `json:"total,omitempty"`
	Created     int `json:"created,omitempty"`
	Scheduled   int `json:"scheduled,omitempty"`
	Deploying   int `json:"deploying,omitempty"`
	Running     int `json:"running,omitempty"`
	Finished    int `json:"finished,omitempty"`
	Canceling   int `json:"canceling,omitempty"`
	Canceled    int `json:"canceled,omitempty"`
	Failed      int `json:"failed,omitempty"`
	Reconciling int `json:"reconciling,omitempty"`
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
	Counts CheckpointCounts  `json:"counts"`
	Latest LatestCheckpoints `json:"latest,omitempty"`
}

type CheckpointCounts struct {
	Restored   int `json:"restored,omitempty"`
	Total      int `json:"total,omitempty"`
	InProgress int `json:"in_progress,omitempty"`
	Completed  int `json:"completed,omitempty"`
	Failed     int `json:"failed,omitempty"`
}

type LatestCheckpoints struct {
	Completed *Checkpoint `json:"completed,omitempty"`
	Failed    *Checkpoint `json:"failed,omitempty"`
	Restored  *Checkpoint `json:"restored,omitempty"`
}

type Checkpoint struct {
	ID                 int64  `json:"id,omitempty"`
	Status             string `json:"status,omitempty"`
	IsSavepoint        bool   `json:"is_savepoint,omitempty"`
	TriggerTimestamp   int64  `json:"trigger_timestamp,omitempty"`
	LatestAckTimestamp int64  `json:"latest_ack_timestamp,omitempty"`
	EndToEndDuration   int64  `json:"end_to_end_duration,omitempty"`
	StateSize          int64  `json:"state_size,omitempty"`
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

type SubtaskBackpressure struct {
	Subtask            int     `json:"subtask,omitempty"`
	BackpressureLevel  string  `json:"backpressure-level,omitempty"`
	BackpressureLevel2 string  `json:"backpressureLevel,omitempty"`
	Ratio              float64 `json:"ratio,omitempty"`
	BusyRatio          float64 `json:"busyRatio,omitempty"`
	IdleRatio          float64 `json:"idleRatio,omitempty"`
}

func (s SubtaskBackpressure) Level() string {
	if s.BackpressureLevel != "" {
		return s.BackpressureLevel
	}
	return s.BackpressureLevel2
}
