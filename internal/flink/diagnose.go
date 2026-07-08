package flink

import (
	"sort"
	"strings"
)

type Report struct {
	Summary  Summary   `json:"summary"`
	Findings []Finding `json:"findings"`
	Snapshot Snapshot  `json:"snapshot"`
}

type Summary struct {
	// Kind is a discriminator so AI consumers can tell which summary shape they
	// are looking at: the top-level `summary` field is a different type per
	// command (diagnose / thread_dump / flamegraph).
	Kind        string         `json:"kind"`
	Critical    int            `json:"critical"`
	Warn        int            `json:"warn"`
	OK          int            `json:"ok"`
	TotalJobs   int            `json:"total_jobs"`
	JobsByState map[string]int `json:"jobs_by_state"`
}

type Finding struct {
	RuleID     string         `json:"rule_id"`
	Severity   string         `json:"severity"`
	Title      string         `json:"title"`
	Evidence   map[string]any `json:"evidence,omitempty"`
	Suggestion string         `json:"suggestion"`
}

func Diagnose(snapshot Snapshot) Report {
	report := Report{
		Summary: Summary{
			Kind:        "diagnose",
			TotalJobs:   len(snapshot.Jobs),
			JobsByState: map[string]int{},
		},
		Snapshot: snapshot,
	}
	for _, job := range snapshot.Jobs {
		state := firstNonEmpty(job.Overview.State, job.Detail.State)
		if state == "" {
			state = "UNKNOWN"
		}
		report.Summary.JobsByState[state]++
		report.Findings = append(report.Findings, diagnoseJob(job)...)
	}
	if len(report.Findings) == 0 {
		report.Findings = append(report.Findings, Finding{
			RuleID:     "no_obvious_issue",
			Severity:   "ok",
			Title:      "未发现明显作业级异常",
			Suggestion: "继续结合业务吞吐、延迟指标和 TaskManager 日志观察。",
		})
	}
	sort.SliceStable(report.Findings, func(i, j int) bool {
		leftSeverity := severityRank(report.Findings[i].Severity)
		rightSeverity := severityRank(report.Findings[j].Severity)
		if leftSeverity != rightSeverity {
			return leftSeverity > rightSeverity
		}
		return rulePriority(report.Findings[i].RuleID) > rulePriority(report.Findings[j].RuleID)
	})
	for _, f := range report.Findings {
		switch f.Severity {
		case "critical":
			report.Summary.Critical++
		case "warn":
			report.Summary.Warn++
		default:
			report.Summary.OK++
		}
	}
	return report
}

func diagnoseJob(job JobSnapshot) []Finding {
	var findings []Finding
	jobID := firstNonEmpty(job.Overview.JID, job.Detail.JID)
	jobName := firstNonEmpty(job.Overview.Name, job.Detail.Name)
	state := firstNonEmpty(job.Overview.State, job.Detail.State)
	switch strings.ToUpper(state) {
	case "FAILED", "FAILING":
		findings = append(findings, Finding{
			RuleID:     "job_failed",
			Severity:   "critical",
			Title:      "Flink 作业处于失败状态",
			Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "state": state, "tasks": job.Overview.Tasks},
			Suggestion: "先查看 root_exception 和失败 vertex；如果异常来自 checkpoint 或 sink，优先排查外部存储、网络和状态后端。",
		})
	case "CANCELED", "CANCELLING":
		findings = append(findings, Finding{
			RuleID:     "job_canceled",
			Severity:   "warn",
			Title:      "Flink 作业已取消或正在取消",
			Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "state": state},
			Suggestion: "确认是否为人为停止；若不是，继续查看 YARN Application diagnostics 和 JobManager 日志。",
		})
	case "RESTARTING":
		findings = append(findings, Finding{
			RuleID:     "job_restarting",
			Severity:   "warn",
			Title:      "Flink 作业正在重启",
			Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "state": state},
			Suggestion: "结合异常链判断是否为外部依赖抖动、checkpoint 超时或反压导致的连续失败。",
		})
	}
	if job.Overview.Tasks.Failed > 0 || job.Overview.Tasks.Canceling > 0 {
		findings = append(findings, Finding{
			RuleID:     "task_state_abnormal",
			Severity:   "critical",
			Title:      "作业存在失败或取消中的 task",
			Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "tasks": job.Overview.Tasks},
			Suggestion: "定位失败 task 所属 vertex，并检查对应 TaskManager 日志与外部依赖错误。",
		})
	}
	root := strings.TrimSpace(job.Exceptions.RootException)
	if root != "" {
		findings = append(findings, Finding{
			RuleID:     "root_exception",
			Severity:   "critical",
			Title:      "Flink REST API 返回 root exception",
			Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "root_exception": truncate(root, 1200)},
			Suggestion: "优先按 root exception 的最内层 cause 排查；如果堆栈被截断，进入 Web UI 或日志系统查看完整 JobManager/TaskManager 日志。",
		})
	}
	findings = append(findings, diagnoseCheckpoints(jobID, jobName, job.Checkpoints)...)
	for _, vertex := range job.Detail.Vertices {
		status := strings.ToUpper(vertex.Status)
		if status == "FAILED" || status == "CANCELED" || status == "CANCELING" {
			findings = append(findings, Finding{
				RuleID:     "vertex_failed",
				Severity:   "critical",
				Title:      "作业 vertex 处于异常状态",
				Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "vertex_id": vertex.ID, "vertex_name": vertex.Name, "status": vertex.Status, "tasks": vertex.Tasks},
				Suggestion: "围绕该 vertex 的算子逻辑、上游输入、下游 sink 和 TaskManager 日志继续下钻。",
			})
		}
		if vertex.Backpressure != nil && strings.ToLower(vertex.Backpressure.Level()) == "high" {
			findings = append(findings, Finding{
				RuleID:     "backpressure_high",
				Severity:   "warn",
				Title:      "作业 vertex 存在高反压",
				Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "vertex_id": vertex.ID, "vertex_name": vertex.Name, "backpressure": summarizeBackpressure(*vertex.Backpressure)},
				Suggestion: "优先检查该 vertex 下游 sink、网络缓冲、checkpoint 对齐耗时和外部系统写入延迟；若是 source 前段反压，继续沿下游 vertex 追踪。",
			})
		}
	}
	findings = append(findings, diagnoseBackpressureChain(jobID, jobName, job.Detail.Vertices)...)
	findings = append(findings, diagnoseBusySinkBackpressure(jobID, jobName, job.Checkpoints, job.Detail.Vertices)...)
	return findings
}

// diagnoseBackpressureChain walks vertices in topological order. When upstream
// vertices are backpressured (high/low) and the chain terminates at a vertex
// that is itself busy but NOT high-backpressured (its downstream is not pushing
// back), that terminus is the real bottleneck. This saves the AI from manually
// querying every vertex's backpressure to find where the chain ends — exactly
// the manual work done during the finderX_data_base_pre investigation.
func diagnoseBackpressureChain(jobID, jobName string, vertices []Vertex) []Finding {
	if len(vertices) < 2 {
		return nil
	}
	// Find the deepest vertex that is still backpressured (high or low).
	lastBackpressuredIdx := -1
	for i, v := range vertices {
		if v.Backpressure == nil {
			continue
		}
		switch strings.ToLower(v.Backpressure.Level()) {
		case "high", "low":
			lastBackpressuredIdx = i
		}
	}
	if lastBackpressuredIdx < 0 {
		return nil
	}
	// The bottleneck is the first vertex AFTER the last backpressured one that
	// is not itself backpressured — that's where the chain terminates. If the
	// last backpressured vertex is the final vertex, it is its own terminus.
	bottleneckIdx := lastBackpressuredIdx + 1
	if bottleneckIdx >= len(vertices) {
		bottleneckIdx = lastBackpressuredIdx
	}
	// Require that at least one upstream vertex is actually high (a lone "low"
	// is not worth a dedicated chain finding — backpressure_high covers it).
	hasHigh := false
	chain := make([]map[string]any, 0, bottleneckIdx+1)
	for i := 0; i <= lastBackpressuredIdx; i++ {
		v := vertices[i]
		level := ""
		if v.Backpressure != nil {
			level = strings.ToLower(v.Backpressure.Level())
		}
		if level == "high" {
			hasHigh = true
		}
		chain = append(chain, map[string]any{
			"vertex_id":          v.ID,
			"vertex_name":        v.Name,
			"backpressure_level": level,
		})
	}
	if !hasHigh {
		return nil
	}
	bottleneck := vertices[bottleneckIdx]
	bottleneckLevel := ""
	if bottleneck.Backpressure != nil {
		bottleneckLevel = strings.ToLower(bottleneck.Backpressure.Level())
	}
	evidence := map[string]any{
		"job_id":                        jobID,
		"job_name":                      jobName,
		"bottleneck_vertex_id":          bottleneck.ID,
		"bottleneck_vertex_name":        bottleneck.Name,
		"bottleneck_backpressure_level": bottleneckLevel,
		"upstream_chain":                chain,
	}
	if bottleneck.Backpressure != nil {
		evidence["bottleneck_backpressure"] = summarizeBackpressure(*bottleneck.Backpressure)
	}
	evidence["interpretation"] = map[string]any{
		"reasoning":  "反压从上游一路传导，到该 vertex 终止（它自身不再被下游反压）。反压链终止点通常就是真正的瓶颈算子。",
		"next_focus": "优先分析 bottleneck_vertex 的算子逻辑、CPU（用 flamegraph --type ON_CPU 看 self-time 热点）、序列化开销、外部系统写入或数据倾斜，而不是只看最上游的 source。",
	}
	return []Finding{{
		RuleID:     "backpressure_chain",
		Severity:   "warn",
		Title:      "反压链路分析：定位到瓶颈终止 vertex",
		Evidence:   evidence,
		Suggestion: "反压链终止在 bottleneck_vertex；对它执行 flink-cli flamegraph --job-id <jobId> --vertex-id " + bottleneck.ID + " --type ON_CPU <url> 看 CPU 热点，并检查该算子序列化、外部写入和数据分布。",
	}}
}

func diagnoseBusySinkBackpressure(jobID, jobName string, checkpoints CheckpointStats, vertices []Vertex) []Finding {
	if len(vertices) < 2 {
		return nil
	}
	var upstreamBackpressured bool
	var upstreamEvidence []map[string]any
	for _, vertex := range vertices {
		if vertex.Metrics.AccumulatedBackpressuredMS <= 0 {
			continue
		}
		upstreamBackpressured = true
		upstreamEvidence = append(upstreamEvidence, map[string]any{
			"vertex_id":                     vertex.ID,
			"vertex_name":                   vertex.Name,
			"accumulated_backpressured":     vertex.Metrics.AccumulatedBackpressuredMS,
			"accumulated_backpressured_sec": round3(vertex.Metrics.AccumulatedBackpressuredMS / 1000),
		})
	}
	if !upstreamBackpressured {
		return nil
	}
	var findings []Finding
	for _, vertex := range vertices {
		if !looksLikeSink(vertex.Name) {
			continue
		}
		total := vertex.Metrics.AccumulatedBusyMS + vertex.Metrics.AccumulatedIdleMS + vertex.Metrics.AccumulatedBackpressuredMS
		if total <= 0 {
			continue
		}
		busyRatio := vertex.Metrics.AccumulatedBusyMS / total
		if busyRatio < 0.30 || vertex.Metrics.AccumulatedBackpressuredMS > 0 {
			continue
		}
		evidence := map[string]any{
			"job_id":                    jobID,
			"job_name":                  jobName,
			"vertex_id":                 vertex.ID,
			"vertex_name":               vertex.Name,
			"parallelism":               vertex.Parallelism,
			"busy_ratio":                round3(busyRatio),
			"read_bytes":                vertex.Metrics.ReadBytes,
			"read_gib":                  round3(vertex.Metrics.ReadBytes / 1024 / 1024 / 1024),
			"read_records":              vertex.Metrics.ReadRecords,
			"write_records":             vertex.Metrics.WriteRecords,
			"accumulated_busy_ms":       vertex.Metrics.AccumulatedBusyMS,
			"accumulated_busy_sec":      round3(vertex.Metrics.AccumulatedBusyMS / 1000),
			"accumulated_idle_ms":       vertex.Metrics.AccumulatedIdleMS,
			"accumulated_idle_sec":      round3(vertex.Metrics.AccumulatedIdleMS / 1000),
			"upstream_backpressured_ms": upstreamEvidence,
		}
		if vertex.DorisMetrics != nil {
			evidence["doris_sink_metrics"] = map[string]any{"summary": vertex.DorisMetrics.Summary}
		}
		if checkpointSummary := summarizeCheckpointStats(checkpoints); len(checkpointSummary) > 0 {
			evidence["checkpoint_summary"] = checkpointSummary
		}
		if interpretation := interpretSinkPressure(vertex, checkpoints); len(interpretation) > 0 {
			evidence["interpretation"] = interpretation
		}
		findings = append(findings, Finding{
			RuleID:     "sink_busy_upstream_backpressure",
			Severity:   "warn",
			Title:      "Sink 写入繁忙并导致上游反压",
			Evidence:   evidence,
			Suggestion: "Sink 自身未必显示 high backpressure；优先检查外部写入系统吞吐、sink flush/load/commit 指标、批次大小和 sink 并发。Doris 场景重点看 Stream Load 的 writeDataTimeMs/loadTimeMs。",
		})
	}
	return findings
}

func interpretSinkPressure(vertex Vertex, checkpoints CheckpointStats) map[string]any {
	out := map[string]any{}
	if vertex.DorisMetrics != nil {
		summary := vertex.DorisMetrics.Summary
		if summary.LoadTimeMsMean > 0 && summary.WriteDataTimeMsMean > 0 {
			writeDataShare := summary.WriteDataShareOfLoad
			if writeDataShare == 0 {
				writeDataShare = round3(summary.WriteDataTimeMsMean / summary.LoadTimeMsMean)
			}
			out["write_data_share_of_load"] = writeDataShare
			if writeDataShare >= 0.80 && summary.LoadTimeMsMean >= 30000 {
				out["primary_bottleneck"] = "doris_stream_load_write_data"
				out["doris_commit_publish_likely_bottleneck"] = false
			}
		}
		if summary.CommitAndPublishTimeMsMean > 0 {
			out["commit_publish_sec_mean"] = summary.CommitAndPublishTimeSecMean
		}
		if summary.PerFlushMiBMean > 0 {
			out["per_flush_mib_mean"] = summary.PerFlushMiBMean
		}
	}
	if checkpoints.Counts.Total > 0 {
		checkpointLikelyBottleneck := checkpoints.Counts.Failed > 0 || checkpoints.Counts.InProgress > 0
		latestDuration := int64(0)
		alignmentBuffered := int64(0)
		if checkpoints.Latest.Completed != nil {
			latestDuration = checkpoints.Latest.Completed.EndToEndDuration
			alignmentBuffered = checkpoints.Latest.Completed.AlignmentBuffered
			if latestDuration >= 60000 || alignmentBuffered > 0 {
				checkpointLikelyBottleneck = true
			}
		}
		out["checkpoint_likely_bottleneck"] = checkpointLikelyBottleneck
		out["checkpoint_completed"] = checkpoints.Counts.Completed
		out["checkpoint_failed"] = checkpoints.Counts.Failed
		if latestDuration > 0 {
			out["checkpoint_latest_duration_sec"] = round3(float64(latestDuration) / 1000)
		}
		out["checkpoint_alignment_buffered_bytes"] = alignmentBuffered
	}
	if len(out) == 0 {
		return nil
	}
	if out["primary_bottleneck"] == "doris_stream_load_write_data" {
		out["next_focus"] = "检查 Doris BE 写入吞吐、tablet/partition 热点、compaction/load queue；或降低单次 Stream Load 批次大小。"
	}
	return out
}

func summarizeCheckpointStats(stats CheckpointStats) map[string]any {
	if stats.Counts.Total == 0 {
		return nil
	}
	out := map[string]any{
		"counts": stats.Counts,
	}
	if stats.Latest.Completed != nil {
		out["latest_completed"] = map[string]any{
			"id":                         stats.Latest.Completed.ID,
			"end_to_end_duration_ms":     stats.Latest.Completed.EndToEndDuration,
			"end_to_end_duration_sec":    round3(float64(stats.Latest.Completed.EndToEndDuration) / 1000),
			"state_size_bytes":           stats.Latest.Completed.StateSize,
			"alignment_buffered_bytes":   stats.Latest.Completed.AlignmentBuffered,
			"latest_ack_timestamp_epoch": stats.Latest.Completed.LatestAckTimestamp,
		}
	}
	if stats.Summary.EndToEndDuration.Avg > 0 || stats.Summary.EndToEndDuration.Max > 0 {
		out["end_to_end_duration"] = map[string]any{
			"avg_ms":  stats.Summary.EndToEndDuration.Avg,
			"avg_sec": round3(float64(stats.Summary.EndToEndDuration.Avg) / 1000),
			"max_ms":  stats.Summary.EndToEndDuration.Max,
			"max_sec": round3(float64(stats.Summary.EndToEndDuration.Max) / 1000),
		}
	}
	if stats.Summary.AlignmentBuffered.Avg > 0 || stats.Summary.AlignmentBuffered.Max > 0 {
		out["alignment_buffered"] = map[string]any{
			"avg_bytes": stats.Summary.AlignmentBuffered.Avg,
			"max_bytes": stats.Summary.AlignmentBuffered.Max,
		}
	}
	if stats.Summary.StateSize.Avg > 0 || stats.Summary.StateSize.Max > 0 {
		out["state_size"] = map[string]any{
			"avg_bytes": stats.Summary.StateSize.Avg,
			"max_bytes": stats.Summary.StateSize.Max,
		}
	}
	return out
}

func summarizeBackpressure(bp BackpressureInfo) map[string]any {
	out := map[string]any{
		"status": bp.Status,
		"level":  bp.Level(),
	}
	if len(bp.Subtasks) == 0 {
		return out
	}
	out["subtasks_total"] = len(bp.Subtasks)
	var high, low, okCount int
	var ratioSum, busySum, idleSum, maxRatio float64
	for _, subtask := range bp.Subtasks {
		switch strings.ToLower(subtask.Level()) {
		case "high":
			high++
		case "low":
			low++
		case "ok":
			okCount++
		}
		ratioSum += subtask.Ratio
		busySum += subtask.BusyRatio
		idleSum += subtask.IdleRatio
		if subtask.Ratio > maxRatio {
			maxRatio = subtask.Ratio
		}
	}
	total := float64(len(bp.Subtasks))
	out["subtasks_high"] = high
	out["subtasks_low"] = low
	out["subtasks_ok"] = okCount
	out["ratio_avg"] = round3(ratioSum / total)
	out["ratio_max"] = round3(maxRatio)
	out["busy_ratio_avg"] = round3(busySum / total)
	out["idle_ratio_avg"] = round3(idleSum / total)
	return out
}

func looksLikeSink(name string) bool {
	n := strings.ToLower(name)
	return strings.Contains(n, "sink") || strings.Contains(n, "doris") || strings.Contains(n, "writer") || strings.Contains(n, "committer")
}

func diagnoseCheckpoints(jobID, jobName string, stats CheckpointStats) []Finding {
	counts := stats.Counts
	if counts.Total == 0 {
		return nil
	}
	var findings []Finding
	if counts.Failed > 0 {
		rate := float64(counts.Failed) / float64(counts.Completed+counts.Failed)
		severity := "warn"
		if counts.Completed == 0 || rate >= 0.5 {
			severity = "critical"
		}
		findings = append(findings, Finding{
			RuleID:     "checkpoint_failure_rate",
			Severity:   severity,
			Title:      "Checkpoint 失败比例偏高",
			Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "total": counts.Total, "completed": counts.Completed, "failed": counts.Failed, "failure_rate": round3(rate), "latest_failed": stats.Latest.Failed},
			Suggestion: "检查状态后端、checkpoint 存储、下游 sink 提交耗时、网络抖动和反压；必要时调大 checkpoint timeout 或降低并发压力。",
		})
	}
	if stats.Latest.Completed != nil && stats.Latest.Completed.EndToEndDuration >= 60000 {
		findings = append(findings, Finding{
			RuleID:     "checkpoint_slow",
			Severity:   "warn",
			Title:      "最近一次成功 checkpoint 耗时较长",
			Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "latest_completed": stats.Latest.Completed},
			Suggestion: "关注 state size、对齐耗时、异步持久化和 checkpoint 存储吞吐；长耗时会放大恢复时间和失败窗口。",
		})
	}
	return findings
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func severityRank(s string) int {
	switch s {
	case "critical":
		return 3
	case "warn":
		return 2
	default:
		return 1
	}
}

func rulePriority(ruleID string) int {
	switch ruleID {
	case "root_exception", "job_failed", "vertex_failed":
		return 30
	case "sink_busy_upstream_backpressure":
		return 25
	case "backpressure_chain":
		return 22
	case "backpressure_high":
		return 20
	case "checkpoint_failure_rate", "checkpoint_slow":
		return 15
	default:
		return 0
	}
}

func round3(v float64) float64 {
	return float64(int(v*1000+0.5)) / 1000
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}
