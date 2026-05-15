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
	Critical    int            `json:"critical"`
	Warn        int            `json:"warn"`
	OK          int            `json:"ok"`
	TotalJobs   int            `json:"total_jobs"`
	JobsByState map[string]int `json:"jobs_by_state"`
	Warnings    []string       `json:"warnings,omitempty"`
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
			TotalJobs:   len(snapshot.Jobs),
			JobsByState: map[string]int{},
			Warnings:    snapshot.Warnings,
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
		return severityRank(report.Findings[i].Severity) > severityRank(report.Findings[j].Severity)
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
				Evidence:   map[string]any{"job_id": jobID, "job_name": jobName, "vertex_id": vertex.ID, "vertex_name": vertex.Name, "backpressure": vertex.Backpressure},
				Suggestion: "优先检查该 vertex 下游 sink、网络缓冲、checkpoint 对齐耗时和外部系统写入延迟；若是 source 前段反压，继续沿下游 vertex 追踪。",
			})
		}
	}
	return findings
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

func round3(v float64) float64 {
	return float64(int(v*1000+0.5)) / 1000
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}
