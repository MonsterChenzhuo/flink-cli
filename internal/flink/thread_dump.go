package flink

import (
	"regexp"
	"strings"
)

var threadStatePattern = regexp.MustCompile(`\bId=\d+\s+([A-Z_]+)\b`)

func SummarizeThreadDump(dump ThreadDump, maxInteresting int) ThreadDumpSummary {
	if maxInteresting <= 0 {
		maxInteresting = 20
	}
	summary := ThreadDumpSummary{
		Kind:         "thread_dump",
		TotalThreads: len(dump.ThreadInfos),
		States:       map[string]int{},
		Reasons:      map[string]int{},
	}
	for _, thread := range dump.ThreadInfos {
		state := threadState(thread.StringifiedThreadInfo)
		if state == "" {
			state = "UNKNOWN"
		}
		summary.States[state]++
		reason := interestingThreadReason(thread, state)
		if reason == "" {
			continue
		}
		summary.Reasons[reason]++
		summary.InterestingCount++
		if len(summary.InterestingThreads) >= maxInteresting {
			continue
		}
		summary.InterestingThreads = append(summary.InterestingThreads, ThreadSummary{
			ThreadName: thread.ThreadName,
			State:      state,
			Reason:     reason,
			TopFrames:  topStackFrames(thread.StringifiedThreadInfo, 8),
		})
	}
	summary.Interpretation = interpretThreadDumpSummary(summary)
	return summary
}

func interpretThreadDumpSummary(summary ThreadDumpSummary) string {
	if summary.TotalThreads == 0 {
		return "未采集到线程信息；确认 TaskManager id 是否正确。"
	}
	if summary.InterestingCount == 0 {
		return "本次线程快照未发现 Doris、Stream Load、checkpoint、HTTP/socket write 或 BLOCKED 等可疑线程；可对多个 TaskManager 连续采样确认。"
	}
	if summary.Reasons["doris_stream_load_socket_write"] > 0 || summary.Reasons["doris_stream_load_waiting_response"] > 0 || summary.Reasons["doris_stream_load_waiting_finish"] > 0 {
		return "线程快照存在 Doris Stream Load 写入、等待响应或等待完成线程；结合 Doris sink metrics 判断是否为写入链路瓶颈。"
	}
	if summary.Reasons["doris_stream_load_waiting_for_records"] > 0 {
		return "线程快照里部分 Doris Stream Load 上传线程在等待本地 RecordBuffer；单次快照不能单独证明 Doris 端阻塞，需要结合 loadTime/writeDataTime 和多 TaskManager 采样。"
	}
	if summary.Reasons["checkpoint"] > 0 {
		return "线程快照存在 checkpoint 相关线程；结合 checkpoint_summary 判断是否真的影响吞吐。"
	}
	return "线程快照存在可疑线程；优先查看 reasons 和 interesting_threads 的 top_frames。"
}

func threadState(s string) string {
	match := threadStatePattern.FindStringSubmatch(s)
	if len(match) != 2 {
		return ""
	}
	return match[1]
}

func interestingThreadReason(thread ThreadInfo, state string) string {
	lower := strings.ToLower(thread.ThreadName + "\n" + thread.StringifiedThreadInfo)
	switch {
	case strings.Contains(lower, "recordbuffer.read") && strings.Contains(lower, "org.apache.doris"):
		return "doris_stream_load_waiting_for_records"
	case strings.Contains(lower, "dorisstreamload.stopload") || strings.Contains(lower, "doriswriter.preparecommit"):
		return "doris_stream_load_waiting_finish"
	case strings.Contains(lower, "stream-load-upload") && strings.Contains(lower, "socketinputstream"):
		return "doris_stream_load_waiting_response"
	case strings.Contains(lower, "stream-load-upload") && (strings.Contains(lower, "socketoutputstream") || strings.Contains(lower, "socketwrite")):
		return "doris_stream_load_socket_write"
	case strings.Contains(lower, "stream-load-upload") && strings.Contains(lower, "org.apache.doris"):
		return "doris_stream_load"
	case strings.Contains(lower, "org.apache.doris"):
		return "doris"
	case strings.Contains(lower, "checkpoint"):
		return "checkpoint"
	case strings.Contains(lower, "socket") && strings.Contains(lower, "write"):
		return "socket_write"
	case strings.Contains(lower, "http") && strings.Contains(lower, "write"):
		return "http_write"
	// Use the parsed JVM thread state, not a substring match: the word
	// "blocked" can appear anywhere in a stack (method names, log lines) and
	// would otherwise flag threads that are not actually BLOCKED.
	case state == "BLOCKED":
		return "blocked"
	default:
		return ""
	}
}

func topStackFrames(raw string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	lines := strings.Split(raw, "\n")
	frames := make([]string, 0, limit)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "at ") {
			continue
		}
		frames = append(frames, strings.TrimPrefix(line, "at "))
		if len(frames) >= limit {
			break
		}
	}
	return frames
}
