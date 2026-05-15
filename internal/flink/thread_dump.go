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
		reason := interestingThreadReason(thread)
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
	return summary
}

func threadState(s string) string {
	match := threadStatePattern.FindStringSubmatch(s)
	if len(match) != 2 {
		return ""
	}
	return match[1]
}

func interestingThreadReason(thread ThreadInfo) string {
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
	case strings.Contains(lower, "blocked"):
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
