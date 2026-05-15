package flink

import (
	"sort"
)

func SummarizeFlameGraph(graph FlameGraph, topN int) FlameGraphSummary {
	if topN <= 0 {
		topN = 10
	}
	total := graph.Data.Value
	summary := FlameGraphSummary{
		TotalSamples: total,
		EndTimestamp: graph.EndTimestamp,
	}
	if total <= 0 {
		summary.Interpretation = "火焰图没有返回有效采样；可能采样仍在进行、vertex 当前无运行 subtask，或 Web UI 火焰图端点不可用。"
		return summary
	}
	var frames []FlameGraphFrame
	var paths []FlameGraphPath
	collectFlameGraphRows(graph.Data, total, nil, true, &frames, &paths)
	sort.SliceStable(frames, func(i, j int) bool {
		if frames[i].Value != frames[j].Value {
			return frames[i].Value > frames[j].Value
		}
		return frames[i].Name < frames[j].Name
	})
	sort.SliceStable(paths, func(i, j int) bool {
		if paths[i].Value != paths[j].Value {
			return paths[i].Value > paths[j].Value
		}
		return len(paths[i].Path) < len(paths[j].Path)
	})
	if len(frames) > topN {
		frames = frames[:topN]
	}
	if len(paths) > topN {
		paths = paths[:topN]
	}
	summary.TopFrames = frames
	summary.TopLeafPaths = paths
	if len(frames) > 0 {
		summary.Interpretation = "优先看 top_frames[0] 和 top_leaf_paths[0]；share 表示该 frame/path 在本次采样中的占比。ON_CPU 偏 CPU 热点，OFF_CPU 偏阻塞/等待，FULL 混合两者。"
	}
	return summary
}

func collectFlameGraphRows(node FlameGraphNode, total int64, path []string, root bool, frames *[]FlameGraphFrame, paths *[]FlameGraphPath) {
	nextPath := path
	if !root && node.Name != "" {
		*frames = append(*frames, FlameGraphFrame{Name: node.Name, Value: node.Value, Share: roundShare(node.Value, total)})
		nextPath = append(append([]string{}, path...), node.Name)
	}
	if len(node.Children) == 0 {
		if !root && len(nextPath) > 0 {
			*paths = append(*paths, FlameGraphPath{Path: nextPath, Value: node.Value, Share: roundShare(node.Value, total)})
		}
		return
	}
	for _, child := range node.Children {
		collectFlameGraphRows(child, total, nextPath, false, frames, paths)
	}
}

func roundShare(value, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return round3(float64(value) / float64(total))
}
