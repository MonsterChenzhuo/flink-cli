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
		Kind:         "flamegraph",
		TotalSamples: total,
		EndTimestamp: graph.EndTimestamp,
	}
	if total <= 0 {
		summary.Interpretation = "total_samples=0 通常是正常现象：Flink 火焰图端点是惰性触发的，第一次请求只会触发一次采样并立即返回空结果，需要等约 2-5 秒后用完全相同的命令再跑一次才会有数据。请重试；如果多次重试后仍为 0，才考虑 vertex 当前无运行 subtask 或火焰图未启用（rest.flamegraph.enabled）。"
		return summary
	}
	var frames []FlameGraphFrame
	var paths []FlameGraphPath
	selfByName := map[string]int64{}
	collectFlameGraphRows(graph.Data, total, nil, true, &frames, &paths, selfByName)
	selfFrames := make([]FlameGraphFrame, 0, len(selfByName))
	for name, self := range selfByName {
		if self <= 0 {
			continue
		}
		selfFrames = append(selfFrames, FlameGraphFrame{Name: name, Value: self, Share: roundShare(self, total)})
	}
	sort.SliceStable(selfFrames, func(i, j int) bool {
		if selfFrames[i].Value != selfFrames[j].Value {
			return selfFrames[i].Value > selfFrames[j].Value
		}
		return selfFrames[i].Name < selfFrames[j].Name
	})
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
	if len(selfFrames) > topN {
		selfFrames = selfFrames[:topN]
	}
	if len(frames) > topN {
		frames = frames[:topN]
	}
	if len(paths) > topN {
		paths = paths[:topN]
	}
	summary.TopSelfFrames = selfFrames
	summary.TopFrames = frames
	summary.TopLeafPaths = paths
	if len(selfFrames) > 0 {
		summary.Interpretation = "优先看 top_self_frames[0]：它是按方法名聚合的自身耗时（self-time），share 表示该方法自身占本次采样的比例，直接对应 CPU 热点或阻塞点。top_frames 是累计耗时，最外层调用栈（Thread.run 等）share 会接近 1 但没有定位价值，只作辅助。ON_CPU 偏 CPU 热点，OFF_CPU 偏阻塞/等待，FULL 混合两者。"
	} else if len(frames) > 0 {
		summary.Interpretation = "本次采样未聚合出自身耗时较高的方法；可回退看 top_frames 和 top_leaf_paths。ON_CPU 偏 CPU 热点，OFF_CPU 偏阻塞/等待，FULL 混合两者。"
	}
	return summary
}

func collectFlameGraphRows(node FlameGraphNode, total int64, path []string, root bool, frames *[]FlameGraphFrame, paths *[]FlameGraphPath, selfByName map[string]int64) {
	nextPath := path
	if !root && node.Name != "" {
		*frames = append(*frames, FlameGraphFrame{Name: node.Name, Value: node.Value, Share: roundShare(node.Value, total)})
		nextPath = append(append([]string{}, path...), node.Name)
		var childrenValue int64
		for _, child := range node.Children {
			childrenValue += child.Value
		}
		if self := node.Value - childrenValue; self > 0 && selfByName != nil {
			selfByName[node.Name] += self
		}
	}
	if len(node.Children) == 0 {
		if !root && len(nextPath) > 0 {
			*paths = append(*paths, FlameGraphPath{Path: nextPath, Value: node.Value, Share: roundShare(node.Value, total)})
		}
		return
	}
	for _, child := range node.Children {
		collectFlameGraphRows(child, total, nextPath, false, frames, paths, selfByName)
	}
}

func roundShare(value, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return round3(float64(value) / float64(total))
}
