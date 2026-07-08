package flink

import "testing"

// A linear call chain where the outermost frames have cumulative value == total
// but zero self-time. Only the deepest frame actually burns CPU. The self-time
// view must surface that deepest frame, not the useless root wrappers.
func TestSummarizeFlameGraphSelfTimeSurfacesRealHotspot(t *testing.T) {
	graph := FlameGraph{
		Data: FlameGraphNode{
			Name:  "root",
			Value: 100,
			Children: []FlameGraphNode{
				{
					Name:  "Thread.run",
					Value: 100,
					Children: []FlameGraphNode{
						{
							Name:  "Task.doRun",
							Value: 100,
							Children: []FlameGraphNode{
								// self = 100 - 80 = 20
								{
									Name:  "MyFunc.process",
									Value: 100,
									Children: []FlameGraphNode{
										// leaf, self = 80
										{Name: "Kryo.readClassAndObject", Value: 80},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	summary := SummarizeFlameGraph(graph, 10)

	if summary.TotalSamples != 100 {
		t.Fatalf("total_samples = %d, want 100", summary.TotalSamples)
	}
	if len(summary.TopSelfFrames) == 0 {
		t.Fatalf("top_self_frames is empty; self-time view missing")
	}
	// The deepest, actually-hot frame must rank first by self-time.
	top := summary.TopSelfFrames[0]
	if top.Name != "Kryo.readClassAndObject" {
		t.Fatalf("top self frame = %q, want %q", top.Name, "Kryo.readClassAndObject")
	}
	if top.Value != 80 {
		t.Fatalf("top self value = %d, want 80", top.Value)
	}
	if top.Share <= 0.79 || top.Share > 0.81 {
		t.Fatalf("top self share = %v, want ~0.8", top.Share)
	}
	// The zero-self-time wrapper frames (Thread.run, Task.doRun) must NOT appear
	// in the self-time view.
	for _, f := range summary.TopSelfFrames {
		if f.Name == "Thread.run" || f.Name == "Task.doRun" {
			t.Fatalf("wrapper frame %q leaked into self-time view", f.Name)
		}
	}
}

// The same method appearing in multiple branches must have its self-time summed.
func TestSummarizeFlameGraphSelfTimeAggregatesByName(t *testing.T) {
	graph := FlameGraph{
		Data: FlameGraphNode{
			Name:  "root",
			Value: 60,
			Children: []FlameGraphNode{
				{Name: "serialize", Value: 30, Children: []FlameGraphNode{
					{Name: "leaf-a", Value: 10},
				}},
				{Name: "serialize", Value: 30, Children: []FlameGraphNode{
					{Name: "leaf-b", Value: 10},
				}},
			},
		},
	}

	summary := SummarizeFlameGraph(graph, 10)

	var serializeSelf int64 = -1
	for _, f := range summary.TopSelfFrames {
		if f.Name == "serialize" {
			serializeSelf = f.Value
		}
	}
	// each "serialize" node: self = 30 - 10 = 20; two branches => 40 total
	if serializeSelf != 40 {
		t.Fatalf("serialize self-time = %d, want 40 (summed across branches)", serializeSelf)
	}
}
