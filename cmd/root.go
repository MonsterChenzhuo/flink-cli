package cmd

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/MonsterChenzhuo/flink-cli/internal/apperr"
	"github.com/MonsterChenzhuo/flink-cli/internal/flink"
	"github.com/MonsterChenzhuo/flink-cli/internal/output"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)
var exitCode int

type envelope struct {
	SchemaVersion   string          `json:"schema_version"`
	Scenario        string          `json:"scenario"`
	UIURL           string          `json:"ui_url"`
	FlinkVersion    string          `json:"flink_version,omitempty"`
	ElapsedMs       int64           `json:"elapsed_ms"`
	SourceEndpoints []string        `json:"source_endpoints"`
	Warnings        []string        `json:"warnings,omitempty"`
	WarningsCount   int             `json:"warnings_count,omitempty"`
	WarningsHint    string          `json:"warnings_hint,omitempty"`
	Summary         flink.Summary   `json:"summary"`
	Findings        []flink.Finding `json:"findings"`
	PrimaryFinding  *flink.Finding  `json:"primary_finding,omitempty"`
	Diagnosis       string          `json:"diagnosis"`
	NextActions     []string        `json:"next_actions"`
	Snapshot        *flink.Snapshot `json:"snapshot,omitempty"`
}

type state struct {
	timeout          time.Duration
	includeSnapshot  bool
	jobID            string
	vertexID         string
	taskManagerID    string
	maxVertices      int
	listJobs         bool
	listTaskManagers bool
	quietWarnings    bool
	insecureTLS      bool
	includeThreads   bool
	maxThreads       int
	flameGraphType   string
	subtaskIndex     int
	topFrames        int
	includeRaw       bool
}

type listJobsEnvelope struct {
	SchemaVersion   string              `json:"schema_version"`
	Scenario        string              `json:"scenario"`
	UIURL           string              `json:"ui_url"`
	ElapsedMs       int64               `json:"elapsed_ms"`
	SourceEndpoints []string            `json:"source_endpoints"`
	Jobs            []flink.JobOverview `json:"jobs"`
	NextActions     []string            `json:"next_actions"`
}

type threadDumpEnvelope struct {
	SchemaVersion   string                      `json:"schema_version"`
	Scenario        string                      `json:"scenario"`
	UIURL           string                      `json:"ui_url"`
	ElapsedMs       int64                       `json:"elapsed_ms"`
	SourceEndpoints []string                    `json:"source_endpoints"`
	TaskManagerID   string                      `json:"taskmanager_id,omitempty"`
	TaskManagers    []flink.TaskManagerOverview `json:"taskmanagers,omitempty"`
	Summary         *flink.ThreadDumpSummary    `json:"summary,omitempty"`
	ThreadInfos     []flink.ThreadInfo          `json:"thread_infos,omitempty"`
	NextActions     []string                    `json:"next_actions"`
}

type flameGraphEnvelope struct {
	SchemaVersion   string                   `json:"schema_version"`
	Scenario        string                   `json:"scenario"`
	UIURL           string                   `json:"ui_url"`
	ElapsedMs       int64                    `json:"elapsed_ms"`
	SourceEndpoints []string                 `json:"source_endpoints"`
	JobID           string                   `json:"job_id,omitempty"`
	VertexID        string                   `json:"vertex_id,omitempty"`
	Type            string                   `json:"type,omitempty"`
	SubtaskIndex    *int                     `json:"subtask_index,omitempty"`
	Vertices        []flink.Vertex           `json:"vertices,omitempty"`
	Summary         *flink.FlameGraphSummary `json:"summary,omitempty"`
	FlameGraph      *flink.FlameGraph        `json:"flamegraph,omitempty"`
	NextActions     []string                 `json:"next_actions"`
}

func newRootCmd() *cobra.Command {
	st := &state{}
	root := &cobra.Command{
		Use:           "flink-cli",
		Short:         "Diagnose Flink jobs from the Web UI REST API.",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}
	root.SetVersionTemplate("flink-cli {{.Version}}\n")
	root.PersistentFlags().DurationVar(&st.timeout, "timeout", 30*time.Second, "HTTP timeout for Flink REST API calls")
	root.AddCommand(newDiagnoseCmd(st))
	root.AddCommand(newThreadDumpCmd(st))
	root.AddCommand(newFlameGraphCmd(st))
	root.AddCommand(newVersionCmd())
	return root
}

func newDiagnoseCmd(st *state) *cobra.Command {
	c := &cobra.Command{
		Use:   "diagnose <flink-web-ui-url>",
		Short: "Collect Flink REST API details and emit diagnosis JSON.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			exitCode = runDiagnose(cmd.Context(), args[0], *st, cmd.OutOrStdout(), cmd.ErrOrStderr())
			return nil
		},
	}
	c.Flags().BoolVar(&st.includeSnapshot, "include-snapshot", false, "include full collected REST snapshot in stdout")
	c.Flags().BoolVar(&st.listJobs, "list-jobs", false, "only list jobs from /jobs/overview without fetching job details")
	c.Flags().BoolVar(&st.quietWarnings, "quiet-warnings", false, "omit warning details and keep only warning count/hint")
	c.Flags().BoolVar(&st.insecureTLS, "insecure-skip-verify", false, "skip HTTPS server certificate verification for internal YARN/Flink gateways")
	c.Flags().StringVar(&st.jobID, "job-id", "", "diagnose only the matching Flink job id from /jobs/overview")
	c.Flags().IntVar(&st.maxVertices, "max-vertices", 20, "maximum vertices per job to query for backpressure; 0 means no limit")
	return c
}

func newThreadDumpCmd(st *state) *cobra.Command {
	c := &cobra.Command{
		Use:   "thread-dump <flink-web-ui-url>",
		Short: "Collect a TaskManager thread dump and emit compact JSON.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			exitCode = runThreadDump(cmd.Context(), args[0], *st, cmd.OutOrStdout(), cmd.ErrOrStderr())
			return nil
		},
	}
	c.Flags().BoolVar(&st.insecureTLS, "insecure-skip-verify", false, "skip HTTPS server certificate verification for internal YARN/Flink gateways")
	c.Flags().BoolVar(&st.listTaskManagers, "list-taskmanagers", false, "only list TaskManagers without fetching a thread dump")
	c.Flags().BoolVar(&st.includeThreads, "include-threads", false, "include full thread dump entries in stdout")
	c.Flags().StringVar(&st.taskManagerID, "taskmanager-id", "", "TaskManager id from /taskmanagers; inferred from #/task-manager/<id>/thread-dump URLs when omitted")
	c.Flags().IntVar(&st.maxThreads, "max-threads", 20, "maximum interesting thread summaries to include")
	return c
}

func newFlameGraphCmd(st *state) *cobra.Command {
	c := &cobra.Command{
		Use:   "flamegraph <flink-web-ui-url>",
		Short: "Collect a vertex flame graph and emit compact JSON.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			exitCode = runFlameGraph(cmd.Context(), args[0], *st, cmd.OutOrStdout(), cmd.ErrOrStderr())
			return nil
		},
	}
	c.Flags().BoolVar(&st.insecureTLS, "insecure-skip-verify", false, "skip HTTPS server certificate verification for internal YARN/Flink gateways")
	c.Flags().StringVar(&st.jobID, "job-id", "", "Flink job id; inferred from #/job/... URLs when omitted")
	c.Flags().StringVar(&st.vertexID, "vertex-id", "", "Flink vertex id; inferred from #/job/.../vertices/<id>/... URLs when omitted")
	c.Flags().StringVar(&st.flameGraphType, "type", "FULL", "flame graph type: FULL, ON_CPU, or OFF_CPU")
	c.Flags().IntVar(&st.subtaskIndex, "subtask-index", -1, "specific subtask index to sample; -1 means aggregate vertex flame graph")
	c.Flags().IntVar(&st.topFrames, "top-frames", 10, "maximum top frames and leaf paths to include")
	c.Flags().BoolVar(&st.includeRaw, "include-raw", false, "include raw flame graph tree in stdout")
	return c
}

func Execute() int {
	exitCode = 0
	root := newRootCmd()
	if err := root.ExecuteContext(context.Background()); err != nil {
		apperr.WriteJSON(os.Stderr, normalizeErr(err))
		return 1
	}
	return exitCode
}

func RunWith(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	exitCode = 0
	root := newRootCmd()
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	if err := root.ExecuteContext(ctx); err != nil {
		apperr.WriteJSON(stderr, normalizeErr(err))
		return 1
	}
	return exitCode
}

func runDiagnose(ctx context.Context, rawURL string, st state, stdout, stderr io.Writer) int {
	if st.jobID == "" {
		st.jobID = flink.ExtractJobIDFromWebURL(rawURL)
	}
	client, err := flink.NewClientWithHTTP(rawURL, newHTTPClient(st.timeout, st.insecureTLS))
	if err != nil {
		apperr.WriteJSON(stderr, apperr.New("URL_INVALID", err.Error(), "传入完整的 Flink Web UI URL，例如 http://jobmanager:8081 或带 gateway path 的代理地址"))
		return 2
	}
	start := time.Now()
	if st.listJobs {
		return runListJobs(ctx, client, rawURL, start, stdout, stderr)
	}
	snapshot, err := client.CollectWithOptions(ctx, flink.CollectOptions{JobID: st.jobID, MaxVertices: st.maxVertices})
	if err != nil {
		var nf *flink.JobNotFoundError
		if errors.As(err, &nf) {
			apperr.WriteJSON(stderr, apperr.New("JOB_NOT_FOUND", err.Error(), "先运行 `flink-cli diagnose <url>` 查看 summary.jobs_by_state，或确认 --job-id 是否来自 /jobs/overview"))
			return 2
		}
		apperr.WriteJSON(stderr, apperr.New("FLINK_API_UNREACHABLE", err.Error(), "确认 URL 可访问，并且指向 Flink 1.18 Web UI 根路径；如果错误包含 x509 或 certificate，可重试加 --insecure-skip-verify；如果返回 HTML，检查 YARN application/proxy 是否过期或被登录页拦截"))
		return 3
	}
	report := flink.Diagnose(snapshot)
	env := envelope{
		SchemaVersion:   "v1",
		Scenario:        "diagnose",
		UIURL:           snapshot.UIURL,
		FlinkVersion:    snapshot.FlinkVersion,
		ElapsedMs:       time.Since(start).Milliseconds(),
		SourceEndpoints: snapshot.SourceEndpoints,
		Summary:         report.Summary,
		Findings:        report.Findings,
		PrimaryFinding:  primaryFinding(report),
		Diagnosis:       buildDiagnosis(report),
		NextActions:     buildNextActions(report),
	}
	if st.includeSnapshot {
		env.Snapshot = &report.Snapshot
	}
	attachWarnings(&env, snapshot.Warnings, st.quietWarnings)
	if err := output.WriteJSON(stdout, env); err != nil {
		apperr.WriteJSON(stderr, apperr.New("OUTPUT_ERROR", err.Error(), "检查 stdout 是否可写"))
		return 1
	}
	return 0
}

func runThreadDump(ctx context.Context, rawURL string, st state, stdout, stderr io.Writer) int {
	if st.taskManagerID == "" {
		st.taskManagerID = flink.ExtractTaskManagerIDFromWebURL(rawURL)
	}
	client, err := flink.NewClientWithHTTP(rawURL, newHTTPClient(st.timeout, st.insecureTLS))
	if err != nil {
		apperr.WriteJSON(stderr, apperr.New("URL_INVALID", err.Error(), "传入完整的 Flink Web UI URL，例如 http://jobmanager:8081 或带 gateway path 的代理地址"))
		return 2
	}
	start := time.Now()
	if st.listTaskManagers || st.taskManagerID == "" {
		taskManagers, err := client.ListTaskManagers(ctx)
		if err != nil {
			apperr.WriteJSON(stderr, apperr.New("FLINK_API_UNREACHABLE", err.Error(), flinkAPIHint("确认 URL 可访问，并且 /taskmanagers 可读取")))
			return 3
		}
		base, _ := flink.NormalizeBaseURL(rawURL)
		env := threadDumpEnvelope{
			SchemaVersion:   "v1",
			Scenario:        "thread-dump-list-taskmanagers",
			UIURL:           base.String(),
			ElapsedMs:       time.Since(start).Milliseconds(),
			SourceEndpoints: []string{"/taskmanagers"},
			TaskManagers:    taskManagers,
			NextActions: []string{
				"选择 taskmanagers[].id 后执行：flink-cli thread-dump --taskmanager-id <id> <url>。",
				"如果 URL 已经是 #/task-manager/<id>/thread-dump 页面，可直接传完整 URL 给 flink-cli thread-dump。",
			},
		}
		if err := output.WriteJSON(stdout, env); err != nil {
			apperr.WriteJSON(stderr, apperr.New("OUTPUT_ERROR", err.Error(), "检查 stdout 是否可写"))
			return 1
		}
		return 0
	}
	dump, err := client.GetThreadDump(ctx, st.taskManagerID)
	if err != nil {
		apperr.WriteJSON(stderr, apperr.New("FLINK_API_UNREACHABLE", err.Error(), flinkAPIHint("确认 TaskManager id 存在，并且 /taskmanagers/<id>/thread-dump 可读取")))
		return 3
	}
	base, _ := flink.NormalizeBaseURL(rawURL)
	summary := flink.SummarizeThreadDump(dump, st.maxThreads)
	env := threadDumpEnvelope{
		SchemaVersion:   "v1",
		Scenario:        "thread-dump",
		UIURL:           base.String(),
		ElapsedMs:       time.Since(start).Milliseconds(),
		SourceEndpoints: []string{"/taskmanagers/" + st.taskManagerID + "/thread-dump"},
		TaskManagerID:   st.taskManagerID,
		Summary:         &summary,
		NextActions: []string{
			"优先查看 summary.interpretation，确认本次线程快照是否命中 Doris、HTTP/socket write、checkpoint 或 BLOCKED 等特征。",
			"再查看 summary.reasons 和 summary.interesting_threads 的 top_frames；interesting_count=0 只代表本次快照没命中特征，不代表作业没有问题。",
			"需要完整线程栈时重新执行：flink-cli thread-dump --include-threads --taskmanager-id <id> <url>。",
		},
	}
	if st.includeThreads {
		env.ThreadInfos = dump.ThreadInfos
	}
	if err := output.WriteJSON(stdout, env); err != nil {
		apperr.WriteJSON(stderr, apperr.New("OUTPUT_ERROR", err.Error(), "检查 stdout 是否可写"))
		return 1
	}
	return 0
}

func runFlameGraph(ctx context.Context, rawURL string, st state, stdout, stderr io.Writer) int {
	if st.jobID == "" {
		st.jobID = flink.ExtractJobIDFromWebURL(rawURL)
	}
	if st.vertexID == "" {
		st.vertexID = flink.ExtractVertexIDFromWebURL(rawURL)
	}
	client, err := flink.NewClientWithHTTP(rawURL, newHTTPClient(st.timeout, st.insecureTLS))
	if err != nil {
		apperr.WriteJSON(stderr, apperr.New("URL_INVALID", err.Error(), "传入完整的 Flink Web UI URL，例如 http://jobmanager:8081 或带 gateway path 的代理地址"))
		return 2
	}
	start := time.Now()
	base, _ := flink.NormalizeBaseURL(rawURL)
	if st.jobID == "" {
		jobs, err := client.ListJobs(ctx)
		if err != nil {
			apperr.WriteJSON(stderr, apperr.New("FLINK_API_UNREACHABLE", err.Error(), flinkAPIHint("确认 URL 可访问，并且 /jobs/overview 可读取")))
			return 3
		}
		env := listJobsEnvelope{
			SchemaVersion:   "v1",
			Scenario:        "list-jobs",
			UIURL:           base.String(),
			ElapsedMs:       time.Since(start).Milliseconds(),
			SourceEndpoints: []string{"/jobs/overview"},
			Jobs:            jobs,
			NextActions: []string{
				"选择目标 jobs[].jid 后执行：flink-cli flamegraph --job-id <jobId> <url>。",
				"如果已经在 Flink Web UI 的 job 页面，可直接传完整 URL，CLI 会从 fragment 推断 job id。",
			},
		}
		if err := output.WriteJSON(stdout, env); err != nil {
			apperr.WriteJSON(stderr, apperr.New("OUTPUT_ERROR", err.Error(), "检查 stdout 是否可写"))
			return 1
		}
		return 0
	}
	if st.vertexID == "" {
		detail, err := client.GetJobDetail(ctx, st.jobID)
		if err != nil {
			apperr.WriteJSON(stderr, apperr.New("FLINK_API_UNREACHABLE", err.Error(), flinkAPIHint("确认 job id 存在，并且 /jobs/<jobid> 可读取")))
			return 3
		}
		env := flameGraphEnvelope{
			SchemaVersion:   "v1",
			Scenario:        "flamegraph-list-vertices",
			UIURL:           base.String(),
			ElapsedMs:       time.Since(start).Milliseconds(),
			SourceEndpoints: []string{"/jobs/" + st.jobID},
			JobID:           st.jobID,
			Vertices:        detail.Vertices,
			NextActions: []string{
				"选择 vertices[].id 后执行：flink-cli flamegraph --job-id <jobId> --vertex-id <vertexId> <url>。",
				"可用 --type ON_CPU 看 CPU 热点，--type OFF_CPU 看阻塞/等待；需要单个 subtask 时加 --subtask-index <n>。",
			},
		}
		if err := output.WriteJSON(stdout, env); err != nil {
			apperr.WriteJSON(stderr, apperr.New("OUTPUT_ERROR", err.Error(), "检查 stdout 是否可写"))
			return 1
		}
		return 0
	}
	req := flink.FlameGraphRequest{
		JobID:    st.jobID,
		VertexID: st.vertexID,
		Type:     st.flameGraphType,
	}
	var subtaskIndex *int
	if st.subtaskIndex >= 0 {
		req.SubtaskIndex = st.subtaskIndex
		req.HasSubtaskIndex = true
		subtaskIndex = &st.subtaskIndex
	}
	graph, err := client.GetFlameGraph(ctx, req)
	if err != nil {
		apperr.WriteJSON(stderr, apperr.New("FLINK_API_UNREACHABLE", err.Error(), flinkAPIHint("确认 job/vertex 仍在运行、火焰图已启用，并且 /jobs/<jobid>/vertices/<vertexid>/flamegraph 可读取")))
		return 3
	}
	sourceEndpoint := "/jobs/" + st.jobID + "/vertices/" + st.vertexID + "/flamegraph"
	summary := flink.SummarizeFlameGraph(graph, st.topFrames)
	env := flameGraphEnvelope{
		SchemaVersion:   "v1",
		Scenario:        "flamegraph",
		UIURL:           base.String(),
		ElapsedMs:       time.Since(start).Milliseconds(),
		SourceEndpoints: []string{sourceEndpoint},
		JobID:           st.jobID,
		VertexID:        st.vertexID,
		Type:            st.flameGraphType,
		SubtaskIndex:    subtaskIndex,
		Summary:         &summary,
		NextActions: []string{
			"优先看 summary.top_frames 和 summary.top_leaf_paths；share 越高说明该 frame/path 在本次采样占比越大。",
			"ON_CPU 用于定位 CPU 热点；OFF_CPU 用于定位阻塞、IO 等待或锁等待；FULL 用于粗看整体。",
			"需要原始火焰图树时重新执行：flink-cli flamegraph --include-raw --job-id <jobId> --vertex-id <vertexId> <url>。",
		},
	}
	if st.includeRaw {
		env.FlameGraph = &graph
	}
	if err := output.WriteJSON(stdout, env); err != nil {
		apperr.WriteJSON(stderr, apperr.New("OUTPUT_ERROR", err.Error(), "检查 stdout 是否可写"))
		return 1
	}
	return 0
}

func attachWarnings(env *envelope, warnings []string, quiet bool) {
	if len(warnings) == 0 {
		return
	}
	if quiet {
		env.WarningsCount = len(warnings)
		env.WarningsHint = "非致命采集 warning 已隐藏；去掉 --quiet-warnings 可查看完整 warning。"
		return
	}
	env.Warnings = warnings
}

func runListJobs(ctx context.Context, client *flink.Client, rawURL string, start time.Time, stdout, stderr io.Writer) int {
	jobs, err := client.ListJobs(ctx)
	if err != nil {
		apperr.WriteJSON(stderr, apperr.New("FLINK_API_UNREACHABLE", err.Error(), flinkAPIHint("确认 URL 可访问，并且 /jobs/overview 可读取")))
		return 3
	}
	base, _ := flink.NormalizeBaseURL(rawURL)
	env := listJobsEnvelope{
		SchemaVersion:   "v1",
		Scenario:        "list-jobs",
		UIURL:           base.String(),
		ElapsedMs:       time.Since(start).Milliseconds(),
		SourceEndpoints: []string{"/jobs/overview"},
		Jobs:            jobs,
		NextActions: []string{
			"选择目标 jobs[].jid 后执行：flink-cli diagnose --job-id <jobId> <url>。",
			"如果只看到历史完成作业，确认当前 URL 指向正在服务该 application 的 Flink Web UI。",
		},
	}
	if err := output.WriteJSON(stdout, env); err != nil {
		apperr.WriteJSON(stderr, apperr.New("OUTPUT_ERROR", err.Error(), "检查 stdout 是否可写"))
		return 1
	}
	return 0
}

func newHTTPClient(timeout time.Duration, insecureTLS bool) *http.Client {
	if !insecureTLS {
		return &http.Client{Timeout: timeout}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// 内网 YARN/Flink 网关经常使用自签名或非标准证书；该开关只在用户显式传入时启用。
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
	return &http.Client{Timeout: timeout, Transport: transport}
}

func primaryFinding(report flink.Report) *flink.Finding {
	if len(report.Findings) == 0 {
		return nil
	}
	return &report.Findings[0]
}

func buildDiagnosis(report flink.Report) string {
	if len(report.Findings) == 0 {
		return "未发现可用诊断结果。"
	}
	primary := report.Findings[0]
	switch primary.Severity {
	case "critical":
		return "发现 critical 级别问题：" + primary.Title
	case "warn":
		return "发现 warn 级别问题：" + primary.Title
	default:
		return "未发现明显作业级异常：" + primary.Title
	}
}

func buildNextActions(report flink.Report) []string {
	for _, f := range report.Findings {
		switch f.RuleID {
		case "root_exception", "job_failed", "vertex_failed":
			return []string{
				"查看 finding.evidence.root_exception 或失败 vertex，并拉取对应 JobManager/TaskManager 日志确认最内层 cause。",
				"如果运行在 YARN application 模式，继续查看 YARN application diagnostics 和 AM/container stderr。",
				"需要完整 REST 原始数据时重新执行：flink-cli diagnose --include-snapshot <url>。",
			}
		case "checkpoint_failure_rate", "checkpoint_slow":
			return []string{
				"优先检查 checkpoint storage、state backend、下游 sink 提交耗时和反压。",
				"对照 Web UI Checkpoints 页面查看 alignment、sync/async duration 和 state size。",
				"需要完整 REST 原始数据时重新执行：flink-cli diagnose --include-snapshot <url>。",
			}
		case "backpressure_high":
			return []string{
				"沿 high backpressure vertex 往下游检查 sink、网络缓冲和外部系统写入延迟。",
				"对照 Web UI Back Pressure 和 Metrics 页面确认 busy/idle/backpressured 比例。",
				"需要完整 REST 原始数据时重新执行：flink-cli diagnose --include-snapshot <url>。",
			}
		case "sink_busy_upstream_backpressure":
			return []string{
				"优先查看 finding.evidence.interpretation，确认 primary_bottleneck、checkpoint_likely_bottleneck 和 next_focus。",
				"再查看 finding.evidence.doris_sink_metrics.summary，确认单批 rows/bytes、loadTimeMs、writeDataTimeMs 和 write_data_share_of_load。",
				"如果 writeDataTimeMs 接近 loadTimeMs，优先排查 Doris BE 写入吞吐、tablet 热点、compaction backlog 和 sink 批次/并发。",
			}
		}
	}
	return []string{
		"当前 REST 数据未发现明显作业级异常；继续结合业务吞吐、延迟、TaskManager 日志和外部系统指标判断。",
		"需要完整 REST 原始数据时重新执行：flink-cli diagnose --include-snapshot <url>。",
	}
}

func normalizeErr(err error) error {
	if err == nil {
		return nil
	}
	return apperr.New("FLAG_INVALID", err.Error(), "see `flink-cli --help`")
}

func flinkAPIHint(prefix string) string {
	return prefix + "；如果错误包含 x509 或 certificate，可重试加 --insecure-skip-verify；如果返回 HTML，检查 YARN application/proxy 是否过期、跳到 attempts 页面或被登录页拦截"
}
