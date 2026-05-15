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
	timeout         time.Duration
	includeSnapshot bool
	jobID           string
	maxVertices     int
	listJobs        bool
	quietWarnings   bool
	insecureTLS     bool
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
		apperr.WriteJSON(stderr, apperr.New("FLINK_API_UNREACHABLE", err.Error(), "确认 URL 可访问，并且 /jobs/overview 可读取"))
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
				"优先查看 finding.evidence.doris_sink_metrics.summary，确认单批 rows/bytes、loadTimeMs 和 writeDataTimeMs。",
				"如果 writeDataTimeMs 接近 loadTimeMs，优先排查 Doris BE 写入吞吐、tablet 热点、compaction backlog 和 sink 批次/并发。",
				"需要完整 REST 原始数据时重新执行：flink-cli diagnose --include-snapshot <url>。",
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
