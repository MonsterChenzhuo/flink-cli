package cmd

import (
	"context"
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
	Summary         flink.Summary   `json:"summary"`
	Findings        []flink.Finding `json:"findings"`
	NextActions     []string        `json:"next_actions"`
	Snapshot        *flink.Snapshot `json:"snapshot,omitempty"`
}

type state struct {
	timeout         time.Duration
	includeSnapshot bool
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
	client, err := flink.NewClientWithHTTP(rawURL, &http.Client{Timeout: st.timeout})
	if err != nil {
		apperr.WriteJSON(stderr, apperr.New("URL_INVALID", err.Error(), "传入完整的 Flink Web UI URL，例如 http://jobmanager:8081 或带 gateway path 的代理地址"))
		return 2
	}
	start := time.Now()
	snapshot, err := client.Collect(ctx)
	if err != nil {
		apperr.WriteJSON(stderr, apperr.New("FLINK_API_UNREACHABLE", err.Error(), "确认 URL 可访问，并且指向 Flink 1.18 Web UI 根路径而不是具体页面路由"))
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
		Warnings:        snapshot.Warnings,
		Summary:         report.Summary,
		Findings:        report.Findings,
		NextActions:     buildNextActions(report),
	}
	if st.includeSnapshot {
		env.Snapshot = &report.Snapshot
	}
	if err := output.WriteJSON(stdout, env); err != nil {
		apperr.WriteJSON(stderr, apperr.New("OUTPUT_ERROR", err.Error(), "检查 stdout 是否可写"))
		return 1
	}
	return 0
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
