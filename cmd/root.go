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
	Scenario     string          `json:"scenario"`
	UIURL        string          `json:"ui_url"`
	FlinkVersion string          `json:"flink_version,omitempty"`
	ElapsedMs    int64           `json:"elapsed_ms"`
	Summary      flink.Summary   `json:"summary"`
	Findings     []flink.Finding `json:"findings"`
	Snapshot     flink.Snapshot  `json:"snapshot"`
}

type state struct {
	timeout time.Duration
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
	return &cobra.Command{
		Use:   "diagnose <flink-web-ui-url>",
		Short: "Collect Flink REST API details and emit diagnosis JSON.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			exitCode = runDiagnose(cmd.Context(), args[0], st.timeout, cmd.OutOrStdout(), cmd.ErrOrStderr())
			return nil
		},
	}
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

func runDiagnose(ctx context.Context, rawURL string, timeout time.Duration, stdout, stderr io.Writer) int {
	client, err := flink.NewClientWithHTTP(rawURL, &http.Client{Timeout: timeout})
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
		Scenario:     "diagnose",
		UIURL:        snapshot.UIURL,
		FlinkVersion: snapshot.FlinkVersion,
		ElapsedMs:    time.Since(start).Milliseconds(),
		Summary:      report.Summary,
		Findings:     report.Findings,
		Snapshot:     report.Snapshot,
	}
	if err := output.WriteJSON(stdout, env); err != nil {
		apperr.WriteJSON(stderr, apperr.New("OUTPUT_ERROR", err.Error(), "检查 stdout 是否可写"))
		return 1
	}
	return 0
}

func normalizeErr(err error) error {
	if err == nil {
		return nil
	}
	return apperr.New("FLAG_INVALID", err.Error(), "see `flink-cli --help`")
}
