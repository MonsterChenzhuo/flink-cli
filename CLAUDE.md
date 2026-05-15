# flink-cli 维护说明

## 项目目标

`flink-cli` 用于诊断 Flink 1.18 YARN application 模式作业。用户输入 Flink Web UI URL，CLI 通过 Flink Web UI 背后的 REST API 获取运行细节，并输出适合 Claude Code / Codex 继续分析的紧凑 JSON。

不要把它做成 Spark EventLog 解析器。Flink 初版只依赖 REST API，不读取 HDFS eventlog，也不直接抓 YARN container log。

## 当前入口

```bash
flink-cli diagnose <flink-web-ui-url>
flink-cli diagnose --include-snapshot <flink-web-ui-url>
flink-cli diagnose --job-id <jobId> <flink-web-ui-url>
flink-cli diagnose --insecure-skip-verify <flink-web-ui-url>
flink-cli thread-dump <flink-web-ui-url>
flink-cli thread-dump --taskmanager-id <taskManagerId> <flink-web-ui-url>
flink-cli thread-dump --include-threads <flink-web-ui-url>
flink-cli flamegraph <flink-web-ui-url>
flink-cli flamegraph --job-id <jobId> --vertex-id <vertexId> --type ON_CPU <flink-web-ui-url>
flink-cli flamegraph --job-id <jobId> --vertex-id <vertexId> --type OFF_CPU --subtask-index <n> <flink-web-ui-url>
```

示例：

```bash
flink-cli diagnose http://jobmanager-host:8081
flink-cli diagnose http://gateway.example.com/proxy/application_1730000000000_0001/
```

URL 规范化规则：

- 必须包含 scheme 和 host。
- 也接受 `host:port`，会自动按 `http://host:port` 处理。
- 去掉 query、fragment 和末尾 `/`。
- 保留 gateway/proxy path，再拼接 REST path。
- 例如 `http://gateway/proxy/application_1/` 会请求 `http://gateway/proxy/application_1/jobs/overview`。
- 如果用户传入 Web UI 作业页完整 URL，例如 `#/job/running/<jobId>/overview`，CLI 会自动从 fragment 里提取 job id，相当于补上 `--job-id <jobId>`。
- 如果用户传入 TaskManager thread dump 页面完整 URL，例如 `#/task-manager/<taskManagerId>/thread-dump`，`thread-dump` 命令会自动从 fragment 里提取 TaskManager id。
- 如果用户传入 vertex flame graph 页面完整 URL，例如 `#/job/running/<jobId>/vertices/<vertexId>/flamegraph`，`flamegraph` 命令会自动从 fragment 里提取 job id 和 vertex id。
- 内网 HTTPS YARN/Flink 网关如果使用自签名或非标准证书，使用 `--insecure-skip-verify`。这是显式开关，不默认跳过证书校验。
- 真实回归 URL 形态示例：`https://110.238.78.142:9022/component/Yarn/ResourceManager/36/proxy/application_1777980975440_135435/`，需要保留 `/component/Yarn/ResourceManager/.../proxy/application_...` 这段 path。

## REST API 依据

以 Apache Flink 1.18 官方文档为准：

https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/rest_api/

当前采集端点：

- `GET /config`：Flink 版本、revision、Web UI 配置。
- `GET /jobmanager/config`：集群配置。
- `GET /jobs/overview`：作业列表。
- `GET /jobs/:jobid`：作业详情、vertex 列表。
- `GET /jobs/:jobid/jobmanager/config`：作业对应 JobManager 配置。
- `GET /jobs/:jobid/exceptions`：root exception 和异常列表。
- `GET /jobs/:jobid/checkpoints`：checkpoint 计数、最近 checkpoint 和 summary 分位/均值统计。注意 Flink 可能把 summary 数值返回成 `88272.0` 这类浮点 JSON，类型要兼容整数和浮点。
- `GET /jobs/:jobid/vertices/:vertexid/backpressure`：vertex 反压信息。
- `GET /jobs/:jobid/vertices/:vertexid/flamegraph`：vertex 火焰图信息，仅在执行 `flink-cli flamegraph` 时读取；支持 `type=FULL|ON_CPU|OFF_CPU` 和 `subtaskindex`。
- `GET /jobs/:jobid/vertices/:vertexid/metrics`：对 Doris Writer vertex 做受限采样，提取 Stream Load flush/load/writeData 指标。
- `GET /taskmanagers`：列出 TaskManager。
- `GET /taskmanagers/:taskmanagerid/thread-dump`：采集指定 TaskManager 线程栈，默认只输出状态统计和可疑线程摘要。

除 `/jobs/overview` 外，单个详情端点失败时不要让整个诊断失败；把错误放入 `snapshot.warnings`，继续输出已采集到的数据。

## 输出契约

stdout 输出 JSON envelope：

```json
{
  "scenario": "diagnose",
  "ui_url": "...",
  "flink_version": "1.18.1",
  "elapsed_ms": 123,
  "source_endpoints": ["/jobs/overview"],
  "warnings": [],
  "summary": {
    "critical": 0,
    "warn": 1,
    "ok": 0,
    "total_jobs": 1,
    "jobs_by_state": {
      "RUNNING": 1
    }
  },
  "findings": [],
  "primary_finding": {},
  "diagnosis": "未发现明显作业级异常：未发现明显作业级异常",
  "next_actions": []
}
```

默认不输出完整 `snapshot`，避免 AI 上下文过大；只有用户或 agent 明确需要原始 REST 数据时才使用 `--include-snapshot`。

大作业默认每个 job 最多对 20 个 vertex 请求 backpressure，避免 REST 调用过多和输出过大；需要全量时使用 `--max-vertices 0`。

stderr 输出 JSON error：

```json
{"error":{"code":"URL_INVALID","message":"...","hint":"..."}}
```

退出码：

- `0`：成功。
- `1`：内部错误或输出错误。
- `2`：用户输入错误，例如 URL 不合法。
- `3`：Flink REST API 不可达或 `/jobs/overview` 拉取失败。
- 如果错误提示 `returned non-JSON response` 或 `got HTML`，通常是 YARN application/proxy 已过期、URL 指错到登录页/错误页，或 gateway 没有把 REST path 代理到 Flink Web UI。不要继续按 Flink 指标诊断，要先让用户换当前 application 的 Web UI URL。

## 诊断规则

当前规则集中在作业级别：

- `job_failed`：作业处于 `FAILED` 或 `FAILING`。
- `job_canceled`：作业处于 `CANCELED` 或 `CANCELLING`。
- `job_restarting`：作业处于 `RESTARTING`。
- `task_state_abnormal`：作业 task 汇总里存在 failed/canceling。
- `root_exception`：REST API 返回 root exception。
- `checkpoint_failure_rate`：checkpoint 失败比例偏高。
- `checkpoint_slow`：最近成功 checkpoint end-to-end duration 超过 60 秒。
- `vertex_failed`：vertex 处于 failed/canceled/canceling。
- `backpressure_high`：vertex backpressure level 为 high。
- `sink_busy_upstream_backpressure`：sink 自身 backpressure 可能是 ok，但 sink busy 较高且上游 vertex 已累计反压。这个规则用于 Doris Writer 这类场景，避免 agent 被 “Writer backpressure=ok” 误导；应继续检查外部系统吞吐、sink flush/load/commit 指标、批次大小、checkpoint 周期和 sink 并发。
  - 如果命中 Doris Writer，`finding.evidence.doris_sink_metrics.summary` 会包含采样 subtask 的 `per_flush_rows_mean`、`per_flush_bytes_mean`、`per_flush_mib_mean`、`per_flush_gib_mean`、`load_time_ms_mean/max`、`load_time_sec_mean/max`、`write_data_time_ms_mean/max`、`write_data_time_sec_mean/max`、`write_data_share_of_load`、`load_mib_per_sec_per_subtask`、`begin_txn_time_ms_mean`、`commit_and_publish_time_ms_mean`、`commit_and_publish_time_sec_mean`。这些均值类字段按 3 位小数输出，避免大作业诊断时出现难读的长小数。
  - 同一个 finding 会尽量带 `finding.evidence.checkpoint_summary`，包含 checkpoint counts、最近成功耗时、历史 avg/max duration、state size 和 alignment buffered。用它先排除 checkpoint 对齐/状态过大问题，不要再手写脚本单独拉 `/checkpoints`。
  - 同一个 finding 会尽量带 `finding.evidence.interpretation`，直接给出 `primary_bottleneck`、`checkpoint_likely_bottleneck`、`doris_commit_publish_likely_bottleneck`、`next_focus` 等面向 agent 的判断提示。Agent 应先复述这些字段，再结合原始指标解释原因。
- `no_obvious_issue`：没有命中明显异常时输出 ok finding。

后续扩展规则时，先加测试，再实现。避免只凭字段存在就输出高置信度结论；`severity` 表示诊断置信度，不等于优化 ROI。

`thread-dump` 输出规则：

- 默认输出 `summary.total_threads`、`summary.states`、`summary.reasons`、`summary.interpretation` 和 `summary.interesting_threads`，避免完整栈撑爆 AI 上下文。
- `summary.interesting_threads` 会优先标注 Doris、Stream Load、checkpoint、HTTP/socket write、BLOCKED 等可疑线程。
- 如果 `summary.interesting_count=0`，`summary.interpretation` 会说明本次快照未发现可疑线程；这不等于作业没有问题，只说明该 TaskManager 的瞬时线程栈没有命中特征，需要结合 metrics 或多 TM/多次采样。
- 用户明确需要完整线程栈时才使用 `--include-threads`。

`flamegraph` 输出规则：

- 如果只有 job URL 或 job id，先输出 `scenario=flamegraph-list-vertices` 和 `vertices[]`，让 agent 选择具体 vertex 后再采样。
- 默认输出 `summary.total_samples`、`summary.top_frames`、`summary.top_leaf_paths` 和 `summary.interpretation`，避免完整火焰图树撑爆 AI 上下文。
- `--type ON_CPU` 用于 CPU 热点，`--type OFF_CPU` 用于阻塞/IO/锁等待，`--type FULL` 用于粗看整体。
- 用户明确需要原始树时才使用 `--include-raw`。

## 开发约定

- 语言：Go 1.22。
- CLI 框架：Cobra。
- 发布：push 到 `main` 后，`.github/workflows/release.yml` 会自动递增 patch tag，并用 GoReleaser 生成 GitHub Release 二进制包。
- 安装：`scripts/install.sh` 从 latest release 下载当前 OS/arch 的 tar.gz，校验 checksum 后安装 `flink-cli`。默认安装到 `~/.local/bin/flink-cli`；如果 `PATH` 当前优先命中 `/usr/local/bin/flink-cli` 等旧版本，脚本必须输出 warning，提示用户运行完整路径、调整 `PATH` 或用 `PREFIX=/usr/local/bin` 覆盖安装。
- Skill：`.claude/skills/flink/SKILL.md` 会随 release 包分发，安装脚本默认同步到 `~/.claude/skills/flink`。如果用户只要二进制，可设置 `NO_SKILL=1`。
- Slash command：`.claude/commands/flink.md` 会随 release 包分发，安装脚本默认同步到 `~/.claude/commands/flink.md`，对应 Claude Code 里的 `/flink`。如果不需要，可设置 `NO_COMMAND=1`。
- Codex：`AGENTS.md` 记录 Codex 侧的中文使用和开发约束；安装脚本默认同步 skill 到 `~/.agents/skills/flink` 和 `~/.codex/skills/flink`。Codex 不读取 Claude Code 的 `/flink` slash command，通常需要新开会话才会加载新 skill。
- Agent 使用体验要求：
  - 首次诊断仍从 `flink-cli diagnose <url>` 开始。
  - 遇到证书错误要自动加 `--insecure-skip-verify` 重试一次，不要退回手写 `curl -k`。
  - 遇到 `returned non-JSON response`/`got HTML` 时，说明当前 URL 没拿到 Flink REST JSON，优先检查 YARN proxy/application 是否过期或被登录页拦截。
  - 看到 `sink_busy_upstream_backpressure` 时，先说明“sink 写入繁忙导致上游反压”，再给证据；不要只说 REST 没发现 high backpressure。
  - Doris Writer 场景要优先引用 `finding.evidence.doris_sink_metrics.summary`，不要再手写 Python/curl 去拼 metrics，除非需要更深的全量 subtask 分析。
  - Doris Writer 场景也要引用 `finding.evidence.checkpoint_summary` 判断 checkpoint 是否稳定；如果 `completed` 持续增长、`failed=0`、`alignment_buffered_bytes=0` 且 duration 稳定，则不要把消费不动优先归因到 checkpoint。
  - Doris Writer 场景还要看 `finding.evidence.interpretation`：如果 `primary_bottleneck=doris_stream_load_write_data` 且 `write_data_share_of_load` 接近 1，说明主要耗在 Stream Load 写数据/等待 Doris 消化，不要把几十毫秒的 commit/publish 当主因。
  - Doris 写入场景优先提醒检查 Stream Load `writeDataTimeMs/loadTimeMs`、`sink.enable.batch-mode`、`sink.buffer-flush.*`、checkpoint 周期、sink 并发、BE load/compaction/tablet 热点。
  - 需要线程栈时使用 `flink-cli thread-dump <url>`；如果 URL 是 `#/task-manager/<id>/thread-dump` 页面，直接传完整 URL。默认不要加 `--include-threads`，先看摘要。
  - 需要火焰图时使用 `flink-cli flamegraph <url>`；如果 URL 是 job overview 页面，先看 `vertices[]` 再带 `--vertex-id` 下钻。默认不要加 `--include-raw`，先看摘要。
- 主要包：
  - `cmd`：命令入口、退出码、JSON envelope。
  - `internal/flink`：URL 规范化、REST client、数据模型和诊断规则。
  - `internal/apperr`：stderr JSON error。
  - `internal/output`：stdout JSON writer。
- 每次改行为先写测试，并至少运行：

```bash
go test ./...
go build ./...
bash -n scripts/install.sh
```

CI 参考 `spark-cli`：`.github/workflows/ci.yml` 负责 `go mod tidy`、`go vet`、`gofmt`、race test、build 和 smoke；`.github/workflows/release.yml` 负责自动打 tag 和 GoReleaser 发布。

## 已知边界

- 暂不支持认证、Kerberos、TLS 客户端证书；普通服务端证书校验问题可用 `--insecure-skip-verify` 绕过。
- 暂不主动读取 YARN ResourceManager 或 NodeManager 日志。
- `/jobs/:jobid/vertices/:vertexid/backpressure` 可能触发采样，详情端点失败只记录 warning。
- gateway 代理如果改写 path 或要求额外 header，需要用户在外层代理侧处理。
- Doris connector 的自定义 Stream Load metrics 只做受限采样，不做全量 subtask 采集，避免大作业输出爆炸；需要更深分析时再按具体 subtask 下钻。
