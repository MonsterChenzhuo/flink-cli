# flink-cli 维护说明

## 项目目标

`flink-cli` 用于诊断 Flink 1.18 YARN application 模式作业。用户输入 Flink Web UI URL，CLI 通过 Flink Web UI 背后的 REST API 获取运行细节，并输出适合 Claude Code / Codex 继续分析的紧凑 JSON。

不要把它做成 Spark EventLog 解析器。Flink 初版只依赖 REST API，不读取 HDFS eventlog，也不直接抓 YARN container log。

## 当前入口

```bash
flink-cli diagnose <flink-web-ui-url>
flink-cli diagnose --include-snapshot <flink-web-ui-url>
flink-cli diagnose --job-id <jobId> <flink-web-ui-url>
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
- `GET /jobs/:jobid/checkpoints`：checkpoint 统计。
- `GET /jobs/:jobid/vertices/:vertexid/backpressure`：vertex 反压信息。

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
- `no_obvious_issue`：没有命中明显异常时输出 ok finding。

后续扩展规则时，先加测试，再实现。避免只凭字段存在就输出高置信度结论；`severity` 表示诊断置信度，不等于优化 ROI。

## 开发约定

- 语言：Go 1.22。
- CLI 框架：Cobra。
- 发布：push 到 `main` 后，`.github/workflows/release.yml` 会自动递增 patch tag，并用 GoReleaser 生成 GitHub Release 二进制包。
- 安装：`scripts/install.sh` 从 latest release 下载当前 OS/arch 的 tar.gz，校验 checksum 后安装 `flink-cli`。
- Skill：`.claude/skills/flink/SKILL.md` 会随 release 包分发，安装脚本默认同步到 `~/.claude/skills/flink`。如果用户只要二进制，可设置 `NO_SKILL=1`。
- Codex：`AGENTS.md` 记录 Codex 侧的中文使用和开发约束，保持与本文件同步。
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

- 暂不支持认证、Kerberos、TLS 客户端证书。
- 暂不主动读取 YARN ResourceManager 或 NodeManager 日志。
- `/jobs/:jobid/vertices/:vertexid/backpressure` 可能触发采样，详情端点失败只记录 warning。
- gateway 代理如果改写 path 或要求额外 header，需要用户在外层代理侧处理。
