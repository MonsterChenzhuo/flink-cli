# flink-cli

`flink-cli` 是一个面向 Claude Code、Codex 和运维人员的 Flink 诊断 CLI。用户只需要传入 Flink Web UI URL，工具会通过 Flink 1.18 REST API 拉取作业、异常、checkpoint、反压和配置摘要，并输出紧凑 JSON。

## 快速开始

```bash
flink-cli diagnose http://jobmanager-host:8081
```

## 一键安装 / 升级

每次代码 push 到 `main` 后，GitHub Actions 会自动递增 patch tag，并通过 GoReleaser 生成 `darwin/linux`、`amd64/arm64` 的二进制包。

安装或升级到 latest release：

```bash
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/flink-cli/main/scripts/install.sh | bash
```

常用覆盖：

```bash
# 锁定版本
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/flink-cli/main/scripts/install.sh | VERSION=v0.1.0 bash

# 安装到无需 sudo 的路径，并跳过 Claude/Codex skill 和 /flink slash command
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/flink-cli/main/scripts/install.sh | PREFIX="$HOME/.local/bin" NO_SUDO=1 NO_SKILL=1 NO_CODEX_SKILL=1 NO_COMMAND=1 bash
```

安装脚本默认还会安装：

- Claude Code skill：`~/.claude/skills/flink/SKILL.md`
- Claude Code slash command：`~/.claude/commands/flink.md`，对应 `/flink`
- Codex skill：`~/.agents/skills/flink/SKILL.md`
- Codex-local skill copy：`~/.codex/skills/flink/SKILL.md`

注意：`/flink` 是 Claude Code slash command。Codex 当前通过 skill / AGENTS 发现能力，不读取 `~/.claude/commands`；安装后需要新开 Codex 会话才能看到新 skill。

安装脚本默认把二进制安装到 `~/.local/bin/flink-cli`。如果机器上已有 `/usr/local/bin/flink-cli` 等旧版本，并且它在 `PATH` 里更靠前，直接执行 `flink-cli -v` 仍会看到旧版本。安装脚本会检测这种冲突并给出 warning；可用下面任一方式处理：

```bash
# 临时验证刚安装的版本
~/.local/bin/flink-cli -v

# 让当前 shell 优先使用 ~/.local/bin
export PATH="$HOME/.local/bin:$PATH"
hash -r

# 或直接覆盖安装到当前优先路径
curl -fsSL https://raw.githubusercontent.com/MonsterChenzhuo/flink-cli/main/scripts/install.sh | PREFIX="/usr/local/bin" bash
```

YARN application 模式下，如果 Web UI 经过 gateway/proxy 暴露，也可以直接传带 path 的地址：

```bash
flink-cli diagnose http://gateway.example.com/proxy/application_1730000000000_0001/
```

CLI 会保留 URL path，并拼接 Flink REST API 路径，例如：

```text
http://gateway.example.com/proxy/application_xxx/jobs/overview
```

## 诊断数据

当前 `diagnose` 会读取：

- `/config`：Flink 版本和 Web UI 配置。
- `/jobmanager/config`：JobManager 集群配置。
- `/jobs/overview`：作业列表和任务状态汇总。
- `/jobs/:jobid`：作业详情和 vertex 列表。
- `/jobs/:jobid/jobmanager/config`：作业对应 JobManager 配置。
- `/jobs/:jobid/exceptions`：root exception 和异常列表。
- `/jobs/:jobid/checkpoints`：checkpoint 统计、最近 checkpoint、耗时/状态大小/alignment buffered 的 summary。
- `/jobs/:jobid/vertices/:vertexid/backpressure`：vertex 反压采样结果。
- `/jobs/:jobid/vertices/:vertexid/flamegraph`：vertex 火焰图采样结果，仅在执行 `flink-cli flamegraph` 时读取。

REST API 依据 Apache Flink 1.18 官方文档：
https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/rest_api/

## 输出示例

```json
{
  "scenario": "diagnose",
  "ui_url": "http://jobmanager-host:8081",
  "flink_version": "1.18.1",
  "elapsed_ms": 120,
  "source_endpoints": ["/jobs/overview", "/jobs/job-1"],
  "summary": {
    "critical": 1,
    "warn": 1,
    "ok": 0,
    "total_jobs": 1,
    "jobs_by_state": {
      "FAILED": 1
    }
  },
  "findings": [
    {
      "rule_id": "job_failed",
      "severity": "critical",
      "title": "Flink 作业处于失败状态",
      "suggestion": "先查看 root_exception 和失败 vertex；如果异常来自 checkpoint 或 sink，优先排查外部存储、网络和状态后端。"
    }
  ],
  "primary_finding": {
    "rule_id": "job_failed",
    "severity": "critical",
    "title": "Flink 作业处于失败状态"
  },
  "diagnosis": "发现 critical 级别问题：Flink 作业处于失败状态",
  "next_actions": [
    "查看 finding.evidence.root_exception 或失败 vertex，并拉取对应 JobManager/TaskManager 日志确认最内层 cause。"
  ]
}
```

默认输出不带完整 REST 快照，避免撑爆 AI 上下文。需要原始数据时：

```bash
flink-cli diagnose --include-snapshot http://jobmanager-host:8081
```

Doris Writer 反压场景下，`sink_busy_upstream_backpressure` 的 evidence 会优先给出两个紧凑摘要：

- `doris_sink_metrics.summary`：单批 rows/bytes、Stream Load `loadTimeMs/writeDataTimeMs`、提交耗时等。
- `checkpoint_summary`：checkpoint completed/failed、最近成功耗时、历史平均/最大耗时、state size、alignment buffered。
- `interpretation`：面向 AI 的判断提示，例如主要瓶颈是否为 `doris_stream_load_write_data`、checkpoint 是否像瓶颈、下一步应该优先看 Doris BE/tablet/compaction 还是 Flink 参数。

`thread-dump` 默认也会输出 `summary.interpretation`。当 `interesting_count=0` 时，它会明确说明本次线程快照没有命中特征，避免把空 `interesting_threads` 误读成采集失败。

多作业或大作业场景：

```bash
# 只诊断指定 job
flink-cli diagnose --job-id <jobId> http://jobmanager-host:8081

# 控制每个 job 采集 backpressure 的 vertex 数；0 表示不限制
flink-cli diagnose --max-vertices 50 http://jobmanager-host:8081
```

火焰图场景：

```bash
# 如果 URL 是 job overview 页面，先列出可选 vertex
flink-cli flamegraph https://gateway/proxy/application_xxx/#/job/running/<jobId>/overview

# 指定 vertex 后读取紧凑火焰图摘要
flink-cli flamegraph --job-id <jobId> --vertex-id <vertexId> --type ON_CPU http://jobmanager-host:8081

# 看阻塞/等待热点，或下钻单个 subtask
flink-cli flamegraph --job-id <jobId> --vertex-id <vertexId> --type OFF_CPU --subtask-index 3 http://jobmanager-host:8081

# 需要原始 flame graph 树时显式打开
flink-cli flamegraph --include-raw --job-id <jobId> --vertex-id <vertexId> http://jobmanager-host:8081
```

`flamegraph` 默认只输出 `summary.top_frames` / `summary.top_leaf_paths`，避免把完整树塞进 stdout。Flink 火焰图端点可能触发采样，所以 CLI 不会在 `diagnose` 默认流程里批量采集。

错误写到 stderr，格式也是 JSON：

```json
{"error":{"code":"URL_INVALID","message":"Flink Web UI URL must include scheme and host"}}
```

退出码：

- `0`：成功。
- `1`：内部错误或输出错误。
- `2`：用户输入错误。
- `3`：Flink REST API 不可达。

## 开发

```bash
go test ./...
go build ./...
bash -n scripts/install.sh
```

本地模拟 release 配置检查：

```bash
goreleaser check
```
