# flink-cli 给 Codex 的说明

## 响应要求

面向用户解释时使用中文。

## 项目定位

`flink-cli` 是给 Claude Code / Codex / 运维人员使用的 Flink 1.18 诊断 CLI。用户提供 Flink Web UI URL，工具通过 Flink REST API 获取作业状态、异常、checkpoint、反压和配置摘要。

不要把本项目实现成 Spark EventLog 解析器。默认只通过 REST API 诊断。

## 常用命令

```bash
flink-cli diagnose <flink-web-ui-url>
```

默认输出是紧凑 JSON，不带完整 REST 快照，避免撑爆 AI 上下文。需要原始数据时再运行：

```bash
flink-cli diagnose --include-snapshot <flink-web-ui-url>
```

`host:port` 会自动按 `http://host:port` 处理；gateway/proxy path 会被保留。

Claude Code 的 `/flink` 来自 `.claude/commands/flink.md`，安装脚本默认安装到 `~/.claude/commands/flink.md`。如果只装了 skill，Claude Code 不会自动出现 `/flink` slash command。

多作业或大作业时优先缩小范围：

```bash
flink-cli diagnose --job-id <jobId> <flink-web-ui-url>
flink-cli diagnose --max-vertices 50 <flink-web-ui-url>
```

## 输出阅读顺序

优先看：

1. `summary.critical` / `summary.warn`
2. `findings[]`
3. `primary_finding`
4. `diagnosis`
5. `next_actions[]`
6. `warnings[]`
7. `source_endpoints[]`

不要因为 `flink-cli diagnose` 退出码是 `0` 就认为作业健康；作业健康状态看 `summary` 和 `findings`。

## 开发检查

改代码后至少运行：

```bash
go test ./...
go build ./...
bash -n scripts/install.sh
```

涉及 release 配置时还要运行：

```bash
go run github.com/goreleaser/goreleaser/v2@latest check
```

## 约束

- 新行为先写测试，再实现。
- 默认输出要保持 agent 友好，避免无界日志、完整堆栈或大快照。
- 完整数据通过显式 flag 暴露，不要默认塞进 stdout。
- 不能仅凭 Flink REST API 断言 YARN 队列、资源调度根因；这类问题需要 YARN diagnostics/container log。
