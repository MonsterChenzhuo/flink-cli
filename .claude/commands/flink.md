---
description: Diagnose a Flink 1.18 job from a Flink Web UI URL using flink-cli
argument-hint: "<flink-web-ui-url> [jobId]"
---

请使用 `flink-cli` 诊断 Flink 作业，并用中文总结结论。

参数：

- 第一个参数是 Flink Web UI URL，也可以是 `host:port`。
- 如果第二个参数存在，把它当作 Flink job id，执行时加 `--job-id`。
- 如果第一个参数已经是 `#/job/running/<jobId>/overview` 这类完整作业页 URL，可以直接传给 `flink-cli diagnose`，CLI 会自动提取 job id。
- 如果第一个参数是 `#/task-manager/<taskManagerId>/thread-dump`，运行 `flink-cli thread-dump "$1"`，CLI 会自动提取 TaskManager id。

执行规则：

1. 如果没有提供 URL，先向用户索要 Flink Web UI URL。
2. 如果用户没有提供 job id，先运行：

   ```bash
   flink-cli diagnose "$1"
   ```

3. 如果用户提供了 job id，运行：

   ```bash
   flink-cli diagnose --job-id "$2" "$1"
   ```

4. 如果 stderr 里出现 `x509`、`certificate` 或内网网关证书错误，用相同参数加 `--insecure-skip-verify` 重试一次。
5. 如果 stderr 里出现 `returned non-JSON response` 或 `got HTML`，说明当前 URL 没有返回 Flink REST JSON，优先检查 YARN application/proxy 是否过期、是否跳到登录页/错误页、或 proxy path 是否丢失。
6. 优先阅读输出里的 `diagnosis`、`primary_finding`、`summary`、`findings`、`next_actions`、`warnings`。
7. 如果出现 `sink_busy_upstream_backpressure`，不要因为 sink 自身 backpressure=ok 就判断无问题；它通常表示 sink/外部系统写入吞吐导致上游反压。Doris 场景优先读取 `evidence.doris_sink_metrics.summary` 里的 `per_flush_*`、`load_time_*`、`write_data_time_*`、`load_mib_per_sec_per_subtask`，再分析批次大小、checkpoint 周期和 sink 并发。
8. thread dump 输出优先阅读 `summary.states`、`summary.reasons` 和 `summary.interesting_threads`；默认不要加 `--include-threads`。
9. 默认不要加 `--include-snapshot`，除非需要完整 REST 原始数据。
10. 如果需要先列出可用 job，运行：

   ```bash
   flink-cli diagnose --list-jobs "$1"
   ```

输出给用户时：

- 先给一句直接结论。
- 再列出 2-4 条关键证据。
- 最后给下一步排查动作。
- 不要把完整 JSON 原样贴给用户，除非用户明确要求。
