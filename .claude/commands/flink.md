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
- 如果用户明确要看火焰图，运行 `flink-cli flamegraph "$1"`；如果 URL 是 `#/job/running/<jobId>/vertices/<vertexId>/flamegraph`，CLI 会自动提取 job id 和 vertex id。

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

4. stderr error 带结构化 `code` 和 `details`，据 `error.code` 分支：`TLS_CERT_ERROR` 用相同参数加 `--insecure-skip-verify` 重试一次；`JOB_NOT_FOUND` 从 `details.available_job_ids` 里挑一个合法 id。
5. 如果 `error.code` 是 `NON_JSON_HTML_RESPONSE`（含 `details.likely_cause=yarn_proxy_expired_or_login_page`），说明当前 URL 没有返回 Flink REST JSON，优先检查 YARN application/proxy 是否过期、是否跳到登录页/错误页、或 proxy path 是否丢失，让用户换当前 application 的 Web UI URL。
6. 优先阅读输出里的 `diagnosis`、`primary_finding`、`summary`（带 `kind` 判别字段）、`findings`、`next_actions`、`warnings`。
7. 如果出现 `backpressure_chain`，CLI 已沿链路定位到瓶颈终止 vertex，直接读 `evidence.bottleneck_vertex_name`，并对它做 `flamegraph --type ON_CPU`，不要逐个 vertex 手工查 backpressure。
8. 如果出现 `sink_busy_upstream_backpressure`，不要因为 sink 自身 backpressure=ok 就判断无问题；它通常表示 sink/外部系统写入吞吐导致上游反压。Doris 场景优先读取 `evidence.doris_sink_metrics.summary` 里的 `per_flush_*`、`load_time_*`、`write_data_time_*`、`load_mib_per_sec_per_subtask`，再分析批次大小、checkpoint 周期和 sink 并发。
9. thread dump 输出优先阅读 `summary.states`、`summary.reasons` 和 `summary.interesting_threads`；默认不要加 `--include-threads`。
10. flamegraph 输出优先阅读 `summary.top_self_frames`：它按方法名聚合自身耗时（self-time），`top_self_frames[0]` 就是真正的热点方法（CPU 或阻塞）。不要用 `summary.top_frames` 排序，那是累计耗时，最外层栈帧（`Thread.run`、`Task.doRun`）share 接近 1 但没有定位价值。再结合 `summary.top_leaf_paths` 和 `summary.interpretation`；火焰图惰性采样，`total_samples=0` 是正常首次结果、CLI 会自动 `--wait` 重试，按 interpretation 重试即可；默认不要加 `--include-raw`。
11. 默认不要加 `--include-snapshot`，除非需要完整 REST 原始数据。
12. 如果需要先列出可用 job，运行：

   ```bash
   flink-cli diagnose --list-jobs "$1"
   ```

输出给用户时：

- 先给一句直接结论。
- 再列出 2-4 条关键证据。
- 最后给下一步排查动作。
- 不要把完整 JSON 原样贴给用户，除非用户明确要求。
