---
name: flink-runtime-diagnostics
description: Use when investigating Apache Flink 1.18 jobs through a Flink Web UI URL, especially YARN application mode jobs, checkpoint failures, root exceptions, failed vertices, backpressure, or flame graphs.
---

# Flink Runtime Diagnostics

Use `flink-cli` to query the Flink Web UI REST API and emit compact JSON for analysis. The required input is a Flink Web UI URL, for example `http://jobmanager:8081` or a YARN gateway/proxy URL such as `http://gateway/proxy/application_.../`.

## Workflow

1. Always start with:

   ```bash
   flink-cli diagnose <flink-web-ui-url>
   ```

   The command exits `0` even when findings are critical. Read `summary.critical`, `summary.warn`, and `findings[]`.
   If stderr contains `x509`, `certificate`, or an internal gateway certificate error, retry once with:

   ```bash
   flink-cli diagnose --insecure-skip-verify <flink-web-ui-url>
   ```

2. Use the top-level fields first:

   - `schema_version`: output contract version.
   - `source_endpoints`: REST endpoints successfully used.
   - `warnings`: non-fatal collection gaps.
   - `summary.jobs_by_state`: quick job state count.
   - `findings`: ordered by severity.
   - `primary_finding`: the top finding to mention first.
   - `diagnosis`: a one-line Chinese diagnosis for chat replies.
   - `next_actions`: recommended next diagnostic steps.

3. Drill down by finding:

   - `root_exception`, `job_failed`, `vertex_failed`: read `evidence.root_exception`, failed vertex evidence, then fetch JobManager/TaskManager logs or YARN diagnostics.
   - `checkpoint_failure_rate`, `checkpoint_slow`: check checkpoint storage, state backend, sink commit latency, alignment duration, and backpressure.
   - `backpressure_high`: follow the affected vertex downstream; inspect sink/external system latency, network buffers, and checkpoint alignment.
   - `backpressure_chain`: the CLI already walked the vertex chain and found where backpressure terminates. Read `evidence.bottleneck_vertex_id` / `bottleneck_vertex_name` — that terminus is the real bottleneck, not the upstream source. Read `evidence.upstream_chain` for the propagation path and `evidence.interpretation.next_focus`, then run `flink-cli flamegraph --job-id <jobId> --vertex-id <bottleneck> --type ON_CPU <url>`. Do not re-query every vertex's backpressure by hand.
   - `sink_busy_upstream_backpressure`: the sink may report `backpressure=ok` while upstream vertices are backpressured. Treat this as a sink/external-system throughput bottleneck. For Doris, first read `evidence.interpretation`, then `evidence.doris_sink_metrics.summary` for sampled `per_flush_*`, `load_time_*`, `write_data_time_*`, `write_data_share_of_load`, and per-subtask throughput; also read `evidence.checkpoint_summary` to rule in/out checkpoint duration, failures, state size, and alignment buffered before blaming checkpoint.
   - `task_state_abnormal`: map failed tasks to their vertex and TaskManager logs.
   - `no_obvious_issue`: REST data does not show obvious job-level failure; continue with business throughput/latency, TaskManager logs, and external system metrics.

4. Only request full raw data when needed:

   ```bash
   flink-cli diagnose --include-snapshot <flink-web-ui-url>
   ```

   Default output intentionally omits the raw snapshot to keep Claude Code/Codex context small.

5. In multi-job or large-job sessions, narrow the scope:

   ```bash
   flink-cli diagnose --job-id <jobId> <flink-web-ui-url>
   flink-cli diagnose --max-vertices 50 <flink-web-ui-url>
   flink-cli diagnose --insecure-skip-verify --job-id <jobId> <flink-web-ui-url>
   ```

   `--max-vertices 0` disables the per-job backpressure collection limit.

6. To inspect a TaskManager thread dump, run:

   ```bash
   flink-cli thread-dump <flink-web-ui-url>
   flink-cli thread-dump --taskmanager-id <taskManagerId> <flink-web-ui-url>
   ```

   If the URL is a full thread-dump page such as `#/task-manager/<taskManagerId>/thread-dump`, `flink-cli` automatically infers the TaskManager id. Read `summary.interpretation`, `summary.states`, `summary.reasons`, and `summary.interesting_threads` first. Do not add `--include-threads` unless full stacks are needed.

7. To inspect a vertex flame graph, run:

   ```bash
   flink-cli flamegraph <flink-web-ui-url>
   flink-cli flamegraph --job-id <jobId> --vertex-id <vertexId> --type ON_CPU <flink-web-ui-url>
   flink-cli flamegraph --job-id <jobId> --vertex-id <vertexId> --type OFF_CPU --subtask-index <n> <flink-web-ui-url>
   ```

   If the URL is a job overview page, `flink-cli flamegraph` first lists vertices and tells you which `--vertex-id` to use. If the URL is a full flame graph page such as `#/job/running/<jobId>/vertices/<vertexId>/flamegraph`, the CLI infers both ids. Read `summary.top_self_frames` first: it aggregates self-time by method name, so `top_self_frames[0]` is the real hotspot (CPU or blocking). Do NOT rank by `summary.top_frames` — those are cumulative values, so the outermost stack frames (`Thread.run`, `Task.doRun`, …) show `share`≈1 but carry no localization value. Also read `summary.top_leaf_paths` and `summary.interpretation`. Do not add `--include-raw` unless the raw flame graph tree is needed. Flink flame-graph sampling is lazy (the first request returns `total_samples=0` and only triggers a round); the CLI now auto-waits and retries (`--wait`, default 8s), so a single invocation usually returns populated data — if you still see `total_samples=0`, `interpretation`/`next_actions` say to just retry, it is not an error.

## Input handling

`flink-cli` accepts both full URLs and `host:port`; `host:port` defaults to `http://host:port`. Gateway path prefixes are preserved when constructing REST paths.
If the URL is a full Flink job page such as `#/job/running/<jobId>/overview`, `flink-cli` automatically infers that job id when `--job-id` is omitted.
If the URL is a full TaskManager thread dump page such as `#/task-manager/<taskManagerId>/thread-dump`, `flink-cli thread-dump` automatically infers that TaskManager id.
If the URL is a full vertex flame graph page such as `#/job/running/<jobId>/vertices/<vertexId>/flamegraph`, `flink-cli flamegraph` automatically infers both ids.

## Errors

Errors go to stderr as structured JSON with `schema_version` and optional `details`:

```json
{"error":{"schema_version":"v1","code":"...","message":"...","hint":"...","details":{}}}
```

Branch on `error.code` (do not grep the message):

- `URL_INVALID` (exit 2): malformed URL.
- `JOB_NOT_FOUND` (exit 2): `details.available_job_ids` lists valid ids — pick one, don't re-run to discover them.
- `TLS_CERT_ERROR` (exit 3): retry with `--insecure-skip-verify` (see `details.retriable_flags`).
- `NON_JSON_HTML_RESPONSE` (exit 3): got HTML, not JSON — `details.likely_cause=yarn_proxy_expired_or_login_page`. Ask the user for the current application's Web UI URL; do not keep diagnosing. This also fires on non-200 responses whose body is HTML.
- `NON_JSON_RESPONSE` (exit 3): body was not valid JSON.
- `HTTP_STATUS_ERROR` (exit 3): non-2xx with a non-HTML body; `details.http_status` has the code.
- `FLINK_API_TIMEOUT` (exit 3): request timed out; raise `--timeout`.
- `FLINK_API_UNREACHABLE` (exit 3): other connection failure.
- `OUTPUT_ERROR` / `FLAG_INVALID` / `INTERNAL` (exit 1).

Exit codes: `0` success; `1` internal/output/flag; `2` user input; `3` REST API layer. Always parse `error.code` and `error.details`, not just the exit code.

For internal HTTPS YARN gateways with self-signed or non-standard certificates, use `--insecure-skip-verify`.

## Do Not

- Do not ask for screenshots before running `flink-cli diagnose`.
- Do not assume missing optional endpoints mean the job is unhealthy; check `warnings`.
- Do not request `--include-snapshot` by default; it can be large.
- Do not request `flink-cli flamegraph --include-raw` by default; the raw tree can be large.
- Do not claim YARN queue/resource-manager root cause from Flink REST data alone. Use YARN diagnostics/logs for scheduler-side causes.
