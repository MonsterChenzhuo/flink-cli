---
name: flink-runtime-diagnostics
description: Use when investigating Apache Flink 1.18 jobs through a Flink Web UI URL, especially YARN application mode jobs, checkpoint failures, root exceptions, failed vertices, or backpressure.
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
   - `sink_busy_upstream_backpressure`: the sink may report `backpressure=ok` while upstream vertices are backpressured. Treat this as a sink/external-system throughput bottleneck. For Doris, first read `evidence.doris_sink_metrics.summary` for sampled `per_flush_*`, `load_time_*`, `write_data_time_*`, and `load_mib_per_sec_per_subtask`; then reason about Stream Load throughput, batch size, checkpoint interval, and sink parallelism.
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

   If the URL is a full thread-dump page such as `#/task-manager/<taskManagerId>/thread-dump`, `flink-cli` automatically infers the TaskManager id. Read `summary.states`, `summary.reasons`, and `summary.interesting_threads` first. Do not add `--include-threads` unless full stacks are needed.

## Input handling

`flink-cli` accepts both full URLs and `host:port`; `host:port` defaults to `http://host:port`. Gateway path prefixes are preserved when constructing REST paths.
If the URL is a full Flink job page such as `#/job/running/<jobId>/overview`, `flink-cli` automatically infers that job id when `--job-id` is omitted.
If the URL is a full TaskManager thread dump page such as `#/task-manager/<taskManagerId>/thread-dump`, `flink-cli thread-dump` automatically infers that TaskManager id.

## Errors

Errors go to stderr as:

```json
{"error":{"code":"...","message":"...","hint":"..."}}
```

Exit codes:

- `0`: command succeeded; inspect findings for job health.
- `1`: internal/output error.
- `2`: user input error.
- `3`: Flink REST API unreachable or `/jobs/overview` failed.

For internal HTTPS YARN gateways with self-signed or non-standard certificates, use `--insecure-skip-verify`.
If the error says `returned non-JSON response` or `got HTML`, the URL did not return Flink REST JSON. Check whether the YARN application/proxy URL expired, points to a login/error page, or lost the gateway proxy path.

## Do Not

- Do not ask for screenshots before running `flink-cli diagnose`.
- Do not assume missing optional endpoints mean the job is unhealthy; check `warnings`.
- Do not request `--include-snapshot` by default; it can be large.
- Do not claim YARN queue/resource-manager root cause from Flink REST data alone. Use YARN diagnostics/logs for scheduler-side causes.
