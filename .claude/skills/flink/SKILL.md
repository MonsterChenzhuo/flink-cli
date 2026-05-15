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

2. Use the top-level fields first:

   - `schema_version`: output contract version.
   - `source_endpoints`: REST endpoints successfully used.
   - `warnings`: non-fatal collection gaps.
   - `summary.jobs_by_state`: quick job state count.
   - `findings`: ordered by severity.
   - `next_actions`: recommended next diagnostic steps.

3. Drill down by finding:

   - `root_exception`, `job_failed`, `vertex_failed`: read `evidence.root_exception`, failed vertex evidence, then fetch JobManager/TaskManager logs or YARN diagnostics.
   - `checkpoint_failure_rate`, `checkpoint_slow`: check checkpoint storage, state backend, sink commit latency, alignment duration, and backpressure.
   - `backpressure_high`: follow the affected vertex downstream; inspect sink/external system latency, network buffers, and checkpoint alignment.
   - `task_state_abnormal`: map failed tasks to their vertex and TaskManager logs.
   - `no_obvious_issue`: REST data does not show obvious job-level failure; continue with business throughput/latency, TaskManager logs, and external system metrics.

4. Only request full raw data when needed:

   ```bash
   flink-cli diagnose --include-snapshot <flink-web-ui-url>
   ```

   Default output intentionally omits the raw snapshot to keep Claude Code/Codex context small.

## Input handling

`flink-cli` accepts both full URLs and `host:port`; `host:port` defaults to `http://host:port`. Gateway path prefixes are preserved when constructing REST paths.

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

## Do Not

- Do not ask for screenshots before running `flink-cli diagnose`.
- Do not assume missing optional endpoints mean the job is unhealthy; check `warnings`.
- Do not request `--include-snapshot` by default; it can be large.
- Do not claim YARN queue/resource-manager root cause from Flink REST data alone. Use YARN diagnostics/logs for scheduler-side causes.
