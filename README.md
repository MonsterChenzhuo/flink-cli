# flink-cli

`flink-cli` 是一个面向 AI agent 和运维人员的 Flink 诊断 CLI。用户只需要传入 Flink Web UI URL，工具会通过 Flink 1.18 REST API 拉取作业、异常、checkpoint、反压和配置摘要，并输出统一 JSON。

## 快速开始

```bash
flink-cli diagnose http://jobmanager-host:8081
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
- `/jobs/:jobid/checkpoints`：checkpoint 统计和最近 checkpoint。
- `/jobs/:jobid/vertices/:vertexid/backpressure`：vertex 反压采样结果。

REST API 依据 Apache Flink 1.18 官方文档：
https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/rest_api/

## 输出示例

```json
{
  "scenario": "diagnose",
  "ui_url": "http://jobmanager-host:8081",
  "flink_version": "1.18.1",
  "elapsed_ms": 120,
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
  ]
}
```

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
```
