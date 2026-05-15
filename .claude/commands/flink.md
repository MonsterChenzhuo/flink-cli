---
description: Diagnose a Flink 1.18 job from a Flink Web UI URL using flink-cli
argument-hint: "<flink-web-ui-url> [jobId]"
---

请使用 `flink-cli` 诊断 Flink 作业，并用中文总结结论。

参数：

- 第一个参数是 Flink Web UI URL，也可以是 `host:port`。
- 如果第二个参数存在，把它当作 Flink job id，执行时加 `--job-id`。

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

4. 优先阅读输出里的 `diagnosis`、`primary_finding`、`summary`、`findings`、`next_actions`、`warnings`。
5. 默认不要加 `--include-snapshot`，除非需要完整 REST 原始数据。
6. 如果需要先列出可用 job，运行：

   ```bash
   flink-cli diagnose --list-jobs "$1"
   ```

输出给用户时：

- 先给一句直接结论。
- 再列出 2-4 条关键证据。
- 最后给下一步排查动作。
- 不要把完整 JSON 原样贴给用户，除非用户明确要求。
