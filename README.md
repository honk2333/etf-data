# ETF 日线数据管理（TickFlow + DuckDB）

这个项目提供一个可直接运行的增量同步脚本：

- 首次：从 TickFlow 回补 ETF 从可获得最早日期到最新日期的日线数据
- 后续：只抓取上次更新后缺失的日线数据（不重复全量拉取）
- 存储：只落地到 DuckDB

## 环境

- Conda 环境：`multifactor-etf`
- 依赖：`tickflow`、`duckdb`、`pandas`、`exchange_calendars`

## ETF 列表文件

默认读取 `data/etf_list.csv`，并固定使用 `代码` 列。
- 若代码是 `510300` 这类 6 位数字，会自动标准化为 `510300.SH` / `159915.SZ`

示例见 [examples/etf_list.sample.csv](/Users/hongke/代码/etf-data/examples/etf_list.sample.csv)。

## 运行

先准备 ETF 列表文件，然后执行：

```bash
python scripts/update_etf_daily.py \
  --etf-list data/etf_list.csv \
  --db-path data/etf_daily.duckdb
```

可选参数：

- `--api-key`：TickFlow API Key（不传则读 `TICKFLOW_API_KEY`；再没有则走 `TickFlow.free()`）
- `--limit`：仅同步前 N 个标的（调试用）
- `--batch-size`：每次请求条数（默认 10000）
- `--sleep-seconds`：请求最小间隔秒数（默认 1.10，避免免费接口限流）

## 输出

- DuckDB：`data/etf_daily.duckdb`，主表 `etf_daily`
