[English](./README.md) | 简体中文

# reload_rustdx

`reload_rustdx` 是一个高性能 Rust 命令行工具，用于将[通达信](http://www.tdx.com.cn/)（TDX）的二进制日线文件（`.day`）批量转换为标准 OHLCV CSV 格式。支持合并 GBBQ 股本变迁数据，并可一键生成**前复权**和**后复权**价格文件。

## 功能特性

- **批量解析**：支持输入单个 `.day` 文件，或递归扫描目录中所有 `.day` 文件
- **并行处理**：利用多线程同时解析文件，自动按 CPU 核心数分配工作线程
- **进度可视**：实时进度条显示处理进度与耗时
- **A 股过滤**：默认按常见 A 股代码前缀过滤，剔除指数、ETF 等非股票文件
- **GBBQ 合并**：传入 GBBQ 文件后，可将送股、分红、配股等公司行为数据合并进 CSV
- **复权计算**：支持前复权（QFQ）、后复权（HFQ）或同时生成两份复权文件
- **批次写入**：按批次刷盘，避免大量数据时内存溢出

## 快速开始

### 环境要求

- Rust 工具链（稳定版，edition 2024）

如果尚未安装，使用 `rustup` 一键安装：

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### 构建

```bash
cargo build --release
```

构建完成后，可执行文件位于：

```
./target/release/reload_rustdx
```

## 用法

```
reload_rustdx --input <路径> [--output <CSV>] [--gbbq <GBBQ文件>] [--adjusted <none|qfq|hfq|both>] [--onlystocks <true|false>] [--stocks-per-batch <N>]
```

### 参数说明

| 参数 | 简写 | 默认值 | 说明 |
|------|------|--------|------|
| `--input <PATH>` | `-i` | 必填 | `.day` 文件路径，或包含 `.day` 文件的目录 |
| `--output <PATH>` | `-o` | `output/stocks.csv` | 输出 CSV 文件路径 |
| `--gbbq <PATH>` | `-g` | 无 | GBBQ 文件路径，启用后合并公司行为字段 |
| `--adjusted <MODE>` | — | `none` | 复权模式：`none` / `qfq` / `hfq` / `both` |
| `--onlystocks <BOOL>` | — | `true` | 是否仅保留 A 股代码前缀 |
| `--stocks-per-batch <N>` | — | `30` | 每次刷盘前最多缓冲的股票数量 |

> **注意**：当 `--adjusted` 不为 `none` 时，必须同时提供 `--gbbq`。

### 支持的 A 股代码前缀

| 交易所 | 代码前缀 |
|--------|----------|
| 上交所（沪市） | `sh600`、`sh601`、`sh603`、`sh605`、`sh688` |
| 深交所（深市） | `sz000`、`sz001`、`sz002`、`sz003`、`sz004`、`sz300` |

## 使用示例

**处理目录中所有 `.day` 文件（最常用）：**

```bash
cargo run --release -- --input ./vipdoc
# 输出到 ./output/stocks.csv
```

**指定输出路径：**

```bash
cargo run --release -- --input ./vipdoc --output ./data/a_shares.csv
```

**启用 GBBQ 合并与前复权（适合量化研究）：**

```bash
cargo run --release -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted qfq
# 生成 output/stocks.csv（原始价格）和 output/stocks_qfq.csv（前复权价格）
```

**同时生成前复权与后复权：**

```bash
cargo run --release -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted both
# 生成 stocks.csv、stocks_qfq.csv、stocks_hfq.csv
```

**处理单个 `.day` 文件：**

```bash
cargo run --release -- --input ./vipdoc/sh/sh600000.day --output ./sh600000.csv
```

**关闭 A 股代码过滤（保留全部文件）：**

```bash
cargo run --release -- --input ./vipdoc --onlystocks false
```

## 输入格式说明

### `.day` 文件格式

通达信日线文件，每条记录固定 **32 字节**，小端序（Little-Endian）存储：

| 偏移 | 长度 | 类型 | 说明 |
|------|------|------|------|
| 0 | 4 | u32 | 日期（YYYYMMDD 整数，例如 `20240131`） |
| 4 | 4 | u32 | 开盘价 × 100 |
| 8 | 4 | u32 | 最高价 × 100 |
| 12 | 4 | u32 | 最低价 × 100 |
| 16 | 4 | u32 | 收盘价 × 100 |
| 20 | 4 | — | 保留字段（未使用） |
| 24 | 4 | u32 | 成交量（股数） |
| 28 | 4 | — | 保留字段（未使用） |

- 股票代码由文件名推导，例如 `sz000001.day` → `sz000001`

### GBBQ 文件格式

通达信 GBBQ（股本变迁）二进制文件，每条记录固定 **29 字节**，使用 Blowfish 算法加密。文件头 4 字节存储记录总数。

## 输出格式说明

### 基础 CSV（无 GBBQ）

```
code,date,open,high,low,close,volume
sz000001,2024-01-31,12.34,13.00,12.00,12.50,10000
```

| 列名 | 类型 | 说明 |
|------|------|------|
| `code` | string | 股票代码（含交易所前缀） |
| `date` | string | 日期（YYYY-MM-DD） |
| `open` | float | 开盘价（元） |
| `high` | float | 最高价（元） |
| `low` | float | 最低价（元） |
| `close` | float | 收盘价（元） |
| `volume` | int | 成交量（股） |

### 启用 GBBQ 后附加列

| 列名 | 类型 | 说明 |
|------|------|------|
| `bonus_shares` | float|null | 送股比例（每股送股数） |
| `cash_dividend` | float|null | 现金分红（每股分红金额，元） |
| `rights_issue_shares` | float|null | 配股比例（每股配股数） |
| `rights_issue_price` | float|null | 配股价格（元） |

> 未发生公司行为的日期，上述列值为空（null）。

### 复权文件命名规则

若输出文件为 `stocks.csv`，复权文件自动命名为：

- 前复权：`stocks_qfq.csv`
- 后复权：`stocks_hfq.csv`

## 项目架构

```
src/
├── main.rs           # 程序入口：CLI 解析、并发调度、CSV 批量写入
├── error.rs          # 错误类型定义
├── cli/
│   ├── mod.rs
│   └── args.rs       # clap 参数定义（Args、AdjustedMode）
└── core/
    ├── mod.rs
    ├── tdx_day.rs    # .day 文件解析（二进制 → OhlcvRow）
    ├── tdx_gbbq.rs   # GBBQ 文件解析与解密
    ├── tdx_gbbq_key.rs # Blowfish 解密密钥
    ├── qfq.rs        # 前复权价格计算
    └── hfq.rs        # 后复权价格计算
```

## 开发

```bash
# 检查代码格式
cargo fmt -- --check

# 运行 Clippy 静态检查
cargo clippy

# 运行所有测试
cargo test

# 构建（调试模式）
cargo build

# 构建（发布模式，性能更佳）
cargo build --release
```

## 依赖项

| Crate | 用途 |
|-------|------|
| [`clap`](https://docs.rs/clap) | 命令行参数解析 |
| [`polars`](https://docs.rs/polars) | 数据帧构建与 CSV 写入 |
| [`indicatif`](https://docs.rs/indicatif) | 终端进度条 |

## 许可证

本项目基于 [MIT License](./LICENSE)。
