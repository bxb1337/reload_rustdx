[English](./README.md) | 简体中文

# reload_rustdx

`reload_rustdx` 是一个 Rust 命令行工具，用于解析通达信 `*.day` 日线文件并导出 OHLCV CSV。可选支持 GBBQ 股本变迁数据合并，并生成复权 CSV。

## 功能

- 支持输入单个 `.day` 文件，或递归扫描目录中的 `.day` 文件。
- 默认按 A 股常见代码前缀过滤（`--onlystocks true`）。
- 传入 `--gbbq` 后可合并公司行为字段（送股、分红、配股等）。
- 通过 `--adjusted qfq|hfq|both|none` 控制复权输出（默认 `none`），启用 GBBQ 时可生成前复权/后复权文件。
- 多线程解析，带进度条，按批次写入 CSV。

## 环境要求

- Rust 工具链（本项目使用 edition 2024）

如果未安装 Rust，可使用 `rustup`：

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## 构建

```bash
cargo build --release
```

可执行文件示例：

```bash
./target/release/reload_rustdx --help
```

## 用法

```bash
reload_rustdx --input <路径> [--output <CSV>] [--gbbq <GBBQ文件>] [--adjusted <none|qfq|hfq|both>] [--onlystocks <true|false>] [--stocks-per-batch <N>]
```

### 参数说明

- `-i, --input <PATH>`：必填。`.day` 文件或包含 `.day` 文件的目录。
- `-o, --output <PATH>`：可选输出路径。默认是当前目录下 `stocks.csv`。
- `-g, --gbbq <PATH>`：可选 GBBQ 文件。启用后会合并公司行为字段。
- `--adjusted <MODE>`：复权输出模式，可选 `none|qfq|hfq|both`，默认 `none`。当模式不是 `none` 时，必须传入 `--gbbq`。
- `--onlystocks <BOOL>`：是否仅保留支持的 A 股代码前缀。默认：`true`。
- `--stocks-per-batch <N>`：每次刷盘前最多缓冲多少只股票。默认：`30`。

### 支持的股票代码前缀

- 上交所：`sh600`、`sh601`、`sh603`、`sh605`、`sh688`
- 深交所：`sz000`、`sz001`、`sz002`、`sz003`、`sz004`、`sz300`

### 示例

处理目录中的 `.day` 文件：

```bash
cargo run -- --input ./vipdoc
```

指定输出路径：

```bash
cargo run -- --input ./vipdoc --output ./out/stocks.csv
```

启用 GBBQ 合并与前复权输出：

```bash
cargo run -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted qfq
```

启用后复权输出：

```bash
cargo run -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted hfq
```

同时生成前复权和后复权：

```bash
cargo run -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted both
```

若输出是 `stocks.csv`，复权文件默认写为 `stocks_qfq.csv` 和/或 `stocks_hfq.csv`。

关闭股票代码过滤：

```bash
cargo run -- --input ./vipdoc --onlystocks false
```

## 输入与输出说明

- `.day` 每条记录固定 32 字节。
- 股票代码来自输入文件名（例如 `sz000001.day` -> `sz000001`）。
- 传入 `--gbbq` 时，输出 CSV 会包含以下附加列：
  - `bonus_shares`
  - `cash_dividend`
  - `rights_issue_shares`
  - `rights_issue_price`

## 开发

```bash
cargo fmt -- --check
cargo clippy
cargo test
cargo build
```

## 许可证

本项目基于 MIT License。
