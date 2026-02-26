English | [简体中文](./README_zh.md)

# reload_rustdx

`reload_rustdx` is a high-performance Rust CLI that converts [TongDaXin (TDX)](http://www.tdx.com.cn/) binary daily stock files (`.day`) into standard OHLCV CSV format. It optionally merges GBBQ corporate-action data and generates **forward-adjusted (QFQ)** and **backward-adjusted (HFQ)** price files.

## Features

- **Batch parsing**: Process a single `.day` file or recursively scan a directory for all `.day` files
- **Parallel processing**: Spawns worker threads automatically scaled to available CPU cores
- **Progress bar**: Real-time terminal progress bar showing elapsed time and position
- **A-share filtering**: Filters to common A-share code prefixes by default, excluding indices and ETFs
- **GBBQ merging**: Merges bonus shares, cash dividends, and rights-issue fields when `--gbbq` is provided
- **Adjusted prices**: Generate forward-adjusted (QFQ), backward-adjusted (HFQ), or both output files
- **Batched writes**: Buffers stocks in configurable batches before flushing to disk

## Quick Start

### Requirements

- Rust toolchain (stable, edition 2024)

Install with `rustup` if needed:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Build

```bash
cargo build --release
```

Binary is located at:

```
./target/release/reload_rustdx
```

## Usage

```
reload_rustdx --input <PATH> [--output <CSV>] [--gbbq <GBBQ>] [--adjusted <none|qfq|hfq|both>] [--onlystocks <true|false>] [--stocks-per-batch <N>]
```

### CLI Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--input <PATH>` | `-i` | Required | A `.day` file or directory containing `.day` files |
| `--output <PATH>` | `-o` | `output/stocks.csv` | Output CSV file path |
| `--gbbq <PATH>` | `-g` | None | GBBQ file path; enables corporate-action columns |
| `--adjusted <MODE>` | — | `none` | Adjustment mode: `none` / `qfq` / `hfq` / `both` |
| `--onlystocks <BOOL>` | — | `true` | Filter to supported A-share code prefixes |
| `--stocks-per-batch <N>` | — | `30` | Max stocks buffered before each CSV flush |
## License

This project is licensed under the [MIT License](./LICENSE).
## Dependencies

| Crate | Purpose |
|-------|---------|
| [`clap`](https://docs.rs/clap) | CLI argument parsing |
| [`polars`](https://docs.rs/polars) | DataFrame construction and CSV writing |
| [`indicatif`](https://docs.rs/indicatif) | Terminal progress bar |
## Development

```bash
# Check formatting
cargo fmt -- --check

# Run linter
cargo clippy

# Run all tests
cargo test

# Debug build
cargo build

# Release build (recommended for production)
cargo build --release
```
> Rows with no corporate action on that date will have `null` in these columns.

### Adjusted Output Naming

If output is `stocks.csv`, adjusted files are auto-named:

- Forward-adjusted: `stocks_qfq.csv`
- Backward-adjusted: `stocks_hfq.csv`

## Project Structure

```
src/
├── main.rs           # Entry point: CLI parsing, concurrency, batched CSV writes
├── error.rs          # Error type hierarchy
├── cli/
│   ├── mod.rs
│   └── args.rs       # clap Args struct and AdjustedMode enum
└── core/
    ├── mod.rs
    ├── tdx_day.rs    # .day binary parser (bytes → OhlcvRow)
    ├── tdx_gbbq.rs   # GBBQ binary parser and Blowfish decryption
    ├── tdx_gbbq_key.rs # Blowfish key material
    ├── qfq.rs        # Forward-adjusted price computation
    └── hfq.rs        # Backward-adjusted price computation
```
### Additional Columns with GBBQ

| Column | Type | Description |
|--------|------|-------------|
| `bonus_shares` | float|null | Bonus shares per existing share |
| `cash_dividend` | float|null | Cash dividend per share (CNY) |
| `rights_issue_shares` | float|null | Rights-issue shares per existing share |
| `rights_issue_price` | float|null | Rights-issue price (CNY) |
Stock code is derived from the filename stem, e.g. `sz000001.day` → `sz000001`.

### GBBQ File Format

TDX GBBQ binary file: 4-byte header (record count), followed by **29-byte** encrypted records (Blowfish cipher).

## Output Format

### Base CSV (no GBBQ)

```
code,date,open,high,low,close,volume
sz000001,2024-01-31,12.34,13.00,12.00,12.50,10000
```

| Column | Type | Description |
|--------|------|-------------|
| `code` | string | Stock code with exchange prefix |
| `date` | string | Date in `YYYY-MM-DD` format |
| `open` | float | Opening price (CNY) |
| `high` | float | Highest price (CNY) |
| `low` | float | Lowest price (CNY) |
| `close` | float | Closing price (CNY) |
| `volume` | int | Volume in shares |
## Examples

**Process all `.day` files in a directory (most common):**

```bash
cargo run --release -- --input ./vipdoc
# Output: ./output/stocks.csv
```

**Specify output path:**

```bash
cargo run --release -- --input ./vipdoc --output ./data/a_shares.csv
```

**Enable GBBQ merge with forward-adjusted output (ideal for quant research):**

```bash
cargo run --release -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted qfq
# Generates: output/stocks.csv (raw) and output/stocks_qfq.csv (QFQ-adjusted)
```

**Generate both forward- and backward-adjusted files:**

```bash
cargo run --release -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted both
# Generates: stocks.csv, stocks_qfq.csv, stocks_hfq.csv
```

**Process a single `.day` file:**

```bash
cargo run --release -- --input ./vipdoc/sh/sh600000.day --output ./sh600000.csv
```

**Disable stock-code filtering:**

```bash
cargo run --release -- --input ./vipdoc --onlystocks false
```

## Input Format

### `.day` File Layout

Each record is exactly **32 bytes**, little-endian:

| Offset | Length | Type | Description |
|--------|--------|------|-------------|
| 0 | 4 | u32 | Date as `YYYYMMDD` integer (e.g. `20240131`) |
| 4 | 4 | u32 | Open price × 100 |
| 8 | 4 | u32 | High price × 100 |
| 12 | 4 | u32 | Low price × 100 |
| 16 | 4 | u32 | Close price × 100 |
| 20 | 4 | — | Reserved (unused) |
| 24 | 4 | u32 | Volume in shares |
| 28 | 4 | — | Reserved (unused) |

> **Note**: `--adjusted` values other than `none` require `--gbbq`.

### Supported A-share Code Prefixes

| Exchange | Prefixes |
|----------|----------|
| Shanghai (SSE) | `sh600`, `sh601`, `sh603`, `sh605`, `sh688` |
| Shenzhen (SZSE) | `sz000`, `sz001`, `sz002`, `sz003`, `sz004`, `sz300` |
