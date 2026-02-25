English | [简体中文](./README_zh.md)

# reload_rustdx

`reload_rustdx` is a Rust CLI for parsing TDX `.day` stock files and exporting OHLCV data to CSV. It can also merge GBBQ corporate-action data and generate adjusted CSV outputs.

## Features

- Parse a single `.day` file or recursively scan a directory for `.day` files.
- Filter to common A-share prefixes by default (`--onlystocks true`).
- Merge GBBQ records (`bonus_shares`, `cash_dividend`, `rights_issue_*`) when `--gbbq` is provided.
- Generate adjusted CSV files with `--adjusted qfq|hfq|both|none` (default: `none`) when GBBQ input is enabled.
- Process files in parallel with a progress bar and batched CSV writes.

## Requirements

- Rust toolchain (stable; edition 2024 project)

Install Rust with `rustup` if needed:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Build

```bash
cargo build --release
```

Binary path:

```bash
./target/release/reload_rustdx --help
```

## Usage

```bash
reload_rustdx --input <PATH> [--output <CSV>] [--gbbq <GBBQ>] [--adjusted <none|qfq|hfq|both>] [--onlystocks <true|false>] [--stocks-per-batch <N>]
```

### CLI options

- `-i, --input <PATH>`: Required. A `.day` file or a directory that contains `.day` files.
- `-o, --output <PATH>`: Optional output path. Default is `stocks.csv` in the current directory.
- `-g, --gbbq <PATH>`: Optional GBBQ file. Enables merge of corporate-action columns.
- `--adjusted <MODE>`: Adjusted output mode. One of `none|qfq|hfq|both`. Default: `none`. When mode is not `none`, `--gbbq` is required.
- `--onlystocks <BOOL>`: Whether to keep only supported A-share code prefixes. Default: `true`.
- `--stocks-per-batch <N>`: Max number of stocks buffered before each CSV flush. Default: `30`.

### Supported stock code prefixes

- Shanghai: `sh600`, `sh601`, `sh603`, `sh605`, `sh688`
- Shenzhen: `sz000`, `sz001`, `sz002`, `sz003`, `sz004`, `sz300`

### Examples

Run on a directory of `.day` files:

```bash
cargo run -- --input ./vipdoc
```

Specify output path:

```bash
cargo run -- --input ./vipdoc --output ./out/stocks.csv
```

Enable GBBQ merge and forward-adjusted output:

```bash
cargo run -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted qfq
```

Generate backward-adjusted output:

```bash
cargo run -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted hfq
```

Generate both adjusted outputs:

```bash
cargo run -- --input ./vipdoc --gbbq ./gbbq.dat --adjusted both
```

If output is `stocks.csv`, adjusted outputs are written to `stocks_qfq.csv` and/or `stocks_hfq.csv`.

Disable stock-code filtering:

```bash
cargo run -- --input ./vipdoc --onlystocks false
```

## Input and output notes

- `.day` records are expected to be 32 bytes each.
- The stock code is derived from the input filename stem (for example, `sz000001.day` -> `sz000001`).
- When `--gbbq` is set, output CSV includes extra columns:
  - `bonus_shares`
  - `cash_dividend`
  - `rights_issue_shares`
  - `rights_issue_price`

## Development

```bash
cargo fmt -- --check
cargo clippy
cargo test
cargo build
```

## License

This project is licensed under the MIT License.
