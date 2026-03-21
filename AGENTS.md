# AGENTS.md - reload_rustdx

**Generated:** 2026-03-21 | **Commit:** 2d9448c | **Branch:** master

## OVERVIEW

TDX binary `.day` file parser → OHLCV CSV converter. Merges GBBQ corporate-action data for forward-adjusted (QFQ) and backward-adjusted (HFQ) price output.

## STRUCTURE

```
src/
├── main.rs           # Entry: CLI parsing, worker spawning, batched CSV writes
├── error.rs          # AppError hierarchy (Input, Parse, Runtime, Output)
├── cli/
│   ├── mod.rs
│   └── args.rs       # clap Args, AdjustedMode enum
└── core/             # TDX binary parsing domain — see src/core/AGENTS.md
```

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Add CLI flag | `src/cli/args.rs` |
| New error type | `src/error.rs` (4 categories: Input, Parse, Runtime, Output) |
| Modify .day parsing | `src/core/tdx_day.rs` |
| GBBQ decryption | `src/core/tdx_gbbq.rs`, `src/core/tdx_gbbq_key.rs` |
| Price adjustment math | `src/core/qfq.rs`, `src/core/hfq.rs` |
| Parallel processing logic | `src/main.rs:spawn_parser_workers` |
| Output CSV format | `src/main.rs:dataframe_from_columns` |

## CONVENTIONS

**Test placement:** `*_tests.rs` files alongside source (not `#[cfg(test)]` modules, not `tests/` dir).
```rust
// In main.rs:
#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
```

**Error handling:** No `thiserror`/`anyhow`. Custom `AppError` enum with `Display` impls for user-facing messages.

**Parallelism:** Rayon `par_iter` for .day file collection; mpsc channels for parser→writer communication.

## ANTI-PATTERNS

- No `unsafe` blocks in this codebase (keep it that way)
- No global variables
- Break complex functions into smaller ones

## COMMANDS

```bash
cargo build --release          # Production build
cargo run --release -- -i ./vipdoc   # Process all .day files
cargo test                     # Run tests
cargo bench                    # Run benchmarks
cargo clippy -- -D warnings    # Lint
```

## NOTES

- Default output: `output/stocks.csv` (auto-created)
- Adjusted files: `stocks_qfq.csv`, `stocks_hfq.csv`
- `--adjusted` requires `--gbbq` path
- Supported A-share prefixes: `sh600/601/603/605/688`, `sz000/001/002/003/004/300`
