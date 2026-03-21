# AGENTS.md - src/core

**Generated:** 2026-03-21

## OVERVIEW

TDX binary format parsing domain. Handles .day OHLCV records, GBBQ corporate-action decryption, and price adjustment algorithms.

## STRUCTURE

```
core/
├── tdx_day.rs        # .day binary parser (32-byte records)
├── tdx_gbbq.rs       # GBBQ parser + Blowfish decryption
├── tdx_gbbq_key.rs   # Blowfish key material (constant)
├── qfq.rs            # Forward-adjusted (前复权) price computation
└── hfq.rs            # Backward-adjusted (后复权) price computation
```

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Parse .day bytes | `tdx_day.rs:parse_day_file_into_columns` |
| Stock code filtering | `tdx_day.rs:is_target_stock_code` |
| Date formatting | `tdx_day.rs:format_date` |
| GBBQ decryption | `tdx_gbbq.rs:decrypt_gbbq_records` |
| QFQ adjustment factor | `qfq.rs:build_qfq_adjusted_prices` |
| HFQ adjustment factor | `hfq.rs:build_hfq_adjusted_prices` |

## BINARY FORMATS

### .day Record (32 bytes, little-endian)
| Offset | Type | Field |
|--------|------|-------|
| 0 | u32 | Date (YYYYMMDD) |
| 4 | u32 | Open × 100 |
| 8 | u32 | High × 100 |
| 12 | u32 | Low × 100 |
| 16 | u32 | Close × 100 |
| 20 | — | Reserved |
| 24 | u32 | Volume |
| 28 | — | Reserved |

### GBBQ Record (29 bytes, Blowfish encrypted)
4-byte header (count), then encrypted records. Decrypted fields: `bonus_shares`, `cash_dividend`, `rights_issue_shares`, `rights_issue_price`.

## CONVENTIONS

- Columnar output: `OhlcvColumns` struct for batch processing
- No allocations in hot path: pre-reserve capacity
- Date string format: `YYYY-MM-DD` (stack-allocated in `format_date`)

## ANTI-PATTERNS

- Don't modify the Blowfish key in `tdx_gbbq_key.rs`
- Don't change the 32-byte .day record size
- Don't filter stock codes outside `is_target_stock_code`
