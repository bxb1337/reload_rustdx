# Draft: HFQ + CLI Output Selection

## Requirements (confirmed)
- User request: add `后复权` capability.
- User request: add CLI parameter to choose whether adjusted file is output.

## Technical Decisions
- Planning target only (no code changes in this phase).
- Existing `qfq` implementation and adjusted CSV writer path generation should be reused as baseline for `hfq` design.
- CLI behavior decision: use one mode parameter supporting `qfq | hfq | both | none`.
- Default mode decision: if mode is omitted, default is `none`.
- Validation decision: if mode is not `none` and `--gbbq` is missing, return hard error and exit.

## Research Findings
- `src/core/qfq.rs`: `build_qfq_adjusted_prices(df)` implements qfq math over grouped `(code, date)` rows.
- `src/main.rs`: `StockBatchCsvWriter` conditionally writes adjusted output file when `include_gbbq` is true.
- `src/main.rs`: `resolve_adjusted_output_path` currently hardcodes `_qfq` suffix.
- `src/cli/args.rs`: CLI currently supports `--input`, `--output`, `--gbbq`, `--onlystocks`, `--stocks-per-batch`.
- `src/main_tests.rs`: has coverage for qfq output creation and filename behavior through `resolve_adjusted_output_path`.
- `src/cli/args_tests.rs`: has CLI parsing tests for bool + numeric options.

## Test Strategy Decision
- **Infrastructure exists**: YES (Rust native unit tests with `cargo test`).
- **Automated tests**: YES (TDD).
- **Agent-Executed QA**: Will be mandatory in final plan.

## Scope Boundaries
- INCLUDE: hfq calculation support, CLI selection mechanism for adjusted output generation, tests and docs updates tied to feature.
- EXCLUDE: unrelated parsing/performance refactors unless required by hfq feature.

## Open Questions
- Output naming: expected suffixes (`_qfq`, `_hfq`, both files?) and default behavior.
