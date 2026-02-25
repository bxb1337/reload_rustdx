use super::{AdjustedMode, Args};
use clap::Parser;

#[test]
fn onlystocks_defaults_to_true() {
    let args = Args::parse_from(["reload_rustdx", "--input", "./input"]);
    assert!(args.onlystocks);
}

#[test]
fn onlystocks_can_be_set_false() {
    let args = Args::parse_from([
        "reload_rustdx",
        "--input",
        "./input",
        "--onlystocks",
        "false",
    ]);
    assert!(!args.onlystocks);
}

#[test]
fn stocks_per_batch_defaults_to_30() {
    let args = Args::parse_from(["reload_rustdx", "--input", "./input"]);
    assert_eq!(args.stocks_per_batch, 30);
}

#[test]
fn stocks_per_batch_can_be_overridden() {
    let args = Args::parse_from([
        "reload_rustdx",
        "--input",
        "./input",
        "--stocks-per-batch",
        "12",
    ]);
    assert_eq!(args.stocks_per_batch, 12);
}

#[test]
fn adjusted_mode_defaults_to_none() {
    let args = Args::parse_from(["reload_rustdx", "--input", "./input"]);
    assert_eq!(args.adjusted, AdjustedMode::None);
}

#[test]
fn adjusted_mode_parses_hfq() {
    let args = Args::parse_from(["reload_rustdx", "--input", "./input", "--adjusted", "hfq"]);
    assert_eq!(args.adjusted, AdjustedMode::Hfq);
}

#[test]
fn adjusted_mode_parses_both() {
    let args = Args::parse_from(["reload_rustdx", "--input", "./input", "--adjusted", "both"]);
    assert_eq!(args.adjusted, AdjustedMode::Both);
}
