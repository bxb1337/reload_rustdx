use super::{
    collect_day_files, is_target_stock_code, parse_day_file, parse_day_file_into_columns,
    OhlcvColumns,
};
use crate::error::{AppError, InputError, ParseError};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn parse_day_file_returns_error_for_truncated_record() {
    let mut path = std::env::temp_dir();
    path.push(unique_name("sz000001.day"));

    fs::write(&path, vec![0_u8; 31]).expect("write temp test file");
    let result = parse_day_file(&path);
    let _ = fs::remove_file(&path);

    assert!(matches!(
        result,
        Err(AppError::Parse(ParseError::InvalidDayFileSize { .. }))
    ));
}

#[test]
fn collect_day_files_returns_custom_error_for_missing_path() {
    let mut path = std::env::temp_dir();
    path.push(unique_name("does_not_exist"));

    let result = collect_day_files(&path);
    assert!(matches!(
        result,
        Err(AppError::Input(InputError::InputPathNotFound(_)))
    ));
}

#[test]
fn collect_day_files_accepts_single_day_file_input() {
    let mut file = std::env::temp_dir();
    file.push(unique_name("sz000001.day"));
    fs::write(&file, vec![0_u8; 32]).expect("write temp day file");

    let files = collect_day_files(&file).expect("collect day files");

    let _ = fs::remove_file(&file);
    assert_eq!(files, vec![file]);
}

#[test]
fn collect_day_files_reads_nested_day_files_from_directory() {
    let mut root = std::env::temp_dir();
    root.push(unique_name("dir"));
    let mut nested = root.clone();
    nested.push("nested");
    fs::create_dir_all(&nested).expect("create temp directories");

    let mut day_a = root.clone();
    day_a.push("sh600000.day");
    let mut day_b = nested.clone();
    day_b.push("sz000001.day");
    let mut txt = nested;
    txt.push("ignore.txt");

    fs::write(&day_a, vec![0_u8; 32]).expect("write day file in root");
    fs::write(&day_b, vec![0_u8; 32]).expect("write day file in nested");
    fs::write(&txt, b"x").expect("write non-day file");

    let files = collect_day_files(&root).expect("collect day files from dir");

    let _ = fs::remove_file(&day_a);
    let _ = fs::remove_file(&day_b);
    let _ = fs::remove_file(&txt);
    let _ = fs::remove_dir_all(&root);

    let mut expected = vec![day_a, day_b];
    expected.sort();
    assert_eq!(files, expected);
}

#[test]
fn parse_day_file_uses_full_file_stem_as_code() {
    let mut root = std::env::temp_dir();
    root.push(unique_name("dir"));
    fs::create_dir_all(&root).expect("create temp directory");

    let mut path = root.clone();
    path.push("sz000001.day");
    fs::write(&path, valid_day_record()).expect("write temp day file");

    let rows = parse_day_file(&path).expect("parse day file");

    let _ = fs::remove_file(&path);
    let _ = fs::remove_dir_all(&root);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].code, "sz000001");
}

#[test]
fn parse_day_file_into_columns_appends_parsed_values() {
    let mut root = std::env::temp_dir();
    root.push(unique_name("dir"));
    fs::create_dir_all(&root).expect("create temp directory");

    let mut path = root.clone();
    path.push("sz000001.day");
    fs::write(&path, valid_day_record()).expect("write temp day file");

    let mut columns = OhlcvColumns::default();
    parse_day_file_into_columns(&path, &mut columns, None).expect("parse day file into columns");

    let _ = fs::remove_file(&path);
    let _ = fs::remove_dir_all(&root);

    assert_eq!(columns.codes, vec!["sz000001".to_owned()]);
    assert_eq!(columns.dates, vec!["2024-01-31".to_owned()]);
    assert_eq!(columns.opens, vec![12.34]);
    assert_eq!(columns.highs, vec![13.0]);
    assert_eq!(columns.lows, vec![12.0]);
    assert_eq!(columns.closes, vec![12.5]);
    assert_eq!(columns.volumes, vec![10_000]);
    assert_eq!(columns.bonus_shares, vec![None]);
    assert_eq!(columns.cash_dividend, vec![None]);
    assert_eq!(columns.rights_issue_shares, vec![None]);
    assert_eq!(columns.rights_issue_price, vec![None]);
}

#[test]
fn is_target_stock_code_matches_configured_prefixes() {
    assert!(is_target_stock_code("sh600000"));
    assert!(is_target_stock_code("sh601318"));
    assert!(is_target_stock_code("sh603288"));
    assert!(is_target_stock_code("sh605499"));
    assert!(is_target_stock_code("sh688001"));
    assert!(is_target_stock_code("sz000001"));
    assert!(is_target_stock_code("sz001696"));
    assert!(is_target_stock_code("sz002415"));
    assert!(is_target_stock_code("sz003816"));
    assert!(is_target_stock_code("sz004001"));
    assert!(is_target_stock_code("sz300750"));

    assert!(!is_target_stock_code("sh602000"));
    assert!(!is_target_stock_code("sh900901"));
    assert!(!is_target_stock_code("sz200001"));
    assert!(!is_target_stock_code("sz301001"));
    assert!(!is_target_stock_code("bj430047"));
    assert!(!is_target_stock_code("invalid"));
}

fn unique_name(suffix: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after unix epoch")
        .as_nanos();
    PathBuf::from(format!("reload_rustdx_{now}_{suffix}"))
}

fn valid_day_record() -> Vec<u8> {
    let mut buf = vec![0_u8; 32];
    buf[0..4].copy_from_slice(&20240131_u32.to_le_bytes());
    buf[4..8].copy_from_slice(&1234_u32.to_le_bytes());
    buf[8..12].copy_from_slice(&1300_u32.to_le_bytes());
    buf[12..16].copy_from_slice(&1200_u32.to_le_bytes());
    buf[16..20].copy_from_slice(&1250_u32.to_le_bytes());
    buf[24..28].copy_from_slice(&10000_u32.to_le_bytes());
    buf
}
