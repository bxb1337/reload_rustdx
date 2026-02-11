use super::{
    collect_filtered_day_files, decide_worker_count, validate_gbbq_path, StockBatchCsvWriter,
};
use crate::cli::Args;
use crate::core::tdx_day::OhlcvColumns;
use crate::error::{AppError, InputError};
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn write_csv_chunk_writes_header_once_when_appending_multiple_stocks() {
    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("stocks.csv"));

    let mut writer = StockBatchCsvWriter::new(&output_path, 8).expect("create csv batch writer");
    let mut first = sample_columns("sz000001", "2024-01-31", 12.34, 13.0, 12.0, 12.5, 10_000);
    writer.push_chunk(&mut first).expect("write first chunk");

    let mut second = sample_columns("sh600000", "2024-01-31", 8.11, 8.5, 8.0, 8.3, 20_000);
    writer.push_chunk(&mut second).expect("write second chunk");
    writer.finish().expect("flush pending rows");

    let mut written = String::new();
    fs::File::open(&output_path)
        .expect("open output csv")
        .read_to_string(&mut written)
        .expect("read output csv");
    let _ = fs::remove_file(&output_path);

    let lines = written.lines().collect::<Vec<_>>();
    assert_eq!(lines[0], "code,date,open,high,low,close,volume");
    assert_eq!(lines[1], "sz000001,2024-01-31,12.34,13.0,12.0,12.5,10000");
    assert_eq!(lines[2], "sh600000,2024-01-31,8.11,8.5,8.0,8.3,20000");
    assert_eq!(lines.len(), 3);
}

#[test]
fn stock_batch_writer_flushes_on_threshold_and_preserves_all_rows() {
    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("stocks_batch_threshold.csv"));

    let mut writer = StockBatchCsvWriter::new(&output_path, 2).expect("create csv batch writer");
    let mut first = sample_columns("sz000001", "2024-01-31", 12.34, 13.0, 12.0, 12.5, 10_000);
    let mut second = sample_columns("sh600000", "2024-01-31", 8.11, 8.5, 8.0, 8.3, 20_000);
    let mut third = sample_columns("sz300750", "2024-01-31", 120.0, 121.0, 119.0, 120.5, 30_000);

    writer.push_chunk(&mut first).expect("push first chunk");
    writer.push_chunk(&mut second).expect("push second chunk");
    writer.push_chunk(&mut third).expect("push third chunk");
    writer.finish().expect("finish writer");

    let mut written = String::new();
    fs::File::open(&output_path)
        .expect("open output csv")
        .read_to_string(&mut written)
        .expect("read output csv");
    let _ = fs::remove_file(&output_path);

    let lines = written.lines().collect::<Vec<_>>();
    assert_eq!(lines[0], "code,date,open,high,low,close,volume");
    assert_eq!(lines[1], "sz000001,2024-01-31,12.34,13.0,12.0,12.5,10000");
    assert_eq!(lines[2], "sh600000,2024-01-31,8.11,8.5,8.0,8.3,20000");
    assert_eq!(
        lines[3],
        "sz300750,2024-01-31,120.0,121.0,119.0,120.5,30000"
    );
    assert_eq!(lines.len(), 4);
}

#[test]
fn decide_worker_count_is_bounded_by_jobs_and_cpu() {
    assert_eq!(decide_worker_count(0, 8), 1);
    assert_eq!(decide_worker_count(1, 8), 1);
    assert_eq!(decide_worker_count(2, 8), 2);
    assert_eq!(decide_worker_count(8, 2), 2);
}

#[test]
fn validate_gbbq_path_returns_custom_error_when_file_missing() {
    let args = Args {
        input: std::env::temp_dir(),
        output: None,
        gbbq: Some(PathBuf::from("/definitely/not/exist.gbbq")),
        onlystocks: true,
        stocks_per_batch: 30,
    };

    let result = validate_gbbq_path(&args);
    assert!(matches!(
        result,
        Err(AppError::Input(InputError::GbbqFileNotFound(_)))
    ));
}

#[test]
fn collect_filtered_day_files_returns_custom_error_when_none_found() {
    let mut input = std::env::temp_dir();
    input.push(unique_name("empty_dir"));
    fs::create_dir_all(&input).expect("create empty input directory");

    let args = Args {
        input: input.clone(),
        output: None,
        gbbq: None,
        onlystocks: true,
        stocks_per_batch: 30,
    };

    let result = collect_filtered_day_files(&args);
    let _ = fs::remove_dir_all(&input);
    assert!(matches!(
        result,
        Err(AppError::Input(InputError::NoDayFilesFound(_)))
    ));
}

#[test]
fn collect_filtered_day_files_keeps_only_target_stock_codes() {
    let mut input = std::env::temp_dir();
    input.push(unique_name("filter_dir"));
    fs::create_dir_all(&input).expect("create temp input directory");

    let mut keep = input.clone();
    keep.push("sz000001.day");
    let mut drop = input.clone();
    drop.push("bj430047.day");

    fs::write(&keep, vec![0_u8; 32]).expect("write target stock file");
    fs::write(&drop, vec![0_u8; 32]).expect("write non-target stock file");

    let args = Args {
        input: input.clone(),
        output: None,
        gbbq: None,
        onlystocks: true,
        stocks_per_batch: 30,
    };

    let result = collect_filtered_day_files(&args).expect("collect filtered files");

    let _ = fs::remove_file(&keep);
    let _ = fs::remove_file(&drop);
    let _ = fs::remove_dir_all(&input);

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].file_stem().and_then(|name| name.to_str()),
        Some("sz000001")
    );
}

fn sample_columns(
    code: &str,
    date: &str,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
) -> OhlcvColumns {
    OhlcvColumns {
        codes: vec![code.to_owned()],
        dates: vec![date.to_owned()],
        opens: vec![open],
        highs: vec![high],
        lows: vec![low],
        closes: vec![close],
        volumes: vec![volume],
    }
}

fn unique_name(suffix: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after unix epoch")
        .as_nanos();
    PathBuf::from(format!("reload_rustdx_{now}_{suffix}"))
}
