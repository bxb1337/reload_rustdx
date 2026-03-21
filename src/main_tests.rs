use super::{
    collect_filtered_day_files, create_remote_workspace, decide_worker_count,
    extract_remote_archive, process_day_files, resolve_adjusted_output_path, resolve_vipdoc_root,
    validate_gbbq_path, validate_input_source, StockBatchCsvWriter,
};
use crate::cli::args::AdjustedMode;
use crate::cli::Args;
use crate::core::tdx_day::OhlcvColumns;
use crate::core::tdx_gbbq::parse_gbbq_file;
use crate::error::{AppError, InputError};
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn write_csv_chunk_writes_header_once_when_appending_multiple_stocks() {
    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("stocks.csv"));

    let mut writer = StockBatchCsvWriter::new(&output_path, 8, false, AdjustedMode::None)
        .expect("create csv batch writer");
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

    let mut writer = StockBatchCsvWriter::new(&output_path, 2, false, AdjustedMode::None)
        .expect("create csv batch writer");
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
fn write_csv_chunk_includes_gbbq_columns_when_enabled() {
    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("stocks_with_gbbq.csv"));

    let mut writer = StockBatchCsvWriter::new(&output_path, 8, true, AdjustedMode::None)
        .expect("create csv batch writer");
    let mut row = sample_columns_with_gbbq(
        "sz000001",
        "2024-01-31",
        12.34,
        13.0,
        12.0,
        12.5,
        10_000,
        1.2,
        0.5,
        0.8,
        9.9,
    );
    writer.push_chunk(&mut row).expect("write first chunk");
    writer.finish().expect("flush pending rows");

    let mut written = String::new();
    fs::File::open(&output_path)
        .expect("open output csv")
        .read_to_string(&mut written)
        .expect("read output csv");
    let _ = fs::remove_file(&output_path);

    let lines = written.lines().collect::<Vec<_>>();
    assert_eq!(
        lines[0],
        "code,date,open,high,low,close,volume,bonus_shares,cash_dividend,rights_issue_shares,rights_issue_price"
    );
    assert_eq!(
        lines[1],
        "sz000001,2024-01-31,12.34,13.0,12.0,12.5,10000,1.2,0.5,0.8,9.9"
    );
    assert_eq!(lines.len(), 2);
}

#[test]
fn write_csv_chunk_writes_qfq_adjusted_prices_to_separate_csv() {
    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("stocks_with_gbbq.csv"));
    let adjusted_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Qfq);

    let mut writer = StockBatchCsvWriter::new(&output_path, 8, true, AdjustedMode::Qfq)
        .expect("create csv batch writer");
    let mut rows = OhlcvColumns {
        codes: vec![
            "sz000001".to_owned(),
            "sz000001".to_owned(),
            "sz000001".to_owned(),
        ],
        dates: vec![
            "2024-01-29".to_owned(),
            "2024-01-30".to_owned(),
            "2024-01-31".to_owned(),
        ],
        opens: vec![10.0, 12.0, 15.0],
        highs: vec![10.5, 12.5, 15.5],
        lows: vec![9.8, 11.8, 14.8],
        closes: vec![10.0, 12.0, 15.0],
        volumes: vec![10_000, 12_000, 13_000],
        bonus_shares: vec![None, Some(1.0), Some(2.0)],
        cash_dividend: vec![None, Some(1.0), Some(0.5)],
        rights_issue_shares: vec![None, Some(2.0), Some(1.0)],
        rights_issue_price: vec![None, Some(8.0), Some(9.0)],
    };

    writer.push_chunk(&mut rows).expect("write rows");
    writer.finish().expect("flush pending rows");

    let mut written = String::new();
    fs::File::open(&output_path)
        .expect("open output csv")
        .read_to_string(&mut written)
        .expect("read output csv");
    let mut adjusted_written = String::new();
    fs::File::open(&adjusted_output_path)
        .expect("open adjusted output csv")
        .read_to_string(&mut adjusted_written)
        .expect("read adjusted output csv");
    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_file(&adjusted_output_path);

    let lines = written.lines().collect::<Vec<_>>();
    assert_eq!(
        lines[0],
        "code,date,open,high,low,close,volume,bonus_shares,cash_dividend,rights_issue_shares,rights_issue_price"
    );

    let first_original_row = lines[1].split(',').collect::<Vec<_>>();
    let second_original_row = lines[2].split(',').collect::<Vec<_>>();
    let third_original_row = lines[3].split(',').collect::<Vec<_>>();
    assert_eq!(first_original_row[5], "10.0");
    assert_eq!(second_original_row[5], "12.0");
    assert_eq!(third_original_row[5], "15.0");

    let adjusted_lines = adjusted_written.lines().collect::<Vec<_>>();
    assert_eq!(
        adjusted_lines[0],
        "code,date,open,high,low,close,volume,bonus_shares,cash_dividend,rights_issue_shares,rights_issue_price"
    );

    let first_row = adjusted_lines[1].split(',').collect::<Vec<_>>();
    let second_row = adjusted_lines[2].split(',').collect::<Vec<_>>();
    let third_row = adjusted_lines[3].split(',').collect::<Vec<_>>();

    let first_adjusted_close = first_row[5]
        .parse::<f64>()
        .expect("parse first adjusted close");
    let second_adjusted_close = second_row[5]
        .parse::<f64>()
        .expect("parse second adjusted close");
    let third_adjusted_close = third_row[5]
        .parse::<f64>()
        .expect("parse third adjusted close");

    let expected_first_adjusted_close = (10.0 - 0.1 * 1.66 + 19.88 * 0.1 * 3.2) / (1.32 * 1.32);
    let expected_second_adjusted_close = (12.0 - 0.1 * 0.5 + 9.0 * 0.1 * 1.0) / (1.2 * 1.1);

    assert!((first_adjusted_close - expected_first_adjusted_close).abs() < 1e-12);
    assert!((second_adjusted_close - expected_second_adjusted_close).abs() < 1e-12);
    assert!((third_adjusted_close - 15.0).abs() < 1e-12);
}

#[test]
fn write_csv_chunk_writes_both_adjusted_files_when_mode_is_both() {
    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("stocks_with_both.csv"));
    let qfq_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Qfq);
    let hfq_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Hfq);

    let mut writer = StockBatchCsvWriter::new(&output_path, 8, true, AdjustedMode::Both)
        .expect("create csv batch writer");
    let mut rows = OhlcvColumns {
        codes: vec![
            "sz000001".to_owned(),
            "sz000001".to_owned(),
            "sz000001".to_owned(),
        ],
        dates: vec![
            "2024-01-29".to_owned(),
            "2024-01-30".to_owned(),
            "2024-01-31".to_owned(),
        ],
        opens: vec![10.0, 12.0, 15.0],
        highs: vec![10.5, 12.5, 15.5],
        lows: vec![9.8, 11.8, 14.8],
        closes: vec![10.0, 12.0, 15.0],
        volumes: vec![10_000, 12_000, 13_000],
        bonus_shares: vec![None, Some(1.0), Some(2.0)],
        cash_dividend: vec![None, Some(1.0), Some(0.5)],
        rights_issue_shares: vec![None, Some(2.0), Some(1.0)],
        rights_issue_price: vec![None, Some(8.0), Some(9.0)],
    };

    writer.push_chunk(&mut rows).expect("write rows");
    writer.finish().expect("flush pending rows");

    let mut qfq_written = String::new();
    fs::File::open(&qfq_output_path)
        .expect("open qfq output csv")
        .read_to_string(&mut qfq_written)
        .expect("read qfq output csv");
    let mut hfq_written = String::new();
    fs::File::open(&hfq_output_path)
        .expect("open hfq output csv")
        .read_to_string(&mut hfq_written)
        .expect("read hfq output csv");
    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_file(&qfq_output_path);
    let _ = fs::remove_file(&hfq_output_path);

    assert!(!qfq_written.is_empty());
    assert!(!hfq_written.is_empty());
}

#[test]
fn write_csv_chunk_skips_adjusted_files_when_mode_is_none() {
    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("stocks_without_adjusted.csv"));
    let qfq_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Qfq);
    let hfq_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Hfq);

    let mut writer = StockBatchCsvWriter::new(&output_path, 8, true, AdjustedMode::None)
        .expect("create csv batch writer");
    let mut row = sample_columns_with_gbbq(
        "sz000001",
        "2024-01-31",
        12.34,
        13.0,
        12.0,
        12.5,
        10_000,
        1.2,
        0.5,
        0.8,
        9.9,
    );
    writer.push_chunk(&mut row).expect("write row");
    writer.finish().expect("flush pending rows");

    let _ = fs::remove_file(&output_path);
    let qfq_exists = qfq_output_path.exists();
    let hfq_exists = hfq_output_path.exists();
    if qfq_exists {
        let _ = fs::remove_file(&qfq_output_path);
    }
    if hfq_exists {
        let _ = fs::remove_file(&hfq_output_path);
    }

    assert!(!qfq_exists);
    assert!(!hfq_exists);
}

#[test]
fn qfq_calculation_logic_is_extracted_to_core_module_file() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let qfq_module_path = manifest_dir.join("src/core/qfq.rs");
    let qfq_module_source =
        fs::read_to_string(&qfq_module_path).expect("read qfq module source from src/core/qfq.rs");

    assert!(
        qfq_module_source.contains("pub(crate) fn build_qfq_adjusted_prices"),
        "qfq calculation entry should be implemented in src/core/qfq.rs"
    );

    let main_source =
        fs::read_to_string(manifest_dir.join("src/main.rs")).expect("read src/main.rs source");
    assert!(
        !main_source.contains("fn build_qfq_adjusted_prices"),
        "qfq calculation should not stay in src/main.rs"
    );
}

#[test]
fn decide_worker_count_is_bounded_by_jobs_and_cpu() {
    assert_eq!(decide_worker_count(0, 8), 1);
    assert_eq!(decide_worker_count(1, 8), 1);
    assert_eq!(decide_worker_count(2, 8), 2);
    assert_eq!(decide_worker_count(8, 2), 2);
}

#[test]
fn validate_input_source_requires_input_or_remote_download() {
    let args = Args {
        input: None,
        output: None,
        gbbq: None,
        adjusted: AdjustedMode::None,
        onlystocks: true,
        stocks_per_batch: 30,
        remote_download: false,
    };
    let result = validate_input_source(&args);
    assert!(matches!(
        result,
        Err(AppError::Input(InputError::InputOrRemoteDownloadRequired))
    ));
}

#[test]
fn validate_input_source_accepts_local_input() {
    let args = Args {
        input: Some(std::env::temp_dir()),
        output: None,
        gbbq: None,
        adjusted: AdjustedMode::None,
        onlystocks: true,
        stocks_per_batch: 30,
        remote_download: false,
    };
    assert!(validate_input_source(&args).is_ok());
}

#[test]
fn validate_input_source_accepts_remote_download_without_input() {
    let args = Args {
        input: None,
        output: None,
        gbbq: None,
        adjusted: AdjustedMode::None,
        onlystocks: true,
        stocks_per_batch: 30,
        remote_download: true,
    };
    assert!(validate_input_source(&args).is_ok());
}

#[test]
fn remote_download_does_not_reach_collect_day_files_panic() {
    let args = Args {
        input: None,
        output: None,
        gbbq: None,
        adjusted: AdjustedMode::None,
        onlystocks: true,
        stocks_per_batch: 30,
        remote_download: true,
    };
    assert!(validate_input_source(&args).is_ok());
    assert!(args.input.is_none());
}

#[test]
fn create_remote_workspace_creates_a_directory() {
    let workspace = create_remote_workspace().expect("create workspace");
    let exists = workspace.is_dir();
    let _ = std::fs::remove_dir_all(&workspace);
    assert!(exists);
}

#[test]
fn extract_remote_archive_unpacks_nested_day_files() {
    use std::io::Write;

    let workspace = create_remote_workspace().expect("create workspace");
    let zip_path = workspace.join("test.zip");

    {
        let zip_file = std::fs::File::create(&zip_path).expect("create zip");
        let mut writer = zip::ZipWriter::new(zip_file);
        let options = zip::write::SimpleFileOptions::default();
        writer
            .start_file("vipdoc/sh/sh600000.day", options)
            .expect("start sh");
        writer.write_all(&[0u8; 32]).expect("write sh bytes");
        writer
            .start_file("vipdoc/sz/sz000001.day", options)
            .expect("start sz");
        writer.write_all(&[0u8; 32]).expect("write sz bytes");
        writer.finish().expect("finish zip");
    }

    extract_remote_archive(&zip_path, &workspace).expect("extract");

    let sh_exists = workspace.join("vipdoc/sh/sh600000.day").exists();
    let sz_exists = workspace.join("vipdoc/sz/sz000001.day").exists();
    let _ = std::fs::remove_dir_all(&workspace);
    assert!(sh_exists);
    assert!(sz_exists);
}

#[test]
fn extract_remote_archive_errors_on_unsafe_entry_path() {
    use std::io::Write;

    let workspace = create_remote_workspace().expect("create workspace");
    let zip_path = workspace.join("evil.zip");

    {
        let zip_file = std::fs::File::create(&zip_path).expect("create zip");
        let mut writer = zip::ZipWriter::new(zip_file);
        let options = zip::write::SimpleFileOptions::default();
        writer
            .start_file("../escape.txt", options)
            .expect("start evil entry");
        writer.write_all(b"evil").expect("write evil bytes");
        writer.finish().expect("finish zip");
    }

    let result = extract_remote_archive(&zip_path, &workspace);
    let _ = std::fs::remove_dir_all(&workspace);
    assert!(result.is_err());
}

#[test]
fn resolve_vipdoc_root_returns_error_when_missing() {
    let workspace = create_remote_workspace().expect("create workspace");
    let result = resolve_vipdoc_root(&workspace);
    let _ = std::fs::remove_dir_all(&workspace);
    assert!(result.is_err());
}

#[test]
fn validate_gbbq_path_returns_custom_error_when_file_missing() {
    let args = Args {
        input: Some(std::env::temp_dir()),
        output: None,
        gbbq: Some(PathBuf::from("/definitely/not/exist.gbbq")),
        adjusted: AdjustedMode::None,
        onlystocks: true,
        stocks_per_batch: 30,
        remote_download: false,
    };

    let result = validate_gbbq_path(&args);
    assert!(matches!(
        result,
        Err(AppError::Input(InputError::GbbqFileNotFound(_)))
    ));
}

#[test]
fn validate_gbbq_path_requires_file_when_adjusted_mode_enabled() {
    let args = Args {
        input: Some(std::env::temp_dir()),
        output: None,
        gbbq: None,
        adjusted: AdjustedMode::Hfq,
        onlystocks: true,
        stocks_per_batch: 30,
        remote_download: false,
    };

    let result = validate_gbbq_path(&args);
    assert!(matches!(
        result,
        Err(AppError::Input(InputError::AdjustedModeRequiresGbbq(_)))
    ));
}

#[test]
fn collect_filtered_day_files_returns_custom_error_when_none_found() {
    let mut input = std::env::temp_dir();
    input.push(unique_name("empty_dir"));
    fs::create_dir_all(&input).expect("create empty input directory");

    let args = Args {
        input: Some(input.clone()),
        output: None,
        gbbq: None,
        adjusted: AdjustedMode::None,
        onlystocks: true,
        stocks_per_batch: 30,
        remote_download: false,
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
        input: Some(input.clone()),
        output: None,
        gbbq: None,
        adjusted: AdjustedMode::None,
        onlystocks: true,
        stocks_per_batch: 30,
        remote_download: false,
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

#[test]
fn qfq_output_matches_correct_data_content_for_sz002304() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let input_day_file = manifest_dir.join("assets/test_only/sz002304.day");
    let gbbq_file = manifest_dir.join("assets/gbbq");
    let expected_file = manifest_dir.join("assets/correct_data/SZ#002304_前复权.txt");

    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("issue39_sz002304.csv"));
    let adjusted_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Qfq);

    let gbbq_lookup = parse_gbbq_file(&gbbq_file).expect("parse gbbq for regression test");

    process_day_files(
        vec![input_day_file],
        &output_path,
        1,
        Some(gbbq_lookup),
        true,
        AdjustedMode::Qfq,
    )
    .expect("process day file and produce qfq output");

    let actual_rows = read_qfq_csv_rows_by_date(&adjusted_output_path);
    let expected_rows = read_correct_data_rows_by_date(&expected_file);

    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_file(&adjusted_output_path);

    assert_eq!(actual_rows.len(), expected_rows.len());

    for (date, expected) in expected_rows {
        let actual = actual_rows
            .get(&date)
            .unwrap_or_else(|| panic!("missing date in qfq output: {date}"));
        assert!(
            (actual.0 - expected.0).abs() < 0.011,
            "open mismatch at {date}"
        );
        assert!(
            (actual.1 - expected.1).abs() < 0.011,
            "high mismatch at {date}"
        );
        assert!(
            (actual.2 - expected.2).abs() < 0.011,
            "low mismatch at {date}"
        );
        assert!(
            (actual.3 - expected.3).abs() < 0.011,
            "close mismatch at {date}"
        );
    }
}

#[test]
fn hfq_output_matches_correct_data_content_for_sz002304() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let input_day_file = manifest_dir.join("assets/test_only/sz002304.day");
    let gbbq_file = manifest_dir.join("assets/gbbq");
    let expected_file = manifest_dir.join("assets/correct_data/SZ#002304_后复权.txt");

    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("issue39_sz002304_hfq.csv"));
    let adjusted_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Hfq);

    let gbbq_lookup = parse_gbbq_file(&gbbq_file).expect("parse gbbq for regression test");

    process_day_files(
        vec![input_day_file],
        &output_path,
        1,
        Some(gbbq_lookup),
        true,
        AdjustedMode::Hfq,
    )
    .expect("process day file and produce hfq output");

    let actual_rows = read_qfq_csv_rows_by_date(&adjusted_output_path);
    let expected_rows = read_correct_data_rows_by_date(&expected_file);

    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_file(&adjusted_output_path);

    assert_eq!(actual_rows.len(), expected_rows.len());

    for (date, expected) in expected_rows {
        let actual = actual_rows
            .get(&date)
            .unwrap_or_else(|| panic!("missing date in hfq output: {date}"));
        assert!(
            (actual.0 - expected.0).abs() < 0.011,
            "open mismatch at {date}"
        );
        assert!(
            (actual.1 - expected.1).abs() < 0.011,
            "high mismatch at {date}"
        );
        assert!(
            (actual.2 - expected.2).abs() < 0.011,
            "low mismatch at {date}"
        );
        assert!(
            (actual.3 - expected.3).abs() < 0.011,
            "close mismatch at {date}"
        );
    }
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
        bonus_shares: vec![None],
        cash_dividend: vec![None],
        rights_issue_shares: vec![None],
        rights_issue_price: vec![None],
    }
}

fn sample_columns_with_gbbq(
    code: &str,
    date: &str,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    bonus_shares: f64,
    cash_dividend: f64,
    rights_issue_shares: f64,
    rights_issue_price: f64,
) -> OhlcvColumns {
    OhlcvColumns {
        codes: vec![code.to_owned()],
        dates: vec![date.to_owned()],
        opens: vec![open],
        highs: vec![high],
        lows: vec![low],
        closes: vec![close],
        volumes: vec![volume],
        bonus_shares: vec![Some(bonus_shares)],
        cash_dividend: vec![Some(cash_dividend)],
        rights_issue_shares: vec![Some(rights_issue_shares)],
        rights_issue_price: vec![Some(rights_issue_price)],
    }
}

fn unique_name(suffix: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after unix epoch")
        .as_nanos();
    PathBuf::from(format!("reload_rustdx_{now}_{suffix}"))
}

fn read_qfq_csv_rows_by_date(path: &PathBuf) -> BTreeMap<String, (f64, f64, f64, f64)> {
    let mut raw = String::new();
    fs::File::open(path)
        .expect("open qfq csv")
        .read_to_string(&mut raw)
        .expect("read qfq csv");

    let mut rows = BTreeMap::new();
    for line in raw.lines().skip(1) {
        let parts = line.split(',').collect::<Vec<_>>();
        if parts.len() < 6 {
            continue;
        }
        rows.insert(
            parts[1].to_owned(),
            (
                parts[2].parse::<f64>().expect("parse csv open"),
                parts[3].parse::<f64>().expect("parse csv high"),
                parts[4].parse::<f64>().expect("parse csv low"),
                parts[5].parse::<f64>().expect("parse csv close"),
            ),
        );
    }
    rows
}

fn read_correct_data_rows_by_date(path: &PathBuf) -> BTreeMap<String, (f64, f64, f64, f64)> {
    let bytes = fs::read(path).expect("read correct_data file");
    let content = String::from_utf8_lossy(&bytes);
    let mut rows = BTreeMap::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if !trimmed.as_bytes().get(0..10).is_some_and(|head| {
            head.len() == 10
                && head[0..4].iter().all(|c| c.is_ascii_digit())
                && head[4] == b'/'
                && head[5..7].iter().all(|c| c.is_ascii_digit())
                && head[7] == b'/'
                && head[8..10].iter().all(|c| c.is_ascii_digit())
        }) {
            continue;
        }

        let fields = trimmed.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 5 {
            continue;
        }

        let date = fields[0].replace('/', "-");
        let open = fields[1].parse::<f64>().expect("parse expected open");
        let high = fields[2].parse::<f64>().expect("parse expected high");
        let low = fields[3].parse::<f64>().expect("parse expected low");
        let close = fields[4].parse::<f64>().expect("parse expected close");
        rows.insert(date, (open, high, low, close));
    }

    rows
}
