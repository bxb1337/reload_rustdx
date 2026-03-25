use super::{
    collect_filtered_day_files, dataframe_from_columns, decide_worker_count, process_day_files,
    resolve_adjusted_output_path, validate_gbbq_path, validate_input_source, StockBatchCsvWriter,
};
use crate::cli::args::AdjustedMode;
use crate::cli::Args;
use crate::core::hfq::build_hfq_adjusted_prices;
use crate::core::qfq::build_qfq_adjusted_prices;
use crate::core::tdx_day::{parse_day_file_into_columns, OhlcvColumns};
use crate::core::tdx_gbbq::parse_gbbq_file;
use crate::download::{create_remote_workspace, extract_remote_archive, resolve_vipdoc_root};
use crate::error::{AppError, InputError};
use polars::prelude::DataFrame;
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
fn raw_output_matches_parsed_day_rows_for_sz002304() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let input_day_file = manifest_dir.join("test/assets/sz002304.day");

    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("sz002304_raw_fixture.csv"));

    process_day_files(
        vec![input_day_file.clone()],
        &output_path,
        1,
        None,
        true,
        AdjustedMode::None,
    )
    .expect("process day file and produce raw output");

    let expected_rows = read_day_file_rows_by_date(&input_day_file);
    let actual_rows = read_output_rows_by_date(&output_path);

    assert_semantic_rows_match(&actual_rows, &expected_rows, "raw");

    let _ = fs::remove_file(&output_path);
}

#[test]
fn provided_unadjusted_and_qfq_fixtures_are_identical_for_sz002304() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let unadjusted = manifest_dir.join("test/correct_result/SZ#002304_不复权.txt");
    let qfq = manifest_dir.join("test/correct_result/SZ#002304_前复权.txt");

    assert_eq!(
        read_fixture_rows_by_date(&unadjusted),
        read_fixture_rows_by_date(&qfq)
    );
}

#[test]
fn qfq_output_remains_distinct_from_available_qfq_fixture_for_sz002304() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let input_day_file = manifest_dir.join("test/assets/sz002304.day");
    let gbbq_file = manifest_dir.join("test/assets/gbbq");
    let expected_file = manifest_dir.join("test/correct_result/SZ#002304_前复权.txt");

    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("sz002304_qfq_fixture.csv"));
    let adjusted_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Qfq);

    let gbbq_lookup = parse_gbbq_file(&gbbq_file).expect("parse gbbq for qfq fixture regression");

    process_day_files(
        vec![input_day_file.clone()],
        &output_path,
        1,
        Some(gbbq_lookup),
        true,
        AdjustedMode::Qfq,
    )
    .expect("process day file and produce qfq output");

    assert_ne!(
        read_output_rows_by_date(&adjusted_output_path),
        read_fixture_rows_by_date(&expected_file)
    );

    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_file(&adjusted_output_path);
}

#[test]
fn hfq_output_matches_in_memory_adjustment_for_sz002304() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let input_day_file = manifest_dir.join("test/assets/sz002304.day");
    let gbbq_file = manifest_dir.join("test/assets/gbbq");

    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("sz002304_hfq_fixture.csv"));
    let adjusted_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Hfq);

    let gbbq_lookup = parse_gbbq_file(&gbbq_file).expect("parse gbbq for hfq fixture regression");

    process_day_files(
        vec![input_day_file.clone()],
        &output_path,
        1,
        Some(gbbq_lookup),
        true,
        AdjustedMode::Hfq,
    )
    .expect("process day file and produce hfq output");

    let expected_rows =
        build_adjusted_rows_by_date(&input_day_file, Some(&gbbq_file), AdjustedMode::Hfq);
    let actual_rows = read_output_rows_by_date(&adjusted_output_path);

    assert_semantic_rows_match(&actual_rows, &expected_rows, "hfq");

    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_file(&adjusted_output_path);
}

#[test]
fn qfq_output_matches_in_memory_adjustment_for_sz002304() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let input_day_file = manifest_dir.join("test/assets/sz002304.day");
    let gbbq_file = manifest_dir.join("test/assets/gbbq");

    let mut output_path = std::env::temp_dir();
    output_path.push(unique_name("sz002304_qfq_adjustment.csv"));
    let adjusted_output_path = resolve_adjusted_output_path(&output_path, AdjustedMode::Qfq);

    let gbbq_lookup = parse_gbbq_file(&gbbq_file).expect("parse gbbq for qfq integration test");

    process_day_files(
        vec![input_day_file.clone()],
        &output_path,
        1,
        Some(gbbq_lookup),
        true,
        AdjustedMode::Qfq,
    )
    .expect("process day file and produce qfq output");

    let expected_rows =
        build_adjusted_rows_by_date(&input_day_file, Some(&gbbq_file), AdjustedMode::Qfq);
    let actual_rows = read_output_rows_by_date(&adjusted_output_path);

    assert_semantic_rows_match(&actual_rows, &expected_rows, "qfq");

    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_file(&adjusted_output_path);
}

#[test]
fn parse_fixture_rows_normalizes_dates_and_ignores_turnover_amount() {
    let fixture = "header\nmeta\n2010/06/09\t8.62\t12.69\t8.60\t11.93\t2234936\t359529440.00\n";

    let rows = parse_fixture_rows(fixture);

    assert_eq!(
        rows,
        vec![SemanticRow {
            date: "2010-06-09".to_owned(),
            open: 8.62,
            high: 12.69,
            low: 8.60,
            close: 11.93,
            volume: 2_234_936,
        }]
    );
}

#[test]
fn parse_output_csv_rows_keeps_semantic_columns_and_ignores_gbbq_tail() {
    let csv = concat!(
        "code,date,open,high,low,close,volume,bonus_shares,cash_dividend,rights_issue_shares,rights_issue_price\n",
        "sz002304,2010-06-09,8.62,12.69,8.60,11.93,2234936,1.0,0.5,0.0,9.0\n"
    );

    let rows = parse_output_csv_rows(csv);

    assert_eq!(
        rows,
        vec![SemanticRow {
            date: "2010-06-09".to_owned(),
            open: 8.62,
            high: 12.69,
            low: 8.60,
            close: 11.93,
            volume: 2_234_936,
        }]
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

#[derive(Debug, Clone, PartialEq)]
struct SemanticRow {
    date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
}

fn unique_name(suffix: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after unix epoch")
        .as_nanos();
    PathBuf::from(format!("reload_rustdx_{now}_{suffix}"))
}

fn parse_output_csv_rows(csv: &str) -> Vec<SemanticRow> {
    csv.lines()
        .skip(1)
        .filter_map(|line| {
            let parts = line.split(',').collect::<Vec<_>>();
            (parts.len() >= 7).then(|| SemanticRow {
                date: parts[1].to_owned(),
                open: parts[2].parse::<f64>().expect("parse csv open"),
                high: parts[3].parse::<f64>().expect("parse csv high"),
                low: parts[4].parse::<f64>().expect("parse csv low"),
                close: parts[5].parse::<f64>().expect("parse csv close"),
                volume: parts[6].parse::<i64>().expect("parse csv volume"),
            })
        })
        .collect()
}

fn parse_fixture_rows(content: &str) -> Vec<SemanticRow> {
    content
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if !trimmed.as_bytes().get(0..10).is_some_and(|head| {
                head.len() == 10
                    && head[0..4].iter().all(|c| c.is_ascii_digit())
                    && head[4] == b'/'
                    && head[5..7].iter().all(|c| c.is_ascii_digit())
                    && head[7] == b'/'
                    && head[8..10].iter().all(|c| c.is_ascii_digit())
            }) {
                return None;
            }

            let fields = trimmed.split_whitespace().collect::<Vec<_>>();
            (fields.len() >= 6).then(|| SemanticRow {
                date: fields[0].replace('/', "-"),
                open: fields[1].parse::<f64>().expect("parse fixture open"),
                high: fields[2].parse::<f64>().expect("parse fixture high"),
                low: fields[3].parse::<f64>().expect("parse fixture low"),
                close: fields[4].parse::<f64>().expect("parse fixture close"),
                volume: fields[5].parse::<i64>().expect("parse fixture volume"),
            })
        })
        .collect()
}

fn assert_semantic_rows_match(
    actual_rows: &BTreeMap<String, SemanticRow>,
    expected_rows: &BTreeMap<String, SemanticRow>,
    mode: &str,
) {
    assert_eq!(
        actual_rows.len(),
        expected_rows.len(),
        "{mode} row count mismatch"
    );

    for (date, expected) in expected_rows {
        let actual = actual_rows
            .get(date)
            .unwrap_or_else(|| panic!("missing date in {mode} output: {date}"));

        assert_eq!(
            actual.volume, expected.volume,
            "volume mismatch at {mode}:{date}"
        );
        assert!(
            (actual.open - expected.open).abs() < 1e-12,
            "open mismatch at {mode}:{date}"
        );
        assert!(
            (actual.high - expected.high).abs() < 1e-12,
            "high mismatch at {mode}:{date}"
        );
        assert!(
            (actual.low - expected.low).abs() < 1e-12,
            "low mismatch at {mode}:{date}"
        );
        assert!(
            (actual.close - expected.close).abs() < 1e-12,
            "close mismatch at {mode}:{date}"
        );
    }
}

fn read_day_file_rows_by_date(path: &PathBuf) -> BTreeMap<String, SemanticRow> {
    let mut columns = OhlcvColumns::default();
    parse_day_file_into_columns(path, &mut columns, None).expect("parse day file into columns");
    semantic_rows_by_date_from_columns(columns)
}

fn build_adjusted_rows_by_date(
    day_path: &PathBuf,
    gbbq_path: Option<&PathBuf>,
    mode: AdjustedMode,
) -> BTreeMap<String, SemanticRow> {
    let gbbq_lookup =
        gbbq_path.map(|path| parse_gbbq_file(path).expect("parse gbbq for expected rows"));
    let mut columns = OhlcvColumns::default();
    parse_day_file_into_columns(day_path, &mut columns, gbbq_lookup.as_ref())
        .expect("parse day file with gbbq into columns");

    let dataframe = dataframe_from_columns(columns, true).expect("build dataframe from columns");
    let adjusted = match mode {
        AdjustedMode::Qfq => build_qfq_adjusted_prices(dataframe).expect("build qfq dataframe"),
        AdjustedMode::Hfq => build_hfq_adjusted_prices(dataframe).expect("build hfq dataframe"),
        AdjustedMode::Both | AdjustedMode::None => {
            panic!("unexpected mode for adjusted expected rows")
        }
    };

    semantic_rows_by_date_from_dataframe(&adjusted)
}

fn semantic_rows_by_date_from_columns(columns: OhlcvColumns) -> BTreeMap<String, SemanticRow> {
    let mut rows = BTreeMap::new();

    for (((((date, open), high), low), close), volume) in columns
        .dates
        .into_iter()
        .zip(columns.opens.into_iter())
        .zip(columns.highs.into_iter())
        .zip(columns.lows.into_iter())
        .zip(columns.closes.into_iter())
        .zip(columns.volumes.into_iter())
    {
        rows.insert(
            date.clone(),
            SemanticRow {
                date,
                open,
                high,
                low,
                close,
                volume,
            },
        );
    }

    rows
}

fn semantic_rows_by_date_from_dataframe(df: &DataFrame) -> BTreeMap<String, SemanticRow> {
    let dates = df
        .column("date")
        .expect("date column")
        .str()
        .expect("date as str");
    let opens = df
        .column("open")
        .expect("open column")
        .f64()
        .expect("open as f64");
    let highs = df
        .column("high")
        .expect("high column")
        .f64()
        .expect("high as f64");
    let lows = df
        .column("low")
        .expect("low column")
        .f64()
        .expect("low as f64");
    let closes = df
        .column("close")
        .expect("close column")
        .f64()
        .expect("close as f64");
    let volumes = df
        .column("volume")
        .expect("volume column")
        .i64()
        .expect("volume as i64");

    let mut rows = BTreeMap::new();
    for idx in 0..df.height() {
        let date = dates.get(idx).expect("date exists").to_owned();
        rows.insert(
            date.clone(),
            SemanticRow {
                date,
                open: opens.get(idx).expect("open exists"),
                high: highs.get(idx).expect("high exists"),
                low: lows.get(idx).expect("low exists"),
                close: closes.get(idx).expect("close exists"),
                volume: volumes.get(idx).expect("volume exists"),
            },
        );
    }
    rows
}

fn assert_semantic_output_matches_fixture(
    output_path: &PathBuf,
    fixture_path: &PathBuf,
    mode: &str,
) {
    let actual_rows = read_output_rows_by_date(output_path);
    let expected_rows = read_fixture_rows_by_date(fixture_path);

    assert_eq!(
        actual_rows.len(),
        expected_rows.len(),
        "{mode} row count mismatch"
    );

    for (date, expected) in expected_rows {
        let actual = actual_rows
            .get(&date)
            .unwrap_or_else(|| panic!("missing date in {mode} output: {date}"));

        assert_eq!(
            actual.volume, expected.volume,
            "volume mismatch at {mode}:{date}"
        );
        assert!(
            (actual.open - expected.open).abs() < 0.011,
            "open mismatch at {mode}:{date}"
        );
        assert!(
            (actual.high - expected.high).abs() < 0.011,
            "high mismatch at {mode}:{date}"
        );
        assert!(
            (actual.low - expected.low).abs() < 0.011,
            "low mismatch at {mode}:{date}"
        );
        assert!(
            (actual.close - expected.close).abs() < 0.011,
            "close mismatch at {mode}:{date}"
        );
    }
}

fn assert_semantic_output_matches_fixture_prefix(
    output_path: &PathBuf,
    fixture_path: &PathBuf,
    mode: &str,
    prefix_len: usize,
) {
    let actual_rows = read_output_rows_by_date(output_path);
    let expected_rows = read_fixture_rows_by_date(fixture_path);

    assert_eq!(
        actual_rows.len(),
        expected_rows.len(),
        "{mode} row count mismatch"
    );

    for (date, expected) in expected_rows.into_iter().take(prefix_len) {
        let actual = actual_rows
            .get(&date)
            .unwrap_or_else(|| panic!("missing date in {mode} output: {date}"));

        assert_eq!(
            actual.volume, expected.volume,
            "volume mismatch at {mode}:{date}"
        );
        assert!(
            (actual.open - expected.open).abs() < 0.011,
            "open mismatch at {mode}:{date}"
        );
        assert!(
            (actual.high - expected.high).abs() < 0.011,
            "high mismatch at {mode}:{date}"
        );
        assert!(
            (actual.low - expected.low).abs() < 0.011,
            "low mismatch at {mode}:{date}"
        );
        assert!(
            (actual.close - expected.close).abs() < 0.011,
            "close mismatch at {mode}:{date}"
        );
    }
}

fn read_output_rows_by_date(path: &PathBuf) -> BTreeMap<String, SemanticRow> {
    let mut raw = String::new();
    fs::File::open(path)
        .expect("open output csv")
        .read_to_string(&mut raw)
        .expect("read output csv");

    parse_output_csv_rows(&raw)
        .into_iter()
        .map(|row| (row.date.clone(), row))
        .collect()
}

fn read_fixture_rows_by_date(path: &PathBuf) -> BTreeMap<String, SemanticRow> {
    let bytes = fs::read(path).expect("read fixture file");
    let content = String::from_utf8_lossy(&bytes);

    parse_fixture_rows(&content)
        .into_iter()
        .map(|row| (row.date.clone(), row))
        .collect()
}
