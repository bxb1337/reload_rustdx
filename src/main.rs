use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
mod cli;
mod core;
mod error;

use cli::Args;
use cli::args::AdjustedMode;
use core::hfq::build_hfq_adjusted_prices;
use core::qfq::build_qfq_adjusted_prices;
use core::tdx_day::{
    OhlcvColumns, collect_day_files, is_target_stock_code, parse_day_file_into_columns,
};
use core::tdx_gbbq::{GbbqLookup, parse_gbbq_file};
use error::{AppError, AppResult, InputError, OutputError, ParseError, RuntimeError};
use polars::prelude::{CsvWriter, DataFrame, NamedFrom, SerWriter, Series};
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;
use zip::ZipArchive;

const REMOTE_DAY_ZIP_URL: &str = "https://data.tdx.com.cn/vipdoc/hsjday.zip";

struct ParseMessage {
    path: PathBuf,
    result: Result<OhlcvColumns, String>,
}

struct StockBatchCsvWriter {
    output: File,
    output_path: PathBuf,
    qfq_output: Option<File>,
    qfq_output_path: Option<PathBuf>,
    hfq_output: Option<File>,
    hfq_output_path: Option<PathBuf>,
    include_header: bool,
    include_qfq_header: bool,
    include_hfq_header: bool,
    max_stocks_per_batch: usize,
    pending_stocks: usize,
    include_gbbq: bool,
    adjusted_mode: AdjustedMode,
    buffered: OhlcvColumns,
}

impl StockBatchCsvWriter {
    fn new(
        output_path: &Path,
        max_stocks_per_batch: usize,
        include_gbbq: bool,
        adjusted_mode: AdjustedMode,
    ) -> AppResult<Self> {
        let output = File::create(output_path).map_err(|source| OutputError::OpenOutput {
            path: output_path.to_path_buf(),
            source,
        })?;
        let (qfq_output, qfq_output_path) = if include_gbbq && adjusted_mode.includes_qfq() {
            let qfq_path = resolve_adjusted_output_path(output_path, AdjustedMode::Qfq);
            let qfq_file = File::create(&qfq_path).map_err(|source| OutputError::OpenOutput {
                path: qfq_path.clone(),
                source,
            })?;
            (Some(qfq_file), Some(qfq_path))
        } else {
            (None, None)
        };
        let (hfq_output, hfq_output_path) = if include_gbbq && adjusted_mode.includes_hfq() {
            let hfq_path = resolve_adjusted_output_path(output_path, AdjustedMode::Hfq);
            let hfq_file = File::create(&hfq_path).map_err(|source| OutputError::OpenOutput {
                path: hfq_path.clone(),
                source,
            })?;
            (Some(hfq_file), Some(hfq_path))
        } else {
            (None, None)
        };
        Ok(Self {
            output,
            output_path: output_path.to_path_buf(),
            qfq_output,
            qfq_output_path,
            hfq_output,
            hfq_output_path,
            include_header: true,
            include_qfq_header: true,
            include_hfq_header: true,
            max_stocks_per_batch: max_stocks_per_batch.max(1),
            pending_stocks: 0,
            include_gbbq,
            adjusted_mode,
            buffered: OhlcvColumns::default(),
        })
    }

    fn push_chunk(&mut self, columns: &mut OhlcvColumns) -> AppResult<()> {
        append_columns(&mut self.buffered, std::mem::take(columns));
        self.pending_stocks += 1;
        if self.pending_stocks >= self.max_stocks_per_batch {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> AppResult<()> {
        self.flush()
    }

    fn flush(&mut self) -> AppResult<()> {
        if self.pending_stocks == 0 {
            return Ok(());
        }

        let mut df = dataframe_from_columns(std::mem::take(&mut self.buffered), self.include_gbbq)?;

        // Write raw CSV first (mutable borrow; df still alive after this).
        CsvWriter::new(&mut self.output)
            .include_header(self.include_header)
            .finish(&mut df)
            .map_err(|source| OutputError::WriteCsv {
                path: self.output_path.clone(),
                source,
            })?;
        self.include_header = false;

        // Compute adjusted DataFrames — clone only when both modes are needed.
        let (mut qfq_adjusted_df, mut hfq_adjusted_df) = match (
            self.include_gbbq && self.adjusted_mode.includes_qfq(),
            self.include_gbbq && self.adjusted_mode.includes_hfq(),
        ) {
            (true, true) => {
                // Need both: clone for qfq, move for hfq (saves one full DataFrame clone).
                let qfq = build_qfq_adjusted_prices(df.clone())?;
                let hfq = build_hfq_adjusted_prices(df)?;
                (Some(qfq), Some(hfq))
            }
            (true, false) => (Some(build_qfq_adjusted_prices(df)?), None),
            (false, true) => (None, Some(build_hfq_adjusted_prices(df)?)),
            (false, false) => (None, None),
        };

        if let (Some(adjusted_output), Some(adjusted_path), Some(adjusted_df)) = (
            self.qfq_output.as_mut(),
            self.qfq_output_path.as_ref(),
            qfq_adjusted_df.as_mut(),
        ) {
            CsvWriter::new(adjusted_output)
                .include_header(self.include_qfq_header)
                .finish(adjusted_df)
                .map_err(|source| OutputError::WriteCsv {
                    path: adjusted_path.clone(),
                    source,
                })?;
            self.include_qfq_header = false;
        }

        if let (Some(adjusted_output), Some(adjusted_path), Some(adjusted_df)) = (
            self.hfq_output.as_mut(),
            self.hfq_output_path.as_ref(),
            hfq_adjusted_df.as_mut(),
        ) {
            CsvWriter::new(adjusted_output)
                .include_header(self.include_hfq_header)
                .finish(adjusted_df)
                .map_err(|source| OutputError::WriteCsv {
                    path: adjusted_path.clone(),
                    source,
                })?;
            self.include_hfq_header = false;
        }

        self.pending_stocks = 0;
        Ok(())
    }
}

fn main() -> AppResult<()> {
    let start_time = Instant::now();
    let args = Args::parse();
    validate_input_source(&args)?;
    validate_gbbq_path(&args)?;

    let remote_workspace: Option<PathBuf> = if args.remote_download {
        let workspace = create_remote_workspace()?;
        let zip_path = workspace.join("hsjday.zip");
        download_remote_archive(REMOTE_DAY_ZIP_URL, &zip_path)?;
        println!("Extracting archive...");
        extract_remote_archive(&zip_path, &workspace)?;
        let _ = std::fs::remove_file(&zip_path);
        Some(workspace)
    } else {
        None
    };

    let effective_input = if let Some(ref ws) = remote_workspace {
        ws.clone()
    } else {
        args.input
            .clone()
            .expect("input is required when not using --remote-download")
    };

    let day_files = collect_filtered_day_files_from(&effective_input, args.onlystocks)?;
    let output_path = resolve_output_path(&args)?;
    let gbbq_lookup = load_gbbq_lookup(&args)?;

    process_day_files(
        day_files,
        output_path.as_path(),
        args.stocks_per_batch,
        gbbq_lookup,
        args.gbbq.is_some(),
        args.adjusted,
    )?;

    if let Some(ref ws) = remote_workspace {
        std::fs::remove_dir_all(ws).map_err(|source| RuntimeError::CleanupTempDir {
            path: ws.clone(),
            source,
        })?;
    }

    println!(
        "Processing complete. Elapsed time: {:?}",
        start_time.elapsed()
    );

    Ok(())
}

fn load_gbbq_lookup(args: &Args) -> AppResult<Option<GbbqLookup>> {
    let Some(path) = args.gbbq.as_ref() else {
        return Ok(None);
    };

    parse_gbbq_file(path).map(Some).map_err(|reason| {
        ParseError::ParseGbbqFile {
            path: path.clone(),
            reason,
        }
        .into()
    })
}

fn validate_gbbq_path(args: &Args) -> AppResult<()> {
    if args.adjusted.requires_gbbq() && args.gbbq.is_none() {
        return Err(InputError::AdjustedModeRequiresGbbq(args.adjusted.as_str().to_owned()).into());
    }

    if let Some(path) = args.gbbq.as_ref()
        && !path.exists()
    {
        return Err(InputError::GbbqFileNotFound(path.clone()).into());
    }
    Ok(())
}

fn create_remote_workspace() -> AppResult<PathBuf> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let workspace = std::env::temp_dir().join(format!("reload_rustdx_{nanos}_remote"));
    std::fs::create_dir_all(&workspace).map_err(|source| RuntimeError::CreateTempDir {
        path: workspace.clone(),
        source,
    })?;
    Ok(workspace)
}

fn extract_remote_archive(zip_path: &Path, workspace: &Path) -> AppResult<()> {
    let zip_file = std::fs::File::open(zip_path).map_err(|source| RuntimeError::ReadDayFile {
        path: zip_path.to_path_buf(),
        source,
    })?;
    let mut archive = ZipArchive::new(zip_file).map_err(|e| RuntimeError::ExtractArchive {
        path: zip_path.to_path_buf(),
        reason: e.to_string(),
    })?;

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| RuntimeError::ExtractArchive {
                path: zip_path.to_path_buf(),
                reason: format!("entry {i}: {e}"),
            })?;

        let out_path = match entry.enclosed_name() {
            Some(path) => workspace.join(path),
            None => {
                return Err(RuntimeError::ExtractArchive {
                    path: zip_path.to_path_buf(),
                    reason: format!("unsafe entry path in archive at index {i}"),
                }
                .into());
            }
        };

        if entry.is_dir() {
            std::fs::create_dir_all(&out_path).map_err(|source| RuntimeError::CreateTempDir {
                path: out_path.clone(),
                source,
            })?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent).map_err(|source| RuntimeError::CreateTempDir {
                    path: parent.to_path_buf(),
                    source,
                })?;
            }
            let mut out_file =
                std::fs::File::create(&out_path).map_err(|source| RuntimeError::CreateDownloadFile {
                    path: out_path.clone(),
                    source,
                })?;
            std::io::copy(&mut entry, &mut out_file).map_err(|source| {
                RuntimeError::ExtractArchive {
                    path: out_path.clone(),
                    reason: source.to_string(),
                }
            })?;
        }
    }
    Ok(())
}

fn download_remote_archive(url: &str, dest: &Path) -> AppResult<()> {
    println!("Downloading {url} ...");

    let response = reqwest::blocking::Client::builder()
        .build()
        .and_then(|client| client.get(url).send())
        .map_err(|e| RuntimeError::DownloadFailed {
            url: url.to_owned(),
            reason: e.to_string(),
        })?;

    if !response.status().is_success() {
        return Err(RuntimeError::DownloadFailed {
            url: url.to_owned(),
            reason: format!("HTTP {}", response.status()),
        }
        .into());
    }

    let total_size = response.content_length();
    let progress = if let Some(size) = total_size {
        let pb = ProgressBar::new(size);
        if let Ok(style) = ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.green/white} {bytes}/{total_bytes} ({bytes_per_sec})",
        ) {
            pb.set_style(style.progress_chars("=>-"));
        }
        pb
    } else {
        let pb = ProgressBar::new_spinner();
        if let Ok(style) =
            ProgressStyle::with_template("[{elapsed_precise}] {bytes} ({bytes_per_sec})")
        {
            pb.set_style(style);
        }
        pb
    };

    let mut out_file =
        File::create(dest).map_err(|source| RuntimeError::CreateDownloadFile {
            path: dest.to_path_buf(),
            source,
        })?;

    let mut reader = response;
    let mut buf = [0u8; 8192];
    loop {
        let n = std::io::Read::read(&mut reader, &mut buf).map_err(|e| {
            RuntimeError::DownloadFailed {
                url: url.to_owned(),
                reason: e.to_string(),
            }
        })?;
        if n == 0 {
            break;
        }
        out_file.write_all(&buf[..n]).map_err(|source| RuntimeError::CreateDownloadFile {
            path: dest.to_path_buf(),
            source: std::io::Error::new(source.kind(), source.to_string()),
        })?;
        progress.inc(n as u64);
    }

    progress.finish_and_clear();
    println!("Download complete.");
    Ok(())
}

#[cfg(test)]
fn resolve_vipdoc_root(workspace: &Path) -> AppResult<PathBuf> {
    let vipdoc = workspace.join("vipdoc");
    if vipdoc.is_dir() {
        Ok(vipdoc)
    } else {
        Err(RuntimeError::ExtractArchive {
            path: workspace.to_path_buf(),
            reason: "extracted archive does not contain a 'vipdoc' directory".to_owned(),
        }
        .into())
    }
}

fn validate_input_source(args: &Args) -> AppResult<()> {
    if args.input.is_none() && !args.remote_download {
        return Err(InputError::InputOrRemoteDownloadRequired.into());
    }
    Ok(())
}

#[cfg(test)]
fn collect_filtered_day_files(args: &Args) -> AppResult<Vec<PathBuf>> {
    let input = args
        .input
        .as_ref()
        .expect("input is required when not using --remote-download");
    collect_filtered_day_files_from(input, args.onlystocks)
}

fn collect_filtered_day_files_from(input: &Path, onlystocks: bool) -> AppResult<Vec<PathBuf>> {
    let mut day_files = collect_day_files(input).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            AppError::Input(InputError::InputPathNotFound(input.to_path_buf()))
        } else if source.kind() == std::io::ErrorKind::InvalidInput {
            AppError::Input(InputError::InputFileNotDay(input.to_path_buf()))
        } else {
            AppError::Runtime(RuntimeError::ReadDir {
                path: input.to_path_buf(),
                source,
            })
        }
    })?;
    if onlystocks {
        day_files.retain(|path| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .is_some_and(is_target_stock_code)
        });
    }

    if day_files.is_empty() {
        return Err(InputError::NoDayFilesFound(input.to_path_buf()).into());
    }

    Ok(day_files)
}

fn resolve_output_path(args: &Args) -> AppResult<PathBuf> {
    match args.output.as_ref() {
        Some(path) => Ok(path.clone()),
        None => Ok(std::env::current_dir()
            .map_err(RuntimeError::CurrentDir)?
            .join("stocks.csv")),
    }
}

fn resolve_adjusted_output_path(output_path: &Path, mode: AdjustedMode) -> PathBuf {
    let suffix = match mode {
        AdjustedMode::Qfq => "qfq",
        AdjustedMode::Hfq => "hfq",
        _ => "adjusted",
    };

    let adjusted_file_name = match (
        output_path.file_stem().and_then(|stem| stem.to_str()),
        output_path.extension().and_then(|ext| ext.to_str()),
    ) {
        (Some(stem), Some(ext)) => format!("{stem}_{suffix}.{ext}"),
        (Some(stem), None) => format!("{stem}_{suffix}"),
        _ => format!("stocks_{suffix}.csv"),
    };

    output_path.with_file_name(adjusted_file_name)
}

fn process_day_files(
    day_files: Vec<PathBuf>,
    output_path: &Path,
    stocks_per_batch: usize,
    gbbq_lookup: Option<GbbqLookup>,
    include_gbbq: bool,
    adjusted_mode: AdjustedMode,
) -> AppResult<()> {
    let total_jobs = day_files.len();
    let available_parallelism = thread::available_parallelism().map_or(1, |n| n.get());

    let gbbq_lookup = gbbq_lookup.map(Arc::new);
    let progress = create_progress_bar(total_jobs as u64);
    let (rx, handles) = spawn_parser_workers(day_files, available_parallelism, gbbq_lookup);
    let processing_result = receive_and_write_results(
        total_jobs,
        rx,
        output_path,
        &progress,
        stocks_per_batch,
        include_gbbq,
        adjusted_mode,
    );
    let join_result = join_workers(handles);

    progress.finish_and_clear();
    join_result?;
    processing_result
}

fn create_progress_bar(total_jobs: u64) -> ProgressBar {
    let progress = ProgressBar::new(total_jobs);
    if let Ok(style) =
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
    {
        progress.set_style(style.progress_chars("=>-"));
    }
    progress
}

fn spawn_parser_workers(
    day_files: Vec<PathBuf>,
    worker_count: usize,
    gbbq_lookup: Option<Arc<GbbqLookup>>,
) -> (mpsc::Receiver<ParseMessage>, Vec<thread::JoinHandle<()>>) {
    // Buffer just large enough to keep Rayon workers fed without excessive memory from
    // thousands of in-flight OhlcvColumns.
    let buf_size = (worker_count * 2).max(1);
    let (tx, rx) = mpsc::sync_channel::<ParseMessage>(buf_size);

    // Single coordinator thread drives the Rayon par_iter.
    // Rayon internally spawns worker threads from its global pool.
    let handle = thread::spawn(move || {
        day_files.into_par_iter().for_each_with(tx, |tx, path| {
            let mut columns = OhlcvColumns::default();
            let result = parse_day_file_into_columns(&path, &mut columns, gbbq_lookup.as_deref())
                .map(|_| columns)
                .map_err(|err| err.to_string());
            // Ignore send error: receiver may have exited on fatal write error.
            let _ = tx.send(ParseMessage { path, result });
        });
    });

    (rx, vec![handle])
}

fn receive_and_write_results(
    total_jobs: usize,
    rx: mpsc::Receiver<ParseMessage>,
    output_path: &Path,
    progress: &ProgressBar,
    stocks_per_batch: usize,
    include_gbbq: bool,
    adjusted_mode: AdjustedMode,
) -> AppResult<()> {
    let mut writer =
        StockBatchCsvWriter::new(output_path, stocks_per_batch, include_gbbq, adjusted_mode)?;

    for _ in 0..total_jobs {
        let message = rx
            .recv()
            .map_err(|err| RuntimeError::ReceiveWorkerResult(err.to_string()))?;

        progress.set_message(message.path.display().to_string());
        match message.result {
            Ok(mut columns) => {
                writer.push_chunk(&mut columns)?;
            }
            Err(err) => {
                return Err(ParseError::ParseDayFile {
                    path: message.path,
                    reason: err,
                }
                .into());
            }
        }
        progress.inc(1);
    }

    writer.finish()
}

fn join_workers(handles: Vec<thread::JoinHandle<()>>) -> AppResult<()> {
    for handle in handles {
        if handle.join().is_err() {
            return Err(RuntimeError::WorkerThreadPanicked.into());
        }
    }
    Ok(())
}

#[cfg(test)]
fn decide_worker_count(total_jobs: usize, available_parallelism: usize) -> usize {
    if total_jobs == 0 {
        return 1;
    }
    total_jobs.min(available_parallelism.max(1))
}

fn dataframe_from_columns(columns: OhlcvColumns, include_gbbq: bool) -> AppResult<DataFrame> {
    let rows = columns.codes.len();
    let mut cols = vec![
        Series::new("code".into(), columns.codes).into(),
        Series::new("date".into(), columns.dates).into(),
        Series::new("open".into(), columns.opens).into(),
        Series::new("high".into(), columns.highs).into(),
        Series::new("low".into(), columns.lows).into(),
        Series::new("close".into(), columns.closes).into(),
        Series::new("volume".into(), columns.volumes).into(),
    ];
    if include_gbbq {
        cols.push(Series::new("bonus_shares".into(), columns.bonus_shares).into());
        cols.push(Series::new("cash_dividend".into(), columns.cash_dividend).into());
        cols.push(Series::new("rights_issue_shares".into(), columns.rights_issue_shares).into());
        cols.push(Series::new("rights_issue_price".into(), columns.rights_issue_price).into());
    }

    DataFrame::new(rows, cols)
        .map_err(OutputError::BuildDataFrame)
        .map_err(Into::into)
}

fn append_columns(target: &mut OhlcvColumns, mut source: OhlcvColumns) {
    target.codes.append(&mut source.codes);
    target.dates.append(&mut source.dates);
    target.opens.append(&mut source.opens);
    target.highs.append(&mut source.highs);
    target.lows.append(&mut source.lows);
    target.closes.append(&mut source.closes);
    target.volumes.append(&mut source.volumes);
    target.bonus_shares.append(&mut source.bonus_shares);
    target.cash_dividend.append(&mut source.cash_dividend);
    target
        .rights_issue_shares
        .append(&mut source.rights_issue_shares);
    target
        .rights_issue_price
        .append(&mut source.rights_issue_price);
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
