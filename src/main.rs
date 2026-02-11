use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
mod cli;
mod core;
mod error;

use cli::Args;
use core::tdx_day::{
    OhlcvColumns, collect_day_files, is_target_stock_code, parse_day_file_into_columns,
};
use error::{AppError, AppResult, InputError, OutputError, ParseError, RuntimeError};
use polars::prelude::{CsvWriter, DataFrame, NamedFrom, SerWriter, Series};
use std::collections::VecDeque;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Instant;

struct ParseMessage {
    path: PathBuf,
    result: Result<OhlcvColumns, String>,
}

struct StockBatchCsvWriter {
    output: File,
    output_path: PathBuf,
    include_header: bool,
    max_stocks_per_batch: usize,
    pending_stocks: usize,
    buffered: OhlcvColumns,
}

impl StockBatchCsvWriter {
    fn new(output_path: &Path, max_stocks_per_batch: usize) -> AppResult<Self> {
        let output = File::create(output_path).map_err(|source| OutputError::OpenOutput {
            path: output_path.to_path_buf(),
            source,
        })?;
        Ok(Self {
            output,
            output_path: output_path.to_path_buf(),
            include_header: true,
            max_stocks_per_batch: max_stocks_per_batch.max(1),
            pending_stocks: 0,
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

        let mut df = dataframe_from_columns(std::mem::take(&mut self.buffered))?;
        CsvWriter::new(&mut self.output)
            .include_header(self.include_header)
            .finish(&mut df)
            .map_err(|source| OutputError::WriteCsv {
                path: self.output_path.clone(),
                source,
            })?;
        self.include_header = false;
        self.pending_stocks = 0;
        Ok(())
    }
}

fn main() -> AppResult<()> {
    let start_time = Instant::now();
    let args = Args::parse();
    validate_gbbq_path(&args)?;
    let day_files = collect_filtered_day_files(&args)?;
    let output_path = resolve_output_path(&args)?;

    process_day_files(day_files, output_path.as_path(), args.stocks_per_batch)?;

    println!(
        "Processing complete. Elapsed time: {:?}",
        start_time.elapsed()
    );

    Ok(())
}

fn validate_gbbq_path(args: &Args) -> AppResult<()> {
    if let Some(path) = args.gbbq.as_ref()
        && !path.exists()
    {
        return Err(InputError::GbbqFileNotFound(path.clone()).into());
    }
    Ok(())
}

fn collect_filtered_day_files(args: &Args) -> AppResult<Vec<PathBuf>> {
    let mut day_files = collect_day_files(args.input.as_path()).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            AppError::Input(InputError::InputPathNotFound(args.input.clone()))
        } else if source.kind() == std::io::ErrorKind::InvalidInput {
            AppError::Input(InputError::InputFileNotDay(args.input.clone()))
        } else {
            AppError::Runtime(RuntimeError::ReadDir {
                path: args.input.clone(),
                source,
            })
        }
    })?;
    if args.onlystocks {
        day_files.retain(|path| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .is_some_and(is_target_stock_code)
        });
    }

    if day_files.is_empty() {
        return Err(InputError::NoDayFilesFound(args.input.clone()).into());
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

fn process_day_files(
    day_files: Vec<PathBuf>,
    output_path: &Path,
    stocks_per_batch: usize,
) -> AppResult<()> {
    let total_jobs = day_files.len();
    let available_parallelism = thread::available_parallelism().map_or(1, |n| n.get());
    let worker_count = decide_worker_count(total_jobs, available_parallelism);

    let progress = create_progress_bar(total_jobs as u64);
    let (rx, handles) = spawn_parser_workers(day_files, worker_count);
    let processing_result =
        receive_and_write_results(total_jobs, rx, output_path, &progress, stocks_per_batch);
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
) -> (mpsc::Receiver<ParseMessage>, Vec<thread::JoinHandle<()>>) {
    let (tx, rx) = mpsc::sync_channel::<ParseMessage>(worker_count.saturating_mul(2).max(1));
    let jobs = Arc::new(Mutex::new(VecDeque::from(day_files)));
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let tx = tx.clone();
        let jobs = Arc::clone(&jobs);
        let handle = thread::spawn(move || {
            loop {
                let next_path = match jobs.lock() {
                    Ok(mut queue) => queue.pop_front(),
                    Err(_) => None,
                };

                let Some(path) = next_path else {
                    break;
                };

                let mut columns = OhlcvColumns::default();
                let result = parse_day_file_into_columns(&path, &mut columns)
                    .map(|_| columns)
                    .map_err(|err| err.to_string());
                if tx.send(ParseMessage { path, result }).is_err() {
                    break;
                }
            }
        });
        handles.push(handle);
    }

    drop(tx);
    (rx, handles)
}

fn receive_and_write_results(
    total_jobs: usize,
    rx: mpsc::Receiver<ParseMessage>,
    output_path: &Path,
    progress: &ProgressBar,
    stocks_per_batch: usize,
) -> AppResult<()> {
    let mut writer = StockBatchCsvWriter::new(output_path, stocks_per_batch)?;

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

fn decide_worker_count(total_jobs: usize, available_parallelism: usize) -> usize {
    if total_jobs == 0 {
        return 1;
    }
    total_jobs.min(available_parallelism.max(1))
}

fn dataframe_from_columns(columns: OhlcvColumns) -> AppResult<DataFrame> {
    let rows = columns.codes.len();
    Ok(DataFrame::new(
        rows,
        vec![
            Series::new("code".into(), columns.codes).into(),
            Series::new("date".into(), columns.dates).into(),
            Series::new("open".into(), columns.opens).into(),
            Series::new("high".into(), columns.highs).into(),
            Series::new("low".into(), columns.lows).into(),
            Series::new("close".into(), columns.closes).into(),
            Series::new("volume".into(), columns.volumes).into(),
        ],
    )
    .map_err(OutputError::BuildDataFrame)?)
}

fn append_columns(target: &mut OhlcvColumns, mut source: OhlcvColumns) {
    target.codes.append(&mut source.codes);
    target.dates.append(&mut source.dates);
    target.opens.append(&mut source.opens);
    target.highs.append(&mut source.highs);
    target.lows.append(&mut source.lows);
    target.closes.append(&mut source.closes);
    target.volumes.append(&mut source.volumes);
}

#[cfg(test)]
#[path = "main_tests.rs"]
mod tests;
