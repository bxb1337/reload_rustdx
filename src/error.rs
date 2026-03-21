use polars::error::PolarsError;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug)]
pub enum AppError {
    Input(InputError),
    Parse(ParseError),
    Runtime(RuntimeError),
    Output(OutputError),
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum InputError {
    InputOrRemoteDownloadRequired,
    GbbqFileNotFound(PathBuf),
    AdjustedModeRequiresGbbq(String),
    NoDayFilesFound(PathBuf),
    InputFileNotDay(PathBuf),
    InputPathNotFound(PathBuf),
    InvalidFileNameUtf8(PathBuf),
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum ParseError {
    InvalidDayFileSize { path: PathBuf, size: usize },
    InvalidRecordBytes { offset: usize },
    InvalidDate { raw: u32 },
    ParseDayFile { path: PathBuf, reason: String },
    ParseGbbqFile { path: PathBuf, reason: String },
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum RuntimeError {
    ReadDir {
        path: PathBuf,
        source: std::io::Error,
    },
    ReadDirEntry {
        path: PathBuf,
        source: std::io::Error,
    },
    ReadDayFile {
        path: PathBuf,
        source: std::io::Error,
    },
    DownloadFailed {
        url: String,
        reason: String,
    },
    CreateTempDir {
        path: PathBuf,
        source: std::io::Error,
    },
    CreateDownloadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    ExtractArchive {
        path: PathBuf,
        reason: String,
    },
    CleanupTempDir {
        path: PathBuf,
        source: std::io::Error,
    },
    CurrentDir(std::io::Error),
    ReceiveWorkerResult(String),
    WorkerThreadPanicked,
}

#[derive(Debug)]
pub enum OutputError {
    OpenOutput {
        path: PathBuf,
        source: std::io::Error,
    },
    BuildDataFrame(PolarsError),
    WriteCsv {
        path: PathBuf,
        source: PolarsError,
    },
}

impl From<InputError> for AppError {
    fn from(value: InputError) -> Self {
        Self::Input(value)
    }
}

impl From<ParseError> for AppError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
    }
}

impl From<RuntimeError> for AppError {
    fn from(value: RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl From<OutputError> for AppError {
    fn from(value: OutputError) -> Self {
        Self::Output(value)
    }
}

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Input(inner) => Display::fmt(inner, f),
            Self::Parse(inner) => Display::fmt(inner, f),
            Self::Runtime(inner) => Display::fmt(inner, f),
            Self::Output(inner) => Display::fmt(inner, f),
        }
    }
}

impl Display for InputError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InputOrRemoteDownloadRequired => write!(
                f,
                "Either --input <PATH> or --remote-download must be provided. Use --help for usage."
            ),
            Self::GbbqFileNotFound(path) => write!(
                f,
                "GBBQ file '{}' was not found. Please check the path or omit --gbbq if you do not need it.",
                path.display()
            ),
            Self::AdjustedModeRequiresGbbq(mode) => write!(
                f,
                "Adjusted mode '{mode}' requires --gbbq <PATH>. Please provide a valid GBBQ file or set --adjusted none."
            ),
            Self::NoDayFilesFound(path) => write!(
                f,
                "No .day files were found under '{}'. Please provide a .day file or a directory containing .day files.",
                path.display()
            ),
            Self::InputFileNotDay(path) => write!(
                f,
                "Input path '{}' is a file, but not a .day file. Please provide a .day file or a directory that contains .day files.",
                path.display()
            ),
            Self::InputPathNotFound(path) => write!(
                f,
                "Input path '{}' does not exist. Please check the path and try again.",
                path.display()
            ),
            Self::InvalidFileNameUtf8(path) => write!(
                f,
                "Could not read stock code from file name '{}'. Please ensure the file name is valid UTF-8.",
                path.display()
            ),
        }
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDayFileSize { path, size } => write!(
                f,
                "Failed to parse '{}': file size is {} bytes, but a valid .day file must be a multiple of 32 bytes.",
                path.display(),
                size
            ),
            Self::InvalidRecordBytes { offset } => {
                write!(f, "invalid record: missing bytes at offset {offset}")
            }
            Self::InvalidDate { raw } => write!(
                f,
                "Invalid date value '{raw}' in .day record. Expected format is YYYYMMDD (for example: 20240131)."
            ),
            Self::ParseDayFile { path, reason } => {
                write!(f, "Failed to parse '{}': {reason}", path.display())
            }
            Self::ParseGbbqFile { path, reason } => {
                write!(
                    f,
                    "Failed to parse gbbq file '{}': {reason}",
                    path.display()
                )
            }
        }
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadDir { path, source } => {
                write!(f, "Failed to read directory '{}': {source}", path.display())
            }
            Self::ReadDirEntry { path, source } => write!(
                f,
                "Failed to read directory entry in '{}': {source}",
                path.display()
            ),
            Self::ReadDayFile { path, source } => {
                write!(f, "Failed to read .day file '{}': {source}", path.display())
            }
            Self::DownloadFailed { url, reason } => {
                write!(f, "Failed to download '{url}': {reason}")
            }
            Self::CreateTempDir { path, source } => write!(
                f,
                "Failed to create temporary workspace at '{}': {source}",
                path.display()
            ),
            Self::CreateDownloadFile { path, source } => write!(
                f,
                "Failed to create download file at '{}': {source}",
                path.display()
            ),
            Self::ExtractArchive { path, reason } => {
                write!(
                    f,
                    "Failed to extract archive '{}': {reason}",
                    path.display()
                )
            }
            Self::CleanupTempDir { path, source } => write!(
                f,
                "Failed to clean up temporary workspace '{}': {source}",
                path.display()
            ),
            Self::CurrentDir(source) => {
                write!(f, "Failed to determine current working directory: {source}")
            }
            Self::ReceiveWorkerResult(reason) => {
                write!(f, "Failed to receive worker result: {reason}")
            }
            Self::WorkerThreadPanicked => {
                write!(f, "A worker thread panicked while parsing .day files.")
            }
        }
    }
}

impl Display for OutputError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenOutput { path, source } => write!(
                f,
                "Failed to open output CSV '{}' for writing: {source}",
                path.display()
            ),
            Self::BuildDataFrame(source) => write!(f, "Failed to build output dataframe: {source}"),
            Self::WriteCsv { path, source } => {
                write!(
                    f,
                    "Failed to write CSV data to '{}': {source}",
                    path.display()
                )
            }
        }
    }
}

impl Error for AppError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Input(inner) => inner.source(),
            Self::Parse(inner) => inner.source(),
            Self::Runtime(inner) => inner.source(),
            Self::Output(inner) => inner.source(),
        }
    }
}

impl Error for InputError {}

impl Error for ParseError {}

impl Error for RuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReadDir { source, .. } => Some(source),
            Self::ReadDirEntry { source, .. } => Some(source),
            Self::ReadDayFile { source, .. } => Some(source),
            Self::CreateTempDir { source, .. } => Some(source),
            Self::CreateDownloadFile { source, .. } => Some(source),
            Self::CleanupTempDir { source, .. } => Some(source),
            Self::CurrentDir(source) => Some(source),
            _ => None,
        }
    }
}

impl Error for OutputError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::OpenOutput { source, .. } => Some(source),
            Self::BuildDataFrame(source) => Some(source),
            Self::WriteCsv { source, .. } => Some(source),
        }
    }
}
