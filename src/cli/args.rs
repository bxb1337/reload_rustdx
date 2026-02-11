use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Input .day file or directory
    #[arg(short, long)]
    pub input: PathBuf,

    /// Optional output file path
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Optional GBBQ file path
    #[arg(short, long)]
    pub gbbq: Option<PathBuf>,

    /// Whether to keep only target A-share stock code prefixes
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub onlystocks: bool,

    /// Maximum number of stocks to buffer per CSV write batch
    #[arg(long, default_value_t = 30)]
    pub stocks_per_batch: usize,
}

#[cfg(test)]
#[path = "args_tests.rs"]
mod tests;
