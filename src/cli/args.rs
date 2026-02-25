use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum AdjustedMode {
    None,
    Qfq,
    Hfq,
    Both,
}

impl AdjustedMode {
    pub const fn requires_gbbq(self) -> bool {
        !matches!(self, Self::None)
    }

    pub const fn includes_qfq(self) -> bool {
        matches!(self, Self::Qfq | Self::Both)
    }

    pub const fn includes_hfq(self) -> bool {
        matches!(self, Self::Hfq | Self::Both)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Qfq => "qfq",
            Self::Hfq => "hfq",
            Self::Both => "both",
        }
    }
}

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

    #[arg(long, value_enum, default_value_t = AdjustedMode::None)]
    pub adjusted: AdjustedMode,

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
