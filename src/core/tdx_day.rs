use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct OhlcvRow {
    pub code: String,
    pub date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
}

#[derive(Debug, Default)]
pub struct OhlcvColumns {
    pub codes: Vec<String>,
    pub dates: Vec<String>,
    pub opens: Vec<f64>,
    pub highs: Vec<f64>,
    pub lows: Vec<f64>,
    pub closes: Vec<f64>,
    pub volumes: Vec<i64>,
}

impl OhlcvColumns {
    fn reserve(&mut self, additional: usize) {
        self.codes.reserve(additional);
        self.dates.reserve(additional);
        self.opens.reserve(additional);
        self.highs.reserve(additional);
        self.lows.reserve(additional);
        self.closes.reserve(additional);
        self.volumes.reserve(additional);
    }

    fn push_row(&mut self, row: OhlcvRow) {
        self.codes.push(row.code);
        self.dates.push(row.date);
        self.opens.push(row.open);
        self.highs.push(row.high);
        self.lows.push(row.low);
        self.closes.push(row.close);
        self.volumes.push(row.volume);
    }
}

pub fn collect_day_files(input: &Path) -> Result<Vec<PathBuf>, io::Error> {
    if input.is_file() {
        if input.extension().and_then(|s| s.to_str()) == Some("day") {
            return Ok(vec![input.to_path_buf()]);
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Input path '{}' is a file, but not a .day file. Please provide a .day file or a directory that contains .day files.",
                input.display()
            ),
        ));
    }

    if input.is_dir() {
        let mut day_files = Vec::new();
        collect_day_files_in_dir(input, &mut day_files)?;
        day_files.sort();
        return Ok(day_files);
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "Input path '{}' does not exist. Please check the path and try again.",
            input.display()
        ),
    ))
}

fn collect_day_files_in_dir(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), io::Error> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_day_files_in_dir(&path, files)?;
            continue;
        }

        if path.extension().and_then(|s| s.to_str()) == Some("day") {
            files.push(path);
        }
    }

    Ok(())
}

#[cfg(test)]
pub fn parse_day_file(path: &Path) -> Result<Vec<OhlcvRow>, Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    if bytes.len() % 32 != 0 {
        return Err(format!(
            "Failed to parse '{}': file size is {} bytes, but a valid .day file must be a multiple of 32 bytes.",
            path.display(),
            bytes.len()
        )
        .into());
    }
    let code = code_from_path(path)?;

    let rows = bytes
        .chunks_exact(32)
        .enumerate()
        .map(|(idx, chunk)| {
            parse_day_record(&code, chunk).map_err(|err| {
                format!(
                    "Failed to parse '{}' at record #{}: {err}",
                    path.display(),
                    idx + 1
                )
                .into()
            })
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

    Ok(rows)
}

pub fn parse_day_file_into_columns(
    path: &Path,
    columns: &mut OhlcvColumns,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    if bytes.len() % 32 != 0 {
        return Err(format!(
            "Failed to parse '{}': file size is {} bytes, but a valid .day file must be a multiple of 32 bytes.",
            path.display(),
            bytes.len()
        )
        .into());
    }
    let code = code_from_path(path)?;
    let records = bytes.len() / 32;
    columns.reserve(records);

    for (idx, chunk) in bytes.chunks_exact(32).enumerate() {
        let row = parse_day_record(&code, chunk).map_err(|err| {
            format!(
                "Failed to parse '{}' at record #{}: {err}",
                path.display(),
                idx + 1
            )
        })?;
        columns.push_row(row);
    }

    Ok(())
}

pub fn is_target_stock_code(code: &str) -> bool {
    matches!(
        code,
        c if c.starts_with("sh600")
            || c.starts_with("sh601")
            || c.starts_with("sh603")
            || c.starts_with("sh605")
            || c.starts_with("sh688")
            || c.starts_with("sz000")
            || c.starts_with("sz001")
            || c.starts_with("sz002")
            || c.starts_with("sz003")
            || c.starts_with("sz004")
            || c.starts_with("sz300")
    )
}

fn parse_day_record(code: &str, chunk: &[u8]) -> Result<OhlcvRow, Box<dyn std::error::Error>> {
    let date_raw = u32_from_le_bytes(chunk, 0)?;
    let date = format_date(date_raw)?;

    let open = u32_from_le_bytes(chunk, 4)? as f64 / 100.0;
    let high = u32_from_le_bytes(chunk, 8)? as f64 / 100.0;
    let low = u32_from_le_bytes(chunk, 12)? as f64 / 100.0;
    let close = u32_from_le_bytes(chunk, 16)? as f64 / 100.0;
    let volume = i64::from(u32_from_le_bytes(chunk, 24)?);

    Ok(OhlcvRow {
        code: code.to_owned(),
        date,
        open,
        high,
        low,
        close,
        volume,
    })
}

fn u32_from_le_bytes(chunk: &[u8], start: usize) -> Result<u32, Box<dyn std::error::Error>> {
    let bytes = chunk
        .get(start..start + 4)
        .ok_or_else(|| format!("invalid record: missing bytes at offset {start}"))?;
    let arr: [u8; 4] = bytes
        .try_into()
        .map_err(|_| format!("invalid record: could not read 4 bytes at offset {start}"))?;
    Ok(u32::from_le_bytes(arr))
}

fn format_date(raw: u32) -> Result<String, Box<dyn std::error::Error>> {
    let year = raw / 10000;
    let month = raw % 10000 / 100;
    let day = raw % 100;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(format!(
            "Invalid date value '{raw}' in .day record. Expected format is YYYYMMDD (for example: 20240131)."
        )
        .into());
    }

    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn code_from_path(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let code = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            format!(
                "Could not read stock code from file name '{}'. Please ensure the file name is valid UTF-8.",
                path.display()
            )
        })?;
    Ok(code.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{collect_day_files, parse_day_file, parse_day_file_into_columns, OhlcvColumns};
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

        assert!(result.is_err());
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
        parse_day_file_into_columns(&path, &mut columns).expect("parse day file into columns");

        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir_all(&root);

        assert_eq!(columns.codes, vec!["sz000001".to_owned()]);
        assert_eq!(columns.dates, vec!["2024-01-31".to_owned()]);
        assert_eq!(columns.opens, vec![12.34]);
        assert_eq!(columns.highs, vec![13.0]);
        assert_eq!(columns.lows, vec![12.0]);
        assert_eq!(columns.closes, vec![12.5]);
        assert_eq!(columns.volumes, vec![10_000]);
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
}
