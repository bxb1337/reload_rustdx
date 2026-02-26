use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::core::tdx_gbbq::{GbbqLookup, GbbqRecord};

#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct OhlcvRow {
    pub code: String,
    pub date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: i64,
    pub bonus_shares: Option<f64>,
    pub cash_dividend: Option<f64>,
    pub rights_issue_shares: Option<f64>,
    pub rights_issue_price: Option<f64>,
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
    pub bonus_shares: Vec<Option<f64>>,
    pub cash_dividend: Vec<Option<f64>>,
    pub rights_issue_shares: Vec<Option<f64>>,
    pub rights_issue_price: Vec<Option<f64>>,
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
        self.bonus_shares.reserve(additional);
        self.cash_dividend.reserve(additional);
        self.rights_issue_shares.reserve(additional);
        self.rights_issue_price.reserve(additional);
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
    use rayon::prelude::*;

    let mut subdirs: Vec<PathBuf> = Vec::new();
    let mut local_files: Vec<PathBuf> = Vec::new();

    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            subdirs.push(path);
        } else if path.extension().and_then(|s| s.to_str()) == Some("day") {
            local_files.push(path);
        }
    }

    files.append(&mut local_files);

    if !subdirs.is_empty() {
        // Scan subdirectories in parallel; each collects into its own Vec.
        let sub_results: Vec<Result<Vec<PathBuf>, io::Error>> = subdirs
            .into_par_iter()
            .map(|sub| {
                let mut sub_files = Vec::new();
                collect_day_files_in_dir(&sub, &mut sub_files)?;
                Ok(sub_files)
            })
            .collect();

        for result in sub_results {
            files.append(&mut result?);
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
            parse_day_record(&code, chunk, None).map_err(|err| {
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

struct DayRecordFields {
    date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: i64,
    bonus_shares: Option<f64>,
    cash_dividend: Option<f64>,
    rights_issue_shares: Option<f64>,
    rights_issue_price: Option<f64>,
}

fn parse_day_record_fields(
    chunk: &[u8],
    gbbq_by_date: Option<&HashMap<String, GbbqRecord>>,
) -> Result<DayRecordFields, Box<dyn std::error::Error>> {
    let date_raw = u32_from_le_bytes(chunk, 0)?;
    let date = format_date(date_raw)?;
    let open = u32_from_le_bytes(chunk, 4)? as f64 / 100.0;
    let high = u32_from_le_bytes(chunk, 8)? as f64 / 100.0;
    let low = u32_from_le_bytes(chunk, 12)? as f64 / 100.0;
    let close = u32_from_le_bytes(chunk, 16)? as f64 / 100.0;
    let volume = i64::from(u32_from_le_bytes(chunk, 24)?);
    let gbbq_row = gbbq_by_date.and_then(|by_date| by_date.get(&date));

    Ok(DayRecordFields {
        date,
        open,
        high,
        low,
        close,
        volume,
        bonus_shares: gbbq_row.map(|row| row.bonus_shares),
        cash_dividend: gbbq_row.map(|row| row.cash_dividend),
        rights_issue_shares: gbbq_row.map(|row| row.rights_issue_shares),
        rights_issue_price: gbbq_row.map(|row| row.rights_issue_price),
    })
}

pub fn parse_day_file_into_columns(
    path: &Path,
    columns: &mut OhlcvColumns,
    gbbq_lookup: Option<&GbbqLookup>,
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
    let gbbq_by_date = gbbq_lookup.and_then(|by_code| by_code.get(code.as_str()));
    let records = bytes.len() / 32;
    columns.reserve(records);

    for (idx, chunk) in bytes.chunks_exact(32).enumerate() {
        let row = parse_day_record_fields(chunk, gbbq_by_date).map_err(|err| {
            format!(
                "Failed to parse '{}' at record #{}: {err}",
                path.display(),
                idx + 1
            )
        })?;
        columns.codes.push(code.clone());
        columns.dates.push(row.date);
        columns.opens.push(row.open);
        columns.highs.push(row.high);
        columns.lows.push(row.low);
        columns.closes.push(row.close);
        columns.volumes.push(row.volume);
        columns.bonus_shares.push(row.bonus_shares);
        columns.cash_dividend.push(row.cash_dividend);
        columns.rights_issue_shares.push(row.rights_issue_shares);
        columns.rights_issue_price.push(row.rights_issue_price);
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

#[cfg(test)]
fn parse_day_record(
    code: &str,
    chunk: &[u8],
    gbbq_lookup: Option<&GbbqLookup>,
) -> Result<OhlcvRow, Box<dyn std::error::Error>> {
    let date_raw = u32_from_le_bytes(chunk, 0)?;
    let date = format_date(date_raw)?;

    let open = u32_from_le_bytes(chunk, 4)? as f64 / 100.0;
    let high = u32_from_le_bytes(chunk, 8)? as f64 / 100.0;
    let low = u32_from_le_bytes(chunk, 12)? as f64 / 100.0;
    let close = u32_from_le_bytes(chunk, 16)? as f64 / 100.0;
    let volume = i64::from(u32_from_le_bytes(chunk, 24)?);
    let gbbq_row = gbbq_lookup
        .and_then(|by_code| by_code.get(code))
        .and_then(|by_date| by_date.get(&date));

    Ok(OhlcvRow {
        code: code.to_owned(),
        date,
        open,
        high,
        low,
        close,
        volume,
        bonus_shares: gbbq_row.map(|row| row.bonus_shares),
        cash_dividend: gbbq_row.map(|row| row.cash_dividend),
        rights_issue_shares: gbbq_row.map(|row| row.rights_issue_shares),
        rights_issue_price: gbbq_row.map(|row| row.rights_issue_price),
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

    let y = year as usize;
    let m = month as usize;
    let d = day as usize;
    let buf: [u8; 10] = [
        b'0' + (y / 1000) as u8,
        b'0' + (y / 100 % 10) as u8,
        b'0' + (y / 10 % 10) as u8,
        b'0' + (y % 10) as u8,
        b'-',
        b'0' + (m / 10) as u8,
        b'0' + (m % 10) as u8,
        b'-',
        b'0' + (d / 10) as u8,
        b'0' + (d % 10) as u8,
    ];

    Ok(String::from_utf8(buf.to_vec()).expect("ASCII date bytes are always valid UTF-8"))
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
    use super::{
        OhlcvColumns, collect_day_files, format_date, parse_day_file, parse_day_file_into_columns,
    };
    use crate::core::tdx_gbbq::{GbbqLookup, GbbqRecord};
    use std::collections::HashMap;
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
        parse_day_file_into_columns(&path, &mut columns, None)
            .expect("parse day file into columns");

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
    fn parse_day_file_into_columns_merges_matching_gbbq_record() {
        let mut root = std::env::temp_dir();
        root.push(unique_name("dir"));
        fs::create_dir_all(&root).expect("create temp directory");

        let mut path = root.clone();
        path.push("sz000001.day");
        fs::write(&path, valid_day_record()).expect("write temp day file");

        let mut columns = OhlcvColumns::default();
        let mut by_date = HashMap::new();
        by_date.insert(
            "2024-01-31".to_owned(),
            GbbqRecord {
                bonus_shares: 1.2,
                cash_dividend: 0.5,
                rights_issue_shares: 0.8,
                rights_issue_price: 9.9,
            },
        );
        let mut lookup = GbbqLookup::new();
        lookup.insert("sz000001".to_owned(), by_date);

        parse_day_file_into_columns(&path, &mut columns, Some(&lookup))
            .expect("parse day file into columns");

        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir_all(&root);

        assert_eq!(columns.bonus_shares, vec![Some(1.2)]);
        assert_eq!(columns.cash_dividend, vec![Some(0.5)]);
        assert_eq!(columns.rights_issue_shares, vec![Some(0.8)]);
        assert_eq!(columns.rights_issue_price, vec![Some(9.9)]);
    }

    #[test]
    fn format_date_stack_buffer_matches_original_output() {
        let result = format_date(20240131).expect("valid date");
        assert_eq!(result, "2024-01-31");

        let result2 = format_date(19900101).expect("valid date");
        assert_eq!(result2, "1990-01-01");

        let result3 = format_date(20001231).expect("valid date");
        assert_eq!(result3, "2000-12-31");

        assert!(format_date(20241301).is_err());
        assert!(format_date(20240132).is_err());
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
