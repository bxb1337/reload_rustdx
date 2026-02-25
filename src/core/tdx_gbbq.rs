use crate::core::tdx_gbbq_key::KEY;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GbbqRecord {
    pub bonus_shares: f64,
    pub cash_dividend: f64,
    pub rights_issue_shares: f64,
    pub rights_issue_price: f64,
}

pub type GbbqLookup = HashMap<String, HashMap<String, GbbqRecord>>;

pub fn parse_gbbq_file(path: &Path) -> Result<GbbqLookup, String> {
    let mut bytes = fs::read(path)
        .map_err(|err| format!("Failed to read gbbq file '{}': {err}", path.display()))?;
    if bytes.len() < 4 {
        return Err(format!(
            "Failed to parse '{}': file is too short to contain gbbq header.",
            path.display()
        ));
    }

    let declared_count = u32_from_le_bytes(&bytes, 0).ok_or_else(|| {
        format!(
            "Failed to parse '{}': invalid gbbq count header.",
            path.display()
        )
    })? as usize;
    let payload_size = bytes.len() - 4;
    if payload_size % 29 != 0 {
        return Err(format!(
            "Failed to parse '{}': gbbq payload size {} is not divisible by 29 bytes.",
            path.display(),
            payload_size
        ));
    }
    let actual_count = payload_size / 29;
    if declared_count != actual_count {
        return Err(format!(
            "Failed to parse '{}': gbbq header count {} does not match payload record count {}.",
            path.display(),
            declared_count,
            actual_count
        ));
    }

    let mut lookup = GbbqLookup::new();
    for encrypted in bytes[4..].chunks_exact_mut(29) {
        let chunk = decrypt_gbbq_record(encrypted);
        let row = parse_record_chunk(chunk)
            .ok_or_else(|| format!("Failed to parse '{}': invalid gbbq record.", path.display()))?;
        if row.category != 1 {
            continue;
        }

        let Some(code) = prefixed_code(row.market, &row.code) else {
            continue;
        };
        let date = format_date(row.date)?;
        lookup.entry(code).or_default().insert(
            date,
            GbbqRecord {
                bonus_shares: f64::from(row.sg_hltp),
                cash_dividend: f64::from(row.fh_qltp),
                rights_issue_shares: f64::from(row.pg_hzgb),
                rights_issue_price: f64::from(row.pgj_qzgb),
            },
        );
    }

    Ok(lookup)
}

#[derive(Debug)]
struct RawGbbq {
    market: u8,
    code: String,
    date: u32,
    category: u8,
    fh_qltp: f32,
    pgj_qzgb: f32,
    sg_hltp: f32,
    pg_hzgb: f32,
}

fn parse_record_chunk(chunk: &[u8]) -> Option<RawGbbq> {
    let code = std::str::from_utf8(chunk.get(1..7)?).ok()?.to_owned();
    Some(RawGbbq {
        market: *chunk.first()?,
        code,
        date: u32_from_le_bytes(chunk, 8)?,
        category: *chunk.get(12)?,
        fh_qltp: f32_from_le_bytes(chunk, 13)?,
        pgj_qzgb: f32_from_le_bytes(chunk, 17)?,
        sg_hltp: f32_from_le_bytes(chunk, 21)?,
        pg_hzgb: f32_from_le_bytes(chunk, 25)?,
    })
}

fn decrypt_gbbq_record(encrypted: &mut [u8]) -> &[u8] {
    let mut pos = 0_usize;
    for i in (0_usize..24).step_by(8) {
        let mut eax = u32_from_le_bytes(KEY, 0x44).unwrap_or_default();
        let mut ebx = u32_from_le_bytes(encrypted, pos).unwrap_or_default();
        let mut num = eax ^ ebx;
        let mut numold = u32_from_le_bytes(encrypted, pos + 4).unwrap_or_default();
        for j in (4_usize..68).step_by(4).rev() {
            ebx = (num & 0xff0000) >> 16;
            eax = u32_from_le_bytes(KEY, ebx as usize * 4 + 0x448).unwrap_or_default();
            ebx = num >> 24;
            let mut eax_add = u32_from_le_bytes(KEY, ebx as usize * 4 + 0x48).unwrap_or_default();
            eax = eax.overflowing_add(eax_add).0;
            ebx = (num & 0xff00) >> 8;
            let mut eax_xor = u32_from_le_bytes(KEY, ebx as usize * 4 + 0x848).unwrap_or_default();
            eax ^= eax_xor;
            ebx = num & 0xff;
            eax_add = u32_from_le_bytes(KEY, ebx as usize * 4 + 0xc48).unwrap_or_default();
            eax = eax.overflowing_add(eax_add).0;
            eax_xor = u32_from_le_bytes(KEY, j).unwrap_or_default();
            eax ^= eax_xor;
            ebx = num;
            num = numold ^ eax;
            numold = ebx;
        }
        numold ^= u32_from_le_bytes(KEY, 0).unwrap_or_default();
        encrypted[i..i + 4].copy_from_slice(&numold.to_le_bytes());
        encrypted[i + 4..i + 8].copy_from_slice(&num.to_le_bytes());
        pos += 8;
    }
    encrypted
}

fn u32_from_le_bytes(chunk: &[u8], start: usize) -> Option<u32> {
    let bytes = chunk.get(start..start + 4)?;
    let arr: [u8; 4] = bytes.try_into().ok()?;
    Some(u32::from_le_bytes(arr))
}

fn f32_from_le_bytes(chunk: &[u8], start: usize) -> Option<f32> {
    let bits = u32_from_le_bytes(chunk, start)?;
    Some(f32::from_bits(bits))
}

fn prefixed_code(market: u8, code: &str) -> Option<String> {
    match code.chars().next() {
        Some('6') => Some(format!("sh{code}")),
        Some('0' | '3') => Some(format!("sz{code}")),
        _ => match market {
            1 => Some(format!("sh{code}")),
            0 => Some(format!("sz{code}")),
            _ => None,
        },
    }
}

fn format_date(raw: u32) -> Result<String, String> {
    let year = raw / 10000;
    let month = raw % 10000 / 100;
    let day = raw % 100;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(format!(
            "Invalid date value '{raw}' in gbbq record. Expected format is YYYYMMDD (for example: 20240131)."
        ));
    }

    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

#[cfg(test)]
mod tests {
    use super::{GbbqRecord, parse_gbbq_file, parse_record_chunk};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parse_gbbq_file_returns_error_for_invalid_payload_size() {
        let mut path = std::env::temp_dir();
        path.push(unique_name("invalid.gbbq"));
        fs::write(&path, vec![0_u8; 5]).expect("write invalid gbbq payload");

        let result = parse_gbbq_file(&path);
        let _ = fs::remove_file(&path);

        assert!(result.is_err());
    }

    #[test]
    fn parse_gbbq_file_returns_error_for_count_mismatch() {
        let mut path = std::env::temp_dir();
        path.push(unique_name("count_mismatch.gbbq"));

        let mut bytes = Vec::with_capacity(4 + 29);
        bytes.extend_from_slice(&2_u32.to_le_bytes());
        bytes.extend_from_slice(&[0_u8; 29]);
        fs::write(&path, bytes).expect("write invalid gbbq count");

        let result = parse_gbbq_file(&path);
        let _ = fs::remove_file(&path);

        assert!(result.is_err());
    }

    #[test]
    fn parse_record_chunk_reads_plain_layout_fields() {
        let mut chunk = [0_u8; 29];
        chunk[0] = 0;
        chunk[1..7].copy_from_slice(b"000001");
        chunk[8..12].copy_from_slice(&20240131_u32.to_le_bytes());
        chunk[12] = 1;
        chunk[13..17].copy_from_slice(&0.5_f32.to_le_bytes());
        chunk[17..21].copy_from_slice(&9.9_f32.to_le_bytes());
        chunk[21..25].copy_from_slice(&1.2_f32.to_le_bytes());
        chunk[25..29].copy_from_slice(&0.8_f32.to_le_bytes());

        let row = parse_record_chunk(&chunk).expect("parse record chunk");
        let extracted = GbbqRecord {
            bonus_shares: f64::from(row.sg_hltp),
            cash_dividend: f64::from(row.fh_qltp),
            rights_issue_shares: f64::from(row.pg_hzgb),
            rights_issue_price: f64::from(row.pgj_qzgb),
        };

        assert_eq!(row.market, 0);
        assert_eq!(row.code, "000001");
        assert_eq!(row.date, 20240131);
        assert_eq!(row.category, 1);
        assert!((extracted.bonus_shares - 1.2).abs() < 1e-6);
        assert!((extracted.cash_dividend - 0.5).abs() < 1e-6);
        assert!((extracted.rights_issue_shares - 0.8).abs() < 1e-6);
        assert!((extracted.rights_issue_price - 9.9).abs() < 1e-6);
    }

    fn unique_name(suffix: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is after unix epoch")
            .as_nanos();
        PathBuf::from(format!("reload_rustdx_{now}_{suffix}"))
    }
}
