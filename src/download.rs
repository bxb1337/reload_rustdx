use crate::error::{AppResult, RuntimeError};
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use zip::ZipArchive;

pub const REMOTE_DAY_ZIP_URL: &str = "https://data.tdx.com.cn/vipdoc/hsjday.zip";

/// Maximum number of concurrent download segments.
const MAX_SEGMENTS: usize = 4;

/// Minimum size per segment in bytes (4 MiB).
const MIN_SEGMENT_BYTES: u64 = 4 * 1024 * 1024;

/// Half-open byte range `[start, end)` for deterministic segment planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ByteRange {
    /// Inclusive start offset (0-indexed).
    pub start: u64,
    /// Exclusive end offset.
    pub end: u64,
}

impl ByteRange {
    /// Creates a new half-open byte range.
    ///
    /// # Panics
    /// Panics if `start > end`.
    fn new(start: u64, end: u64) -> Self {
        assert!(start <= end, "ByteRange start must not exceed end");
        Self { start, end }
    }

    /// Converts to HTTP Range header value (inclusive end convention).
    /// Returns `"bytes=start-end"` where end is inclusive.
    fn to_http_range_header(self) -> String {
        format!("bytes={}-{}", self.start, self.end.saturating_sub(1))
    }
}

/// A single download segment with its assigned byte range and output path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Segment {
    /// The byte range this segment should download.
    pub range: ByteRange,
    /// The part-file path for this segment's output.
    pub part_path: PathBuf,
    /// Zero-based segment index.
    pub index: usize,
}

/// Result of segment planning: either fall back to single-stream or use segmented download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SegmentPlan {
    /// Archive too small or unsuitable for segmentation; use existing single-stream path.
    SingleStream,
    /// Archive suitable for segmented download with planned segments.
    Segmented { segments: Vec<Segment> },
}

/// Plans deterministic byte ranges and part-file layout for a known archive size.
///
/// # Rules
/// - Maximum 4 segments
/// - Minimum 4 MiB per segment
/// - Falls back to `SingleStream` when archive size < 2 * MIN_SEGMENT_BYTES (8 MiB)
///
/// # Part-file Naming
/// Part files are named alongside the destination ZIP:
/// - `hsjday.zip.part0`, `hsjday.zip.part1`, ..., `hsjday.zip.partN`
pub(crate) fn plan_segments(archive_size: u64, dest_zip: &Path) -> SegmentPlan {
    let min_size_for_segmentation = 2 * MIN_SEGMENT_BYTES;
    if archive_size < min_size_for_segmentation {
        return SegmentPlan::SingleStream;
    }

    let segment_count = compute_segment_count(archive_size);
    let segments = build_segments(archive_size, dest_zip, segment_count);

    SegmentPlan::Segmented { segments }
}

fn compute_segment_count(archive_size: u64) -> usize {
    let by_min_size = (archive_size / MIN_SEGMENT_BYTES) as usize;
    by_min_size.clamp(1, MAX_SEGMENTS)
}

fn build_segments(archive_size: u64, dest_zip: &Path, segment_count: usize) -> Vec<Segment> {
    let mut segments = Vec::with_capacity(segment_count);

    let base_size = archive_size / segment_count as u64;
    let remainder = archive_size % segment_count as u64;

    let mut offset: u64 = 0;

    for i in 0..segment_count {
        // Distribute remainder bytes across first N segments for even coverage.
        let segment_size = if i < remainder as usize {
            base_size + 1
        } else {
            base_size
        };

        let start = offset;
        let end = offset + segment_size;

        let range = ByteRange::new(start, end);
        let part_path = part_file_path(dest_zip, i);

        segments.push(Segment {
            range,
            part_path,
            index: i,
        });

        offset = end;
    }

    debug_assert_eq!(offset, archive_size, "Segments must exactly cover archive");
    segments
}

fn part_file_path(dest_zip: &Path, index: usize) -> PathBuf {
    let zip_name = dest_zip
        .file_name()
        .expect("dest_zip must have a file name");
    let part_name = format!("{}.part{}", zip_name.to_string_lossy(), index);
    dest_zip.with_file_name(part_name)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DownloadStrategy {
    MissingContentLength,
    MissingRangeSupport,
    UnsupportedProbeResponse,
    ProbeRequestFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeOutcome {
    SegmentedSupported { content_length: u64 },
    FallbackToSingleStream { reason: DownloadStrategy },
}

pub fn create_remote_workspace() -> AppResult<std::path::PathBuf> {
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

pub fn extract_remote_archive(zip_path: &Path, workspace: &Path) -> AppResult<()> {
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

        let out_path = match normalized_archive_entry_path(entry.name()) {
            Some(path) => workspace.join(path_under_vipdoc_root(path)),
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
            let mut out_file = std::fs::File::create(&out_path).map_err(|source| {
                RuntimeError::CreateDownloadFile {
                    path: out_path.clone(),
                    source,
                }
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

fn normalized_archive_entry_path(entry_name: &str) -> Option<PathBuf> {
    let normalized = entry_name.replace('\u{5c}', "/");
    let mut safe_path = PathBuf::new();

    for component in Path::new(&normalized).components() {
        match component {
            Component::Normal(part) => safe_path.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    if safe_path.as_os_str().is_empty() {
        None
    } else {
        Some(safe_path)
    }
}

fn path_under_vipdoc_root(path: PathBuf) -> PathBuf {
    if path.starts_with("vipdoc") {
        path
    } else if path
        .components()
        .next()
        .is_some_and(|component| matches!(component, Component::Normal(part) if part == "sh" || part == "sz" || part == "bj"))
    {
        Path::new("vipdoc").join(path)
    } else {
        path
    }
}

pub fn download_remote_archive(url: &str, dest: &Path) -> AppResult<()> {
    let response = reqwest::blocking::Client::builder()
        .build()
        .and_then(|client| client.get(url).send())
        .map_err(|e| RuntimeError::DownloadFailed {
            url: url.to_owned(),
            reason: e.to_string(),
        })?;

    download_remote_archive_from_response(url, dest, response)
}

pub(crate) fn evaluate_segmented_download_capability(
    content_length: Option<u64>,
    accept_ranges: Option<&str>,
) -> ProbeOutcome {
    match content_length {
        Some(content_length) => {
            if !accept_ranges_supports_bytes(accept_ranges) {
                return ProbeOutcome::FallbackToSingleStream {
                    reason: DownloadStrategy::MissingRangeSupport,
                };
            }

            ProbeOutcome::SegmentedSupported { content_length }
        }
        None => ProbeOutcome::FallbackToSingleStream {
            reason: DownloadStrategy::MissingContentLength,
        },
    }
}

fn probe_segmented_download_capability(
    client: &reqwest::blocking::Client,
    url: &str,
) -> ProbeOutcome {
    let response = match client.head(url).send() {
        Ok(response) => response,
        Err(_) => {
            return ProbeOutcome::FallbackToSingleStream {
                reason: DownloadStrategy::ProbeRequestFailed,
            };
        }
    };

    if !response.status().is_success() {
        return ProbeOutcome::FallbackToSingleStream {
            reason: DownloadStrategy::UnsupportedProbeResponse,
        };
    }

    let content_length = response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let accept_ranges = response
        .headers()
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|value| value.to_str().ok());
    evaluate_segmented_download_capability(content_length, accept_ranges)
}

pub fn download_remote_archive_with_capability_probe(url: &str, dest: &Path) -> AppResult<()> {
    let client =
        reqwest::blocking::Client::builder()
            .build()
            .map_err(|e| RuntimeError::DownloadFailed {
                url: url.to_owned(),
                reason: e.to_string(),
            })?;

    let probe = probe_segmented_download_capability(&client, url);

    match probe {
        ProbeOutcome::SegmentedSupported { content_length } => {
            match plan_segments(content_length, dest) {
                SegmentPlan::Segmented { segments } => {
                    println!(
                        "Segmented download enabled (size={content_length}, segments={}).",
                        segments.len()
                    );
                    return download_remote_archive_segmented(
                        &client,
                        url,
                        dest,
                        segments,
                        content_length,
                    );
                }
                SegmentPlan::SingleStream => {
                    println!(
                        "Segmented download supported but archive too small (size={content_length}); using single-stream download."
                    );
                }
            }
        }
        ProbeOutcome::FallbackToSingleStream { reason } => {
            println!(
                "Segmented download unavailable ({reason:?}); falling back to single-stream download."
            );
        }
    }

    download_remote_archive(url, dest)
}

fn download_remote_archive_segmented(
    client: &reqwest::blocking::Client,
    url: &str,
    dest: &Path,
    segments: Vec<Segment>,
    total_size: u64,
) -> AppResult<()> {
    let progress = create_progress_bar(Some(total_size));
    let shared_client = Arc::new(client.clone());
    let shared_url = Arc::new(url.to_owned());
    let planned_segments = segments.clone();

    let mut worker_handles: Vec<Option<thread::JoinHandle<AppResult<()>>>> = segments
        .into_iter()
        .map(|segment| {
            let client = Arc::clone(&shared_client);
            let url = Arc::clone(&shared_url);
            let progress = progress.clone();

            Some(thread::spawn(move || {
                download_segment_to_part_file(&client, &url, segment, progress)
            }))
        })
        .collect();

    let mut remaining_workers = worker_handles.len();
    while remaining_workers > 0 {
        let mut joined_worker = false;

        for handle_slot in &mut worker_handles {
            let Some(handle) = handle_slot.as_ref() else {
                continue;
            };

            if !handle.is_finished() {
                continue;
            }

            joined_worker = true;
            remaining_workers -= 1;

            let handle = handle_slot
                .take()
                .expect("finished handle should still be present");
            let result = handle
                .join()
                .map_err(|_| RuntimeError::WorkerThreadPanicked);

            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    progress.finish_and_clear();
                    cleanup_partial_download_artifacts(dest, &planned_segments);
                    return Err(error.into());
                }
            };

            if let Err(error) = result {
                progress.finish_and_clear();
                cleanup_partial_download_artifacts(dest, &planned_segments);
                return Err(error);
            }
        }

        if !joined_worker {
            thread::sleep(Duration::from_millis(10));
        }
    }

    progress.finish_and_clear();
    merge_segment_files(&planned_segments, dest)?;
    println!("Segmented download complete.");
    Ok(())
}

pub(crate) fn merge_segment_files(segments: &[Segment], dest: &Path) -> AppResult<()> {
    let mut ordered_segments: Vec<&Segment> = segments.iter().collect();
    ordered_segments.sort_by_key(|segment| segment.index);

    let merge_result = (|| -> AppResult<()> {
        let mut out_file =
            File::create(dest).map_err(|source| RuntimeError::CreateDownloadFile {
                path: dest.to_path_buf(),
                source,
            })?;

        for segment in ordered_segments {
            let mut part_file =
                File::open(&segment.part_path).map_err(|source| RuntimeError::ReadDayFile {
                    path: segment.part_path.clone(),
                    source,
                })?;
            std::io::copy(&mut part_file, &mut out_file).map_err(|source| {
                RuntimeError::CreateDownloadFile {
                    path: dest.to_path_buf(),
                    source: std::io::Error::new(source.kind(), source.to_string()),
                }
            })?;
        }

        drop(out_file);

        for segment in segments {
            std::fs::remove_file(&segment.part_path).map_err(|source| {
                RuntimeError::CreateDownloadFile {
                    path: segment.part_path.clone(),
                    source,
                }
            })?;
        }

        Ok(())
    })();

    if merge_result.is_err() {
        cleanup_partial_download_artifacts(dest, segments);
    }

    merge_result
}

pub(crate) fn cleanup_partial_download_artifacts(dest: &Path, segments: &[Segment]) {
    let _ = std::fs::remove_file(dest);
    for segment in segments {
        let _ = std::fs::remove_file(&segment.part_path);
    }
}

fn download_segment_to_part_file(
    client: &reqwest::blocking::Client,
    url: &str,
    segment: Segment,
    progress: ProgressBar,
) -> AppResult<()> {
    let response = client
        .get(url)
        .header(reqwest::header::RANGE, segment.range.to_http_range_header())
        .send()
        .map_err(|error| RuntimeError::DownloadFailed {
            url: url.to_owned(),
            reason: format!("segment {} request failed: {error}", segment.index),
        })?;

    let mut response = validate_segment_response(url, &segment, response)?;
    let mut out_file =
        File::create(&segment.part_path).map_err(|source| RuntimeError::CreateDownloadFile {
            path: segment.part_path.clone(),
            source,
        })?;

    let mut buf = [0u8; 8192];
    loop {
        let bytes_read = response
            .read(&mut buf)
            .map_err(|error| RuntimeError::DownloadFailed {
                url: url.to_owned(),
                reason: format!("segment {} read failed: {error}", segment.index),
            })?;

        if bytes_read == 0 {
            break;
        }

        out_file.write_all(&buf[..bytes_read]).map_err(|source| {
            RuntimeError::CreateDownloadFile {
                path: segment.part_path.clone(),
                source: std::io::Error::new(source.kind(), source.to_string()),
            }
        })?;
        progress.inc(bytes_read as u64);
    }

    Ok(())
}

fn validate_segment_response(
    url: &str,
    segment: &Segment,
    response: reqwest::blocking::Response,
) -> AppResult<reqwest::blocking::Response> {
    let status = response.status();
    if status != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(RuntimeError::DownloadFailed {
            url: url.to_owned(),
            reason: format!(
                "segment {} expected HTTP 206 Partial Content, got HTTP {}",
                segment.index, status
            ),
        }
        .into());
    }

    Ok(response)
}

fn download_remote_archive_from_response(
    url: &str,
    dest: &Path,
    mut response: reqwest::blocking::Response,
) -> AppResult<()> {
    println!("Downloading {url} ...");

    if !response.status().is_success() {
        return Err(RuntimeError::DownloadFailed {
            url: url.to_owned(),
            reason: format!("HTTP {}", response.status()),
        }
        .into());
    }

    let total_size = response.content_length();
    let progress = create_progress_bar(total_size);

    let mut out_file = File::create(dest).map_err(|source| RuntimeError::CreateDownloadFile {
        path: dest.to_path_buf(),
        source,
    })?;

    let mut buf = [0u8; 8192];
    loop {
        let n = std::io::Read::read(&mut response, &mut buf).map_err(|e| {
            RuntimeError::DownloadFailed {
                url: url.to_owned(),
                reason: e.to_string(),
            }
        })?;
        if n == 0 {
            break;
        }
        out_file
            .write_all(&buf[..n])
            .map_err(|source| RuntimeError::CreateDownloadFile {
                path: dest.to_path_buf(),
                source: std::io::Error::new(source.kind(), source.to_string()),
            })?;
        progress.inc(n as u64);
    }

    progress.finish_and_clear();
    println!("Download complete.");
    Ok(())
}

fn create_progress_bar(total_size: Option<u64>) -> ProgressBar {
    if let Some(size) = total_size {
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
    }
}

fn accept_ranges_supports_bytes(accept_ranges: Option<&str>) -> bool {
    accept_ranges.is_some_and(|value| {
        value
            .split(',')
            .any(|token| token.trim().eq_ignore_ascii_case("bytes"))
    })
}

/// Prepares a remote workspace for processing.
///
/// This function orchestrates the full remote-download workflow:
/// 1. Creates a temporary workspace directory
/// 2. Downloads the remote archive
/// 3. Extracts the archive into the workspace
/// 4. Removes the downloaded zip file
/// 5. Validates that a `vipdoc` directory exists in the extracted content
///
/// Returns the workspace path on success. The caller owns cleanup responsibility.
pub fn prepare_remote_workspace() -> AppResult<std::path::PathBuf> {
    prepare_remote_workspace_from_url(REMOTE_DAY_ZIP_URL)
}

pub(crate) fn prepare_remote_workspace_from_url(url: &str) -> AppResult<std::path::PathBuf> {
    let workspace = create_remote_workspace()?;
    let zip_path = workspace.join("hsjday.zip");
    if let Err(error) = download_remote_archive_with_capability_probe(url, &zip_path) {
        cleanup_failed_remote_workspace(&workspace, &zip_path);
        return Err(error);
    }
    println!("Extracting archive...");
    if let Err(error) = extract_remote_archive(&zip_path, &workspace) {
        cleanup_failed_remote_workspace(&workspace, &zip_path);
        return Err(error);
    }
    let _ = std::fs::remove_file(&zip_path);
    if let Err(error) = validate_vipdoc_exists(&workspace) {
        cleanup_failed_remote_workspace(&workspace, &zip_path);
        return Err(error);
    }
    Ok(workspace)
}

fn cleanup_failed_remote_workspace(workspace: &Path, zip_path: &Path) {
    let _ = std::fs::remove_file(zip_path);
    let _ = std::fs::remove_dir_all(workspace);
}

/// Validates that the extracted archive contains a `vipdoc` directory.
fn validate_vipdoc_exists(workspace: &Path) -> AppResult<()> {
    let vipdoc = workspace.join("vipdoc");
    if vipdoc.is_dir() {
        Ok(())
    } else {
        Err(RuntimeError::ExtractArchive {
            path: workspace.to_path_buf(),
            reason: "extracted archive does not contain a 'vipdoc' directory".to_owned(),
        }
        .into())
    }
}

#[cfg(test)]
pub fn resolve_vipdoc_root(workspace: &Path) -> AppResult<std::path::PathBuf> {
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

#[cfg(test)]
#[path = "download_tests.rs"]
mod tests;
