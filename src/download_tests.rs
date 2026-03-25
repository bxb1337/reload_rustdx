use super::{
    cleanup_partial_download_artifacts, download_remote_archive_with_capability_probe,
    evaluate_segmented_download_capability, extract_remote_archive, merge_segment_files,
    plan_segments, prepare_remote_workspace_from_url, resolve_vipdoc_root, DownloadStrategy,
    ProbeOutcome, SegmentPlan, MIN_SEGMENT_BYTES,
};
use crate::error::{AppError, RuntimeError};
use reqwest::StatusCode;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[test]
fn capability_probe_requires_content_length_and_range_support() {
    let with_all_headers = evaluate_segmented_download_capability(Some(128), Some("bytes"));
    assert!(matches!(
        with_all_headers,
        ProbeOutcome::SegmentedSupported {
            content_length: 128
        }
    ));

    assert!(matches!(
        evaluate_segmented_download_capability(None, Some("bytes")),
        ProbeOutcome::FallbackToSingleStream {
            reason: DownloadStrategy::MissingContentLength
        }
    ));

    assert!(matches!(
        evaluate_segmented_download_capability(Some(128), None),
        ProbeOutcome::FallbackToSingleStream {
            reason: DownloadStrategy::MissingRangeSupport
        }
    ));

    assert!(matches!(
        evaluate_segmented_download_capability(Some(128), Some("none")),
        ProbeOutcome::FallbackToSingleStream {
            reason: DownloadStrategy::MissingRangeSupport
        }
    ));
}

#[test]
fn probe_failure_falls_back_to_single_stream() {
    let body = b"archive-bytes".to_vec();
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = TestServer::start(TestServerConfig {
        requests: Arc::clone(&requests),
        head_status_line: "HTTP/1.1 500 Internal Server Error",
        head_headers: vec![],
        get_status_line: "HTTP/1.1 200 OK",
        get_headers: vec![
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".to_owned()),
        ],
        get_body: body.clone(),
    });

    let mut dest = std::env::temp_dir();
    dest.push(unique_name("probe_fallback.bin"));

    download_remote_archive_with_capability_probe(&server.url(), &dest)
        .expect("probe failure should fallback to single stream");

    let saved = std::fs::read(&dest).expect("read downloaded bytes");
    let _ = std::fs::remove_file(&dest);
    assert_eq!(saved, body);

    let seen = requests.lock().expect("lock requests").clone();
    assert!(seen.iter().any(|r| r.starts_with("HEAD ")));
    assert!(seen.iter().any(|r| r.starts_with("GET ")));
}

#[test]
fn probe_get_non_success_still_fails_when_fallback_download_fails() {
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = TestServer::start(TestServerConfig {
        requests: Arc::clone(&requests),
        head_status_line: "HTTP/1.1 500 Internal Server Error",
        head_headers: vec![("Connection", "close".to_owned())],
        get_status_line: "HTTP/1.1 503 Service Unavailable",
        get_headers: vec![("Connection", "close".to_owned())],
        get_body: Vec::new(),
    });

    let mut dest = std::env::temp_dir();
    dest.push(unique_name("probe_fallback_error.bin"));

    let result = download_remote_archive_with_capability_probe(&server.url(), &dest);
    let _ = std::fs::remove_file(&dest);

    match result {
        Err(AppError::Runtime(RuntimeError::DownloadFailed { .. })) => {}
        other => panic!("expected download failure from fallback path, got {other:?}"),
    }
}

#[test]
fn missing_content_length_probe_falls_back_to_single_stream_download() {
    let body = b"fallback-missing-content-length".to_vec();
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = TestServer::start(TestServerConfig {
        requests: Arc::clone(&requests),
        head_status_line: "HTTP/1.1 200 OK",
        head_headers: vec![
            ("Accept-Ranges", "bytes".to_owned()),
            ("Connection", "close".to_owned()),
        ],
        get_status_line: "HTTP/1.1 200 OK",
        get_headers: vec![
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".to_owned()),
        ],
        get_body: body.clone(),
    });

    let mut dest = std::env::temp_dir();
    dest.push(unique_name("missing_content_length_fallback.bin"));

    download_remote_archive_with_capability_probe(&server.url(), &dest)
        .expect("missing content length should fallback to single stream");

    let saved = std::fs::read(&dest).expect("read downloaded bytes");
    let _ = std::fs::remove_file(&dest);
    assert_eq!(saved, body);

    let seen = requests.lock().expect("lock requests").clone();
    assert_eq!(
        count_head_requests(&seen),
        1,
        "expected one probe HEAD request"
    );
    assert_eq!(
        count_range_requests(&seen),
        0,
        "fallback path must not send ranged GETs"
    );
    assert_eq!(
        count_plain_get_requests(&seen),
        1,
        "expected one plain fallback GET"
    );
}

#[test]
fn unsupported_range_probe_falls_back_to_single_stream_download() {
    let body = b"fallback-missing-range-support".to_vec();
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = TestServer::start(TestServerConfig {
        requests: Arc::clone(&requests),
        head_status_line: "HTTP/1.1 200 OK",
        head_headers: vec![
            ("Content-Length", body.len().to_string()),
            ("Accept-Ranges", "none".to_owned()),
            ("Connection", "close".to_owned()),
        ],
        get_status_line: "HTTP/1.1 200 OK",
        get_headers: vec![
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".to_owned()),
        ],
        get_body: body.clone(),
    });

    let mut dest = std::env::temp_dir();
    dest.push(unique_name("unsupported_range_fallback.bin"));

    download_remote_archive_with_capability_probe(&server.url(), &dest)
        .expect("missing range support should fallback to single stream");

    let saved = std::fs::read(&dest).expect("read downloaded bytes");
    let _ = std::fs::remove_file(&dest);
    assert_eq!(saved, body);

    let seen = requests.lock().expect("lock requests").clone();
    assert_eq!(
        count_head_requests(&seen),
        1,
        "expected one probe HEAD request"
    );
    assert_eq!(
        count_range_requests(&seen),
        0,
        "fallback path must not send ranged GETs"
    );
    assert_eq!(
        count_plain_get_requests(&seen),
        1,
        "expected one plain fallback GET"
    );
}

#[test]
fn prepare_remote_workspace_cleans_up_workspace_on_vipdoc_validation_failure() {
    let archive = build_stored_zip_without_vipdoc(1024);
    let server = RangeTestServer::start(
        archive,
        Arc::new(Mutex::new(Vec::<String>::new())),
        VecDeque::new(),
    );

    let result = prepare_remote_workspace_from_url(&server.url());

    let workspace = match result {
        Err(AppError::Runtime(RuntimeError::ExtractArchive { path, reason })) => {
            assert!(
                reason.contains("does not contain a 'vipdoc' directory"),
                "unexpected validation error: {reason}"
            );
            path
        }
        other => panic!("expected vipdoc validation failure, got {other:?}"),
    };

    assert!(
        !workspace.exists(),
        "failed remote setup must remove the temp workspace before returning"
    );
}

fn unique_name(suffix: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after unix epoch")
        .as_nanos();
    PathBuf::from(format!("reload_rustdx_{now}_{suffix}"))
}

struct TestServer {
    address: String,
}

impl TestServer {
    fn start(config: TestServerConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind local test server");
        let address = listener.local_addr().expect("resolve local addr");
        thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept connection");
                handle_connection(&mut stream, &config);
            }
        });
        Self {
            address: format!("http://{address}"),
        }
    }

    fn url(&self) -> String {
        self.address.clone()
    }
}

struct TestServerConfig {
    requests: Arc<Mutex<Vec<String>>>,
    head_status_line: &'static str,
    head_headers: Vec<(&'static str, String)>,
    get_status_line: &'static str,
    get_headers: Vec<(&'static str, String)>,
    get_body: Vec<u8>,
}

fn handle_connection(stream: &mut TcpStream, config: &TestServerConfig) {
    let request_text = read_http_request(stream);
    config
        .requests
        .lock()
        .expect("lock requests")
        .push(request_text.replace("\r\n", " | "));

    let request_line = request_text.lines().next().unwrap_or_default().to_owned();

    let (status_line, headers, body) = if request_line.starts_with("HEAD ") {
        (
            config.head_status_line,
            &config.head_headers,
            Vec::<u8>::new(),
        )
    } else {
        (
            config.get_status_line,
            &config.get_headers,
            config.get_body.clone(),
        )
    };

    write!(stream, "{status_line}\r\n").expect("write status line");
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n").expect("write header");
    }
    write!(stream, "\r\n").expect("write header terminator");
    if !body.is_empty() {
        stream.write_all(&body).expect("write body");
    }
    stream.flush().expect("flush response");
}

#[test]
fn segment_plan_covers_archive_without_gaps_or_overlap() {
    let dest = PathBuf::from("/tmp/hsjday.zip");

    // Test with a large archive (100 MiB) that should produce multiple segments.
    let archive_size = 100 * 1024 * 1024;
    let plan = plan_segments(archive_size, &dest);

    let SegmentPlan::Segmented { segments } = plan else {
        panic!("expected segmented plan for 100 MiB archive");
    };

    // Verify all ranges are non-empty and non-overlapping.
    let mut prev_end: u64 = 0;
    for seg in &segments {
        assert!(
            seg.range.start == prev_end,
            "gap detected at segment {}",
            seg.index
        );
        assert!(
            seg.range.end > seg.range.start,
            "empty range at segment {}",
            seg.index
        );
        prev_end = seg.range.end;
    }

    // Verify full coverage: last segment's end must equal archive size.
    assert_eq!(
        prev_end, archive_size,
        "segments must exactly cover archive"
    );

    // Verify part-file naming.
    for seg in &segments {
        let expected = PathBuf::from(format!("/tmp/hsjday.zip.part{}", seg.index));
        assert_eq!(
            seg.part_path, expected,
            "part file path mismatch at segment {}",
            seg.index
        );
    }

    // Verify total bytes equals archive size.
    let total_bytes: u64 = segments.iter().map(|s| s.range.end - s.range.start).sum();
    assert_eq!(
        total_bytes, archive_size,
        "total segment bytes must equal archive size"
    );
}

#[test]
fn small_archives_use_single_stream_plan() {
    let dest = PathBuf::from("/tmp/hsjday.zip");

    // Exactly at the boundary: 2 * MIN_SEGMENT_BYTES - 1.
    let just_below_threshold = 2 * MIN_SEGMENT_BYTES - 1;
    let plan = plan_segments(just_below_threshold, &dest);
    assert!(matches!(plan, SegmentPlan::SingleStream));

    // Exactly at the threshold: 2 * MIN_SEGMENT_BYTES.
    let at_threshold = 2 * MIN_SEGMENT_BYTES;
    let plan = plan_segments(at_threshold, &dest);
    assert!(matches!(plan, SegmentPlan::Segmented { .. }));

    // Very small file.
    let plan = plan_segments(1024, &dest);
    assert!(matches!(plan, SegmentPlan::SingleStream));

    // Zero bytes edge case.
    let plan = plan_segments(0, &dest);
    assert!(matches!(plan, SegmentPlan::SingleStream));
}

#[test]
fn merged_archive_extracts_into_vipdoc_root() {
    let archive = build_stored_zip_with_vipdoc_day(2 * MIN_SEGMENT_BYTES as usize);
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = RangeTestServer::start(
        archive.clone(),
        Arc::clone(&requests),
        VecDeque::from(vec![RangeResponse::success(), RangeResponse::success()]),
    );

    let workspace = temp_test_dir("segmented_parts_success");
    let dest = workspace.join("hsjday.zip");

    download_remote_archive_with_capability_probe(&server.url(), &dest)
        .expect("segmented download should merge into final zip");

    let merged = std::fs::read(&dest).expect("read merged archive");
    assert_eq!(merged, archive);
    assert!(
        !dest.with_file_name("hsjday.zip.part0").exists(),
        "merged download should remove part0"
    );
    assert!(
        !dest.with_file_name("hsjday.zip.part1").exists(),
        "merged download should remove part1"
    );

    extract_remote_archive(&dest, &workspace).expect("extract merged archive");
    let vipdoc_root = resolve_vipdoc_root(&workspace).expect("resolve vipdoc root");
    assert_eq!(vipdoc_root, workspace.join("vipdoc"));
    assert!(
        workspace.join("vipdoc/sh/sh600000.day").exists(),
        "expected extracted .day file inside vipdoc"
    );

    let seen = requests.lock().expect("lock requests").clone();
    assert!(seen.iter().any(|line| line.starts_with("HEAD / ")));
    assert_eq!(
        count_range_requests(&seen),
        2,
        "expected one GET per segment"
    );

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn extract_remote_archive_normalizes_backslash_entry_paths_into_vipdoc_tree() {
    let archive = build_stored_zip_with_windows_style_vipdoc_entries();
    let workspace = temp_test_dir("windows_style_zip_paths");
    let zip_path = workspace.join("windows_paths.zip");
    std::fs::write(&zip_path, archive).expect("write zip fixture");

    extract_remote_archive(&zip_path, &workspace).expect("extract windows-style archive");

    let vipdoc_root = resolve_vipdoc_root(&workspace).expect("resolve vipdoc root");
    assert_eq!(vipdoc_root, workspace.join("vipdoc"));
    assert!(
        workspace.join("vipdoc/sh/lday/sh000001.day").is_file(),
        "expected sh lday file extracted into nested directories"
    );
    assert!(
        workspace.join("vipdoc/bj/lday/bj430001.day").is_file(),
        "expected bj lday file extracted into nested directories"
    );
    assert!(
        !workspace.join(r"vipdoc\sh\lday\sh000001.day").exists(),
        "backslash-separated filename should not remain flat on linux"
    );
    assert!(
        !workspace.join("sh/lday/sh000001.day").exists(),
        "rootless archive entries should be placed under vipdoc"
    );

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn extract_remote_archive_does_not_synthesize_vipdoc_for_unrelated_root_entries() {
    let archive = build_stored_zip_without_vipdoc(32);
    let workspace = temp_test_dir("non_vipdoc_root_entries");
    let zip_path = workspace.join("non_vipdoc.zip");
    std::fs::write(&zip_path, archive).expect("write non-vipdoc zip fixture");

    extract_remote_archive(&zip_path, &workspace).expect("extract non-vipdoc archive");

    let vipdoc_result = resolve_vipdoc_root(&workspace);
    assert!(
        vipdoc_result.is_err(),
        "archives without vipdoc entries should not gain a synthetic vipdoc root"
    );
    assert!(
        workspace.join("otherdir/sh600000.day").is_file(),
        "original non-vipdoc path should remain under workspace root"
    );
    assert!(
        !workspace.join("vipdoc/otherdir/sh600000.day").exists(),
        "non-vipdoc paths must not be rewritten under vipdoc"
    );

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn cleanup_removes_part_files_after_merge_or_extract_failure() {
    let workspace = temp_test_dir("segmented_cleanup_paths");
    let dest = workspace.join("hsjday.zip");

    let segmented_archive = build_stored_zip_with_vipdoc_day(2 * MIN_SEGMENT_BYTES as usize);
    let SegmentPlan::Segmented { segments } = plan_segments(segmented_archive.len() as u64, &dest)
    else {
        panic!("expected segmented plan for cleanup test");
    };

    for segment in &segments {
        let range = segment.range.start as usize..segment.range.end as usize;
        std::fs::write(&segment.part_path, &segmented_archive[range]).expect("write part bytes");
    }

    merge_segment_files(&segments, &dest).expect("merge segment files");
    assert!(
        dest.exists(),
        "merged zip should exist before extraction failure"
    );
    assert!(
        segments.iter().all(|segment| !segment.part_path.exists()),
        "merge should remove all part files"
    );

    let extract_error = extract_remote_archive(&dest, &dest);
    assert!(
        extract_error.is_err(),
        "invalid workspace path should fail extraction"
    );
    cleanup_partial_download_artifacts(&dest, &segments);
    assert!(
        !dest.exists(),
        "cleanup should remove merged zip after extract failure"
    );
    assert!(
        segments.iter().all(|segment| !segment.part_path.exists()),
        "cleanup should leave no part files after extract failure"
    );

    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let failing_dest = workspace.join("failing.zip");
    let failing_archive = build_stored_zip_with_vipdoc_day(2 * MIN_SEGMENT_BYTES as usize);
    let server = RangeTestServer::start(
        failing_archive,
        Arc::clone(&requests),
        VecDeque::from(vec![
            RangeResponse::failure(StatusCode::SERVICE_UNAVAILABLE),
            RangeResponse::success(),
        ]),
    );

    let result = download_remote_archive_with_capability_probe(&server.url(), &failing_dest);

    match result {
        Err(AppError::Runtime(RuntimeError::DownloadFailed { .. })) => {}
        other => panic!("expected segmented worker failure, got {other:?}"),
    }

    assert!(
        !failing_dest.exists(),
        "failed segmented path must not leave final zip"
    );
    assert!(
        !failing_dest.with_file_name("failing.zip.part0").exists(),
        "failed segmented path must remove part0"
    );
    assert!(
        !failing_dest.with_file_name("failing.zip.part1").exists(),
        "failed segmented path must remove part1"
    );

    let seen = requests.lock().expect("lock requests").clone();
    assert!(
        count_range_requests(&seen) >= 1,
        "expected ranged GET requests"
    );

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn segmented_worker_http_200_response_is_reported_as_error() {
    let archive = build_stored_zip_with_vipdoc_day(2 * MIN_SEGMENT_BYTES as usize);
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = RangeTestServer::start(
        archive,
        Arc::clone(&requests),
        VecDeque::from(vec![
            RangeResponse::success(),
            RangeResponse::failure(StatusCode::OK),
        ]),
    );

    let workspace = temp_test_dir("segmented_worker_200_error");
    let dest = workspace.join("hsjday.zip");

    let result = download_remote_archive_with_capability_probe(&server.url(), &dest);

    match result {
        Err(AppError::Runtime(RuntimeError::DownloadFailed { reason, .. })) => {
            assert!(
                reason.contains("expected HTTP 206 Partial Content, got HTTP 200 OK"),
                "unexpected error reason: {reason}"
            );
        }
        other => panic!("expected segmented worker 200 failure, got {other:?}"),
    }

    assert!(
        !dest.exists(),
        "failed segmented path must not leave final zip"
    );
    assert!(
        !dest.with_file_name("hsjday.zip.part0").exists(),
        "failed segmented path must remove part0"
    );
    assert!(
        !dest.with_file_name("hsjday.zip.part1").exists(),
        "failed segmented path must remove part1"
    );

    let seen = requests.lock().expect("lock requests").clone();
    assert_eq!(
        count_head_requests(&seen),
        1,
        "expected one probe HEAD request"
    );
    assert_eq!(
        count_range_requests(&seen),
        2,
        "expected two ranged worker GETs"
    );

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn segmented_worker_http_416_response_is_reported_as_error() {
    let archive = build_stored_zip_with_vipdoc_day(2 * MIN_SEGMENT_BYTES as usize);
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = RangeTestServer::start(
        archive,
        Arc::clone(&requests),
        VecDeque::from(vec![
            RangeResponse::success(),
            RangeResponse::failure(StatusCode::RANGE_NOT_SATISFIABLE),
        ]),
    );

    let workspace = temp_test_dir("segmented_worker_416_error");
    let dest = workspace.join("hsjday.zip");

    let result = download_remote_archive_with_capability_probe(&server.url(), &dest);

    match result {
        Err(AppError::Runtime(RuntimeError::DownloadFailed { reason, .. })) => {
            assert!(
                reason.contains(
                    "expected HTTP 206 Partial Content, got HTTP 416 Range Not Satisfiable"
                ),
                "unexpected error reason: {reason}"
            );
        }
        other => panic!("expected segmented worker 416 failure, got {other:?}"),
    }

    assert!(
        !dest.exists(),
        "failed segmented path must not leave final zip"
    );
    assert!(
        !dest.with_file_name("hsjday.zip.part0").exists(),
        "failed segmented path must remove part0"
    );
    assert!(
        !dest.with_file_name("hsjday.zip.part1").exists(),
        "failed segmented path must remove part1"
    );

    let seen = requests.lock().expect("lock requests").clone();
    assert_eq!(
        count_head_requests(&seen),
        1,
        "expected one probe HEAD request"
    );
    assert_eq!(
        count_range_requests(&seen),
        2,
        "expected two ranged worker GETs"
    );

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn segmented_download_reports_later_worker_failure_without_waiting_for_slow_first_worker() {
    let archive = build_stored_zip_with_vipdoc_day(2 * MIN_SEGMENT_BYTES as usize);
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let release_slow_segment = Arc::new(AtomicBool::new(false));
    let second_failure_seen = Arc::new(AtomicBool::new(false));
    let server = RangeTestServer::start_with_behavior(
        archive,
        Arc::clone(&requests),
        VecDeque::from(vec![
            RangeResponse::delayed_success(Arc::clone(&release_slow_segment)),
            RangeResponse::failing_and_signal(
                StatusCode::SERVICE_UNAVAILABLE,
                Arc::clone(&second_failure_seen),
            ),
        ]),
    );

    let workspace = temp_test_dir("segmented_prompt_failure");
    let dest = workspace.join("hsjday.zip");
    let started_at = Instant::now();

    let result = download_remote_archive_with_capability_probe(&server.url(), &dest);
    let elapsed = started_at.elapsed();

    assert!(
        second_failure_seen.load(Ordering::SeqCst),
        "fixture should observe the later worker fail"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "worker failure should surface promptly instead of waiting on slow earlier workers; elapsed={elapsed:?}"
    );

    match result {
        Err(AppError::Runtime(RuntimeError::DownloadFailed { .. })) => {}
        other => panic!("expected prompt segmented worker failure, got {other:?}"),
    }

    release_slow_segment.store(true, Ordering::SeqCst);

    assert!(
        !dest.exists(),
        "failed segmented path must not leave final zip"
    );
    assert!(
        !dest.with_file_name("hsjday.zip.part0").exists(),
        "failed segmented path must remove slow worker part"
    );
    assert!(
        !dest.with_file_name("hsjday.zip.part1").exists(),
        "failed segmented path must remove failed worker part"
    );

    let _ = std::fs::remove_dir_all(&workspace);
}

#[test]
fn real_network_is_not_required_for_fallback_and_range_cases() {
    let body = b"local-only-fixture".to_vec();
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let server = TestServer::start(TestServerConfig {
        requests: Arc::clone(&requests),
        head_status_line: "HTTP/1.1 200 OK",
        head_headers: vec![
            ("Content-Length", body.len().to_string()),
            ("Accept-Ranges", "none".to_owned()),
            ("Connection", "close".to_owned()),
        ],
        get_status_line: "HTTP/1.1 200 OK",
        get_headers: vec![
            ("Content-Length", body.len().to_string()),
            ("Connection", "close".to_owned()),
        ],
        get_body: body.clone(),
    });

    let mut dest = std::env::temp_dir();
    dest.push(unique_name("local_only_network_guard.bin"));

    download_remote_archive_with_capability_probe(&server.url(), &dest)
        .expect("download tests should succeed against local fixture only");

    let saved = std::fs::read(&dest).expect("read downloaded bytes");
    let _ = std::fs::remove_file(&dest);
    assert_eq!(saved, body);

    let seen = requests.lock().expect("lock requests").clone();
    assert_eq!(
        count_head_requests(&seen),
        1,
        "expected one probe HEAD request"
    );
    assert_eq!(
        count_plain_get_requests(&seen),
        1,
        "expected one local fallback GET"
    );
    assert_eq!(
        count_range_requests(&seen),
        0,
        "guard test must remain local fallback only"
    );
}

fn build_stored_zip_with_vipdoc_day(day_bytes_len: usize) -> Vec<u8> {
    use std::io::Cursor;

    assert_eq!(
        day_bytes_len % 32,
        0,
        "day fixture must align to 32-byte records"
    );

    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer
        .start_file("vipdoc/sh/sh600000.day", options)
        .expect("start day file in zip");
    writer
        .write_all(&vec![0u8; day_bytes_len])
        .expect("write stored day bytes");
    writer.finish().expect("finish stored zip").into_inner()
}

fn build_stored_zip_with_windows_style_vipdoc_entries() -> Vec<u8> {
    use std::io::Cursor;

    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

    writer
        .start_file(r"sh\lday\sh000001.day", options)
        .expect("start windows-style sh file in zip");
    writer
        .write_all(&vec![1u8; 32])
        .expect("write sh day bytes");

    writer
        .start_file(r"bj\lday\bj430001.day", options)
        .expect("start windows-style bj file in zip");
    writer
        .write_all(&vec![2u8; 32])
        .expect("write bj day bytes");

    writer.finish().expect("finish stored zip").into_inner()
}

fn build_stored_zip_without_vipdoc(file_bytes_len: usize) -> Vec<u8> {
    use std::io::Cursor;

    let cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer
        .start_file("otherdir/sh600000.day", options)
        .expect("start non-vipdoc file in zip");
    writer
        .write_all(&vec![0u8; file_bytes_len])
        .expect("write stored bytes outside vipdoc");
    writer.finish().expect("finish stored zip").into_inner()
}

fn temp_test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(unique_name(name));
    std::fs::create_dir_all(&dir).expect("create temp test dir");
    dir
}

fn count_range_requests(requests: &[String]) -> usize {
    requests
        .iter()
        .filter(|line| {
            line.starts_with("GET / ") && line.to_ascii_lowercase().contains("range: bytes=")
        })
        .count()
}

fn count_head_requests(requests: &[String]) -> usize {
    requests
        .iter()
        .filter(|line| line.starts_with("HEAD / "))
        .count()
}

fn count_plain_get_requests(requests: &[String]) -> usize {
    requests
        .iter()
        .filter(|line| {
            line.starts_with("GET / ") && !line.to_ascii_lowercase().contains("range: bytes=")
        })
        .count()
}

struct RangeTestServer {
    address: String,
}

impl RangeTestServer {
    fn start(
        body: Vec<u8>,
        requests: Arc<Mutex<Vec<String>>>,
        responses: VecDeque<RangeResponse>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind range test server");
        let address = listener.local_addr().expect("resolve local addr");
        let state = Arc::new(RangeServerState {
            body,
            requests,
            responses: Mutex::new(responses),
        });

        thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                let state = Arc::clone(&state);
                thread::spawn(move || handle_range_connection(&mut stream, &state));
            }
        });

        Self {
            address: format!("http://{address}"),
        }
    }

    fn url(&self) -> String {
        self.address.clone()
    }

    fn start_with_behavior(
        body: Vec<u8>,
        requests: Arc<Mutex<Vec<String>>>,
        responses: VecDeque<RangeResponse>,
    ) -> Self {
        Self::start(body, requests, responses)
    }
}

struct RangeServerState {
    body: Vec<u8>,
    requests: Arc<Mutex<Vec<String>>>,
    responses: Mutex<VecDeque<RangeResponse>>,
}

#[derive(Clone)]
struct RangeResponse {
    status: StatusCode,
    release_gate: Option<Arc<AtomicBool>>,
    failure_signal: Option<Arc<AtomicBool>>,
}

impl RangeResponse {
    fn success() -> Self {
        Self {
            status: StatusCode::PARTIAL_CONTENT,
            release_gate: None,
            failure_signal: None,
        }
    }

    fn failure(status: StatusCode) -> Self {
        Self {
            status,
            release_gate: None,
            failure_signal: None,
        }
    }

    fn delayed_success(release_gate: Arc<AtomicBool>) -> Self {
        Self {
            status: StatusCode::PARTIAL_CONTENT,
            release_gate: Some(release_gate),
            failure_signal: None,
        }
    }

    fn failing_and_signal(status: StatusCode, failure_signal: Arc<AtomicBool>) -> Self {
        Self {
            status,
            release_gate: None,
            failure_signal: Some(failure_signal),
        }
    }
}

fn handle_range_connection(stream: &mut TcpStream, state: &RangeServerState) {
    let request_text = read_http_request(stream);
    state
        .requests
        .lock()
        .expect("lock requests")
        .push(request_text.replace("\r\n", " | "));

    let mut lines = request_text.lines();
    let request_line = lines.next().unwrap_or_default().to_owned();

    if request_line.starts_with("HEAD ") {
        write!(stream, "HTTP/1.1 200 OK\r\n").expect("write head status");
        write!(stream, "Accept-Ranges: bytes\r\n").expect("write accept-ranges");
        write!(stream, "Content-Length: {}\r\n", state.body.len()).expect("write content length");
        write!(stream, "Connection: close\r\n\r\n").expect("finish head response");
        stream.flush().expect("flush head response");
        return;
    }

    let response = state
        .responses
        .lock()
        .expect("lock range responses")
        .pop_front()
        .unwrap_or_else(RangeResponse::success);

    let range = request_text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.trim().eq_ignore_ascii_case("range") {
            Some(parse_range(value.trim(), state.body.len()))
        } else {
            None
        }
    });

    if let Some(release_gate) = &response.release_gate {
        while !release_gate.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(10));
        }
    }

    if let Some(signal) = &response.failure_signal {
        signal.store(true, Ordering::SeqCst);
    }

    match (response.status, range) {
        (StatusCode::PARTIAL_CONTENT, Some(range)) => {
            write_partial_response(stream, &state.body, range)
        }
        (StatusCode::PARTIAL_CONTENT, None) => write_full_response(stream, &state.body),
        (status, _) => write_status_response(stream, status),
    }
}

fn read_http_request(stream: &mut TcpStream) -> String {
    let mut req_buf = Vec::new();
    let mut chunk = [0u8; 1024];

    loop {
        let n = stream.read(&mut chunk).expect("read request");
        if n == 0 {
            break;
        }

        req_buf.extend_from_slice(&chunk[..n]);
        if req_buf.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    String::from_utf8_lossy(&req_buf).to_string()
}

fn parse_range(header_value: &str, total_len: usize) -> Range<usize> {
    let bytes = header_value.strip_prefix("bytes=").expect("bytes prefix");
    let (start, end_inclusive) = bytes.split_once('-').expect("range delimiter");
    let start: usize = start.parse().expect("numeric range start");
    let end_inclusive: usize = end_inclusive.parse().expect("numeric range end");
    assert!(end_inclusive < total_len, "range end within body");
    start..(end_inclusive + 1)
}

fn write_partial_response(stream: &mut TcpStream, body: &[u8], range: Range<usize>) {
    let slice = &body[range.clone()];
    write!(stream, "HTTP/1.1 206 Partial Content\r\n").expect("write partial status");
    write!(stream, "Content-Length: {}\r\n", slice.len()).expect("write partial content length");
    write!(
        stream,
        "Content-Range: bytes {}-{}/{}\r\n",
        range.start,
        range.end - 1,
        body.len()
    )
    .expect("write content range");
    write!(stream, "Connection: close\r\n\r\n").expect("finish partial response");
    let _ = stream.write_all(slice);
    let _ = stream.flush();
}

fn write_full_response(stream: &mut TcpStream, body: &[u8]) {
    write!(stream, "HTTP/1.1 200 OK\r\n").expect("write full status");
    write!(stream, "Content-Length: {}\r\n", body.len()).expect("write full content length");
    write!(stream, "Connection: close\r\n\r\n").expect("finish full response");
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

fn write_status_response(stream: &mut TcpStream, status: StatusCode) {
    write!(
        stream,
        "HTTP/1.1 {} {}\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("Unknown")
    )
    .expect("write error status");
    write!(stream, "Connection: close\r\n\r\n").expect("finish error response");
    stream.flush().expect("flush error response");
}
