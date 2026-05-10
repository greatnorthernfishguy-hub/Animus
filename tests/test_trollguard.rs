// tests/test_trollguard.rs
// Integration tests for TrollGuard HTTP bridge
//
// ---- Changelog ----
// 2026-05-10 Task6/trollguard — Integration tests for TrollGuard perimeter scan
// What: Three async tests for TrollGuard bridge using mockito mock server
// Why: Verify graceful fallback when TG unavailable, and correct verdict handling
// How: mockito async server simulates TrollGuard responses; tests cover SAFE, MALICIOUS, and down states
// -------------------

use animus::trollguard::TrollGuardBridge;

#[tokio::test]
async fn safe_verdict_passes() {
    let mut server = mockito::Server::new_async().await;
    let mock = server.mock("POST", "/scan/text")
        .with_status(200)
        .with_body(r#"{"verdict":"SAFE","sanitized_text":"hello","max_score":0.01,"chunks_scanned":1,"flagged_chunks":0,"scan_time_ms":5.0,"source":"animus"}"#)
        .create_async().await;

    let bridge = TrollGuardBridge::new(&server.url());
    let result = bridge.scan("hello", "animus").await;
    assert!(result.is_clean);
    assert_eq!(result.sanitized_text, "hello");
    mock.assert_async().await;
}

#[tokio::test]
async fn malicious_verdict_blocks() {
    let mut server = mockito::Server::new_async().await;
    let mock = server.mock("POST", "/scan/text")
        .with_status(200)
        .with_body(r#"{"verdict":"MALICIOUS","sanitized_text":"","max_score":0.95,"chunks_scanned":1,"flagged_chunks":1,"scan_time_ms":5.0,"source":"animus"}"#)
        .create_async().await;

    let bridge = TrollGuardBridge::new(&server.url());
    let result = bridge.scan("inject me", "animus").await;
    assert!(!result.is_clean);
    mock.assert_async().await;
}

#[tokio::test]
async fn trollguard_down_allows_with_flag() {
    // Port 19999 has nothing listening — simulates TG unavailable
    let bridge = TrollGuardBridge::new("http://127.0.0.1:19999");
    let result = bridge.scan("hello", "animus").await;
    assert!(result.is_clean);        // allow through when TG is down
    assert!(result.tg_unavailable);  // but flag it
}
