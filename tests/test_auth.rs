// tests/test_auth.rs
// Integration tests for the constant-time token validator
//
// ---- Changelog ----
// 2026-05-10 Task4/auth — Integration tests for constant-time validation
// What: Three tests validating token comparison behavior
// Why: Ensure the auth gate correctly accepts/rejects tokens without timing leaks
// How: Direct calls to animus::auth::validate_token with various input combinations
// -------------------

use animus::auth::validate_token;

#[test]
fn valid_token_passes() {
    assert!(validate_token("correct_token", "correct_token"));
}

#[test]
fn wrong_token_fails() {
    assert!(!validate_token("correct_token", "wrong_token"));
}

#[test]
fn empty_token_fails() {
    assert!(!validate_token("correct_token", ""));
}
