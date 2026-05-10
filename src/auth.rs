// ---- Changelog ----
// 2026-05-10 Task4/auth — constant-time token validation
// What: validate_token() using XOR accumulator — no early exit on mismatch
// Why: Prevents timing attacks on the WebSocket auth gate (spec §2 WebSocket Server)
// How: Byte-by-byte XOR; lengths must match; accumulates diff without branching on it
// -------------------

/// Returns true if `provided` matches `expected`, using a constant-time comparison
/// that does not short-circuit on mismatch.
pub fn validate_token(expected: &str, provided: &str) -> bool {
    if expected.is_empty() || provided.is_empty() {
        return false;
    }
    let expected_bytes = expected.as_bytes();
    let provided_bytes = provided.as_bytes();
    if expected_bytes.len() != provided_bytes.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (a, b) in expected_bytes.iter().zip(provided_bytes.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}
