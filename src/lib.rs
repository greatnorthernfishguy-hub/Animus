// src/lib.rs
// Animus library root — re-exports public modules for integration tests.
//
// ---- Changelog ----
// [2026-05-10] Claude (Sonnet 4.6) — Task 1: Initial scaffold
// What: Empty lib.rs to enable integration test crate
// Why: Integration tests in tests/ need a lib target to import from
// How: Empty for now — modules added in subsequent tasks
// [2026-05-10] Claude (Sonnet 4.6) — Task 2: Envelope module
// What: Added pub mod envelope for TurnEnvelope + ChannelContext types
// Why: Core types needed for channel adapter ↔ RPC pipeline handoff
// How: Module declaration re-exports serde types for integration tests
// [2026-05-10] Claude (Sonnet 4.6) — Task 4: Auth module
// What: Added pub mod auth for constant-time token validation
// Why: WebSocket gateway requires constant-time comparison to prevent timing attacks
// How: Module declaration re-exports validate_token() for integration tests
// -------------------

pub mod auth;
pub mod config;
pub mod envelope;
pub mod rpc_adapter;
pub mod trollguard;
