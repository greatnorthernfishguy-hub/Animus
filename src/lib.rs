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
// -------------------

pub mod envelope;
