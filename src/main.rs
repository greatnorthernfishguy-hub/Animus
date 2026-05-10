// src/main.rs
// Animus — Native Agentic Gateway
// E-T Systems / NeuroGraph Ecosystem — AGPL-3.0
//
// ---- Changelog ----
// [2026-05-10] Claude (Sonnet 4.6) — Task 1: Initial scaffold
// What: Placeholder main.rs for initial cargo build verification
// Why: Project scaffold requires a compiling binary target
// How: Minimal main() function — real entry point wired in subsequent tasks
// [2026-05-10] Claude (Sonnet 4.6) — Task 2: Envelope module
// What: Added pub mod envelope for TurnEnvelope + ChannelContext types
// Why: Core types needed for channel adapter ↔ RPC pipeline handoff
// How: Module declaration before main()
// -------------------

pub mod envelope;

fn main() {
    println!("animus starting (scaffold)");
}
