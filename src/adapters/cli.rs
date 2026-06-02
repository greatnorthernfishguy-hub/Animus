// src/adapters/cli.rs
// CLI adapter — stdin/stdout line protocol.
// Each newline-terminated input is a complete turn.
// Primarily for local interactive use and integration testing.
//
// ---- Changelog ----
// 2026-05-10 Task10/cli-adapter — CliAdapter
// What: stdin/stdout turn pipeline: TrollGuard → ingest → assemble → TID → afterTurn
// Why: First channel adapter for Anima — enables local interactive use + integration tests
// How: process_line() builds ChannelContext + TurnEnvelope, runs run_pipeline() through
//      all 5 stages, deposits River events at each stage boundary
// 2026-05-25 Claude (Sonnet 4.6) — Phase 1: drop RpcAdapter + IntrospectionRelay, delegate to TurnPipeline
// What: CliAdapter now holds TurnPipeline instead of RpcAdapter/IntrospectionRelay
// Why: Bridge subprocess eliminated; pipeline is substrate-direct via TractWriter
// How: process_line() deposits channel_connection event, builds TurnContext, calls pipeline.run()
// -------------------

use crate::pipeline::{SourceType, TurnContext, TurnPipeline};
use crate::tract_writer::TractWriter;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub struct CliAdapter {
    pipeline: Arc<TurnPipeline>,
    tract: Arc<TractWriter>,
}

impl CliAdapter {
    pub fn new(pipeline: Arc<TurnPipeline>, tract: Arc<TractWriter>) -> Self {
        Self { pipeline, tract }
    }

    /// Process one CLI line as a complete turn. Returns response text.
    pub async fn process_line(&self, line: &str, user_id: &str) -> String {
        if line.trim().is_empty() {
            return String::new();
        }
        self.tract.deposit_event_silent("channel_connection", serde_json::json!({
            "channel_id": "cli",
            "user_id": user_id,
            "channel_type": "cli",
            "connection_start": now_secs(),
        }));
        let ctx = TurnContext {
            text: line.trim().to_string(),
            channel_id: "cli".to_string(),
            user_id: user_id.to_string(),
            source: SourceType::Channel,
        };
        self.pipeline.run(ctx).await
    }
}
