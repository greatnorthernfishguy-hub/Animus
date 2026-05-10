// src/adapters/cli.rs
// CLI adapter — stdin/stdout line protocol.
// Each newline-terminated input is a complete turn.
// Primarily for local interactive use and integration testing.
//
// ---- Changelog ----
// 2026-05-10 Task10/cli-adapter — CliAdapter
// What: stdin/stdout turn pipeline: TrollGuard → ingest → assemble → TID → afterTurn
// Why: First channel adapter for Animus — enables local interactive use + integration tests
// How: process_line() builds ChannelContext + TurnEnvelope, runs run_pipeline() through
//      all 5 stages, deposits River events at each stage boundary
// -------------------

use crate::envelope::{ChannelContext, TurnEnvelope};
use crate::rpc_adapter::RpcAdapter;
use crate::tract_writer::TractWriter;
use crate::trollguard::TrollGuardBridge;
use crate::introspection::IntrospectionRelay;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub struct CliAdapter {
    trollguard: Arc<TrollGuardBridge>,
    rpc: Arc<RpcAdapter>,
    tract: Arc<TractWriter>,
    introspection: Arc<IntrospectionRelay>,
    tid_client: reqwest::Client,
    tid_url: String,
}

impl CliAdapter {
    pub fn new(
        trollguard: Arc<TrollGuardBridge>,
        rpc: Arc<RpcAdapter>,
        tract: Arc<TractWriter>,
        introspection: Arc<IntrospectionRelay>,
        tid_url: String,
    ) -> Self {
        let tid_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .expect("failed to build TID HTTP client");
        Self { trollguard, rpc, tract, introspection, tid_client, tid_url }
    }

    /// Process one CLI line as a complete turn. Returns response text.
    pub async fn process_line(&self, line: &str, user_id: &str) -> String {
        if line.trim().is_empty() {
            return String::new();
        }
        let connection_start = now_secs();
        let context = ChannelContext {
            channel_id: "cli".to_string(),
            user_id: user_id.to_string(),
            channel_type: "cli".to_string(),
            connection_start,
        };

        // Deposit channel_connection event (this IS the session)
        self.tract.deposit_event_silent("channel_connection", serde_json::json!({
            "channel_id": "cli",
            "user_id": user_id,
            "channel_type": "cli",
            "connection_start": connection_start,
        }));

        let envelope = TurnEnvelope::new(line, context.clone());
        self.run_pipeline(envelope).await
    }

    async fn run_pipeline(&self, envelope: TurnEnvelope) -> String {
        info!("CLI turn: {:.60}", envelope.text);

        // 1. TrollGuard perimeter
        let scan = self.trollguard.scan(&envelope.text, "animus_cli").await;
        if scan.tg_unavailable {
            warn!("TrollGuard unavailable — proceeding with original text");
        }
        if !scan.is_clean {
            self.tract.deposit_event_silent("tg_block", serde_json::json!({
                "verdict": scan.verdict, "channel_type": "cli"
            }));
            return format!("[TrollGuard blocked: {}]", scan.verdict);
        }
        let clean_text = scan.sanitized_text;

        self.tract.deposit_event_silent("tg_pass", serde_json::json!({
            "verdict": scan.verdict, "channel_type": "cli"
        }));

        // 2. Ingest
        let ingest_result = self.rpc.call("ingest", serde_json::json!({
            "message": {"role": "user", "content": clean_text},
            "_animus_channel_context": {
                "channel_id": envelope.context.channel_id,
                "user_id": envelope.context.user_id,
            }
        })).await;

        if let Err(e) = &ingest_result {
            warn!("Ingest failed: {}", e);
            self.tract.deposit_event_silent("ingest_error", serde_json::json!({
                "error": e.to_string(), "channel_type": "cli"
            }));
        }

        // 3. Introspection context
        let introspection_ctx = self.introspection.format_context(5).await;

        // 4. Assemble
        let assemble_result = self.rpc.call("assemble", serde_json::json!({
            "introspection_context": introspection_ctx
        })).await;

        let system_prompt = assemble_result
            .as_ref()
            .ok()
            .and_then(|r| r.get("systemPromptAddition"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // 5. TID inference (HTTP POST to :7437)
        let response_text = self.call_tid(&clean_text, &system_prompt).await;

        // 6. afterTurn
        let _ = self.rpc.call("afterTurn", serde_json::json!({
            "response": response_text
        })).await;

        self.tract.deposit_event_silent("turn_complete", serde_json::json!({
            "channel_type": "cli", "response_len": response_text.len()
        }));

        response_text
    }

    async fn call_tid(&self, text: &str, system_prompt: &str) -> String {
        let body = serde_json::json!({
            "messages": [{"role": "user", "content": text}],
            "system": system_prompt,
        });

        match self.tid_client.post(format!("{}/chat", self.tid_url)).json(&body).send().await {
            Ok(resp) => {
                resp.json::<serde_json::Value>().await
                    .ok()
                    .and_then(|v| v.get("content").and_then(|c| c.as_str()).map(String::from))
                    .unwrap_or_else(|| "[TID: empty response]".to_string())
            }
            Err(e) => {
                warn!("TID unavailable: {} — no fallback configured", e);
                "[TID unavailable]".to_string()
            }
        }
    }
}
