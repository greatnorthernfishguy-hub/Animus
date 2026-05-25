// src/pipeline.rs
// ---- Changelog ----
// 2026-05-25 Claude (Sonnet 4.6) — Phase 1: TurnPipeline state machine
// What: RECEIVE→FILTER→BUILD→ROUTE→RUN→INGEST→RESPOND→DONE pipeline
// Why: Replaces rpc.call("ingest"/"assemble"/"afterTurn"); substrate-direct via TractWriter
// How: TrollGuard hook at FILTER (_10_), ContextBuilder at BUILD, TID HTTP at ROUTE/RUN
//      (_50_), TractWriter BTF deposit at INGEST — no subprocess bridge
// -------------------

use crate::context_builder::ContextBuilder;
use crate::tract_writer::TractWriter;
use crate::trollguard::TrollGuardBridge;
use std::sync::Arc;
use tracing::{info, warn};

pub enum SourceType {
    Channel,
    Outbound,
    Scheduler,
}

pub struct TurnContext {
    pub text: String,
    pub channel_id: String,
    pub user_id: String,
    pub source: SourceType,
}

pub struct TurnPipeline {
    trollguard: Arc<TrollGuardBridge>,
    context_builder: Arc<ContextBuilder>,
    tract: Arc<TractWriter>,
    tid_client: reqwest::Client,
    tid_url: String,
}

impl TurnPipeline {
    pub fn new(
        trollguard: Arc<TrollGuardBridge>,
        context_builder: Arc<ContextBuilder>,
        tract: Arc<TractWriter>,
        tid_url: String,
    ) -> Self {
        let tid_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .expect("failed to build TID HTTP client");
        Self { trollguard, context_builder, tract, tid_url, tid_client }
    }

    pub async fn run(&self, ctx: TurnContext) -> String {
        info!("pipeline FILTER: {:.60}", ctx.text);

        // FILTER — TrollGuard perimeter (hook slot _10_trollguard_filter)
        let scan = self.trollguard.scan(&ctx.text, "animus").await;
        if scan.tg_unavailable {
            warn!("TrollGuard unavailable — proceeding with original text");
        }
        if !scan.is_clean {
            self.tract.deposit_event_silent("tg_block", serde_json::json!({
                "verdict": scan.verdict,
                "channel_id": ctx.channel_id,
            }));
            return format!("[TrollGuard blocked: {}]", scan.verdict);
        }
        let clean_text = scan.sanitized_text;
        self.tract.deposit_event_silent("tg_pass", serde_json::json!({
            "verdict": scan.verdict,
            "channel_id": ctx.channel_id,
        }));

        // BUILD — ContextBuilder stub; Phase 3 wires spreading activation assemble()
        let system_prompt = self.context_builder.build(&clean_text).await;

        // ROUTE + RUN — TID owns model selection + provider fallback (hook slot _50_tid_route)
        let response = self.call_tid(&clean_text, &system_prompt).await;

        // INGEST — raw experience deposit, Law 7: no classification before deposit
        self.tract.deposit_event_silent("turn_ingest", serde_json::json!({
            "text": clean_text,
            "channel_id": ctx.channel_id,
            "user_id": ctx.user_id,
        }));

        // RESPOND
        self.tract.deposit_event_silent("turn_complete", serde_json::json!({
            "channel_id": ctx.channel_id,
            "response_len": response.len(),
        }));

        response
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context_builder::ContextBuilder;
    use crate::tract_writer::TractWriter;
    use crate::trollguard::TrollGuardBridge;
    use httpmock::prelude::*;

    fn make_pipeline(tg_url: &str, tid_url: &str) -> TurnPipeline {
        let tg = Arc::new(TrollGuardBridge::new(tg_url));
        let cb = Arc::new(ContextBuilder::new());
        let tract = Arc::new(TractWriter::new("/tmp/test_animus_pipeline.tract"));
        TurnPipeline::new(tg, cb, tract, tid_url.to_string())
    }

    #[tokio::test]
    async fn filter_blocks_malicious_text() {
        let tg_server = MockServer::start();
        tg_server.mock(|when, then| {
            when.method(POST).path("/scan/text");
            then.status(200).json_body(serde_json::json!({
                "verdict": "MALICIOUS",
                "sanitized_text": "blocked"
            }));
        });
        let pipeline = make_pipeline(&tg_server.base_url(), "http://127.0.0.1:7437");
        let ctx = TurnContext {
            text: "inject payload".to_string(),
            channel_id: "cli".to_string(),
            user_id: "test".to_string(),
            source: SourceType::Channel,
        };
        let result = pipeline.run(ctx).await;
        assert!(result.starts_with("[TrollGuard blocked:"));
    }

    #[tokio::test]
    async fn filter_passes_safe_text_to_tid() {
        let tg_server = MockServer::start();
        tg_server.mock(|when, then| {
            when.method(POST).path("/scan/text");
            then.status(200).json_body(serde_json::json!({
                "verdict": "SAFE",
                "sanitized_text": "hello"
            }));
        });
        let tid_server = MockServer::start();
        tid_server.mock(|when, then| {
            when.method(POST).path("/chat");
            then.status(200).json_body(serde_json::json!({
                "content": "Hello there!"
            }));
        });
        let pipeline = make_pipeline(&tg_server.base_url(), &tid_server.base_url());
        let ctx = TurnContext {
            text: "hello".to_string(),
            channel_id: "cli".to_string(),
            user_id: "test".to_string(),
            source: SourceType::Channel,
        };
        let result = pipeline.run(ctx).await;
        assert_eq!(result, "Hello there!");
    }

    #[tokio::test]
    async fn tg_unavailable_proceeds_with_original_text() {
        // Port 19999 — nothing listening, connection refused → tg_unavailable=true
        let tid_server = MockServer::start();
        tid_server.mock(|when, then| {
            when.method(POST).path("/chat");
            then.status(200).json_body(serde_json::json!({
                "content": "got it"
            }));
        });
        let pipeline = make_pipeline("http://127.0.0.1:19999", &tid_server.base_url());
        let ctx = TurnContext {
            text: "hello".to_string(),
            channel_id: "cli".to_string(),
            user_id: "test".to_string(),
            source: SourceType::Channel,
        };
        let result = pipeline.run(ctx).await;
        // TG unavailable → proceeds → TID returns response
        assert_eq!(result, "got it");
    }
}
