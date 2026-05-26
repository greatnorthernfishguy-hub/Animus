// src/pipeline.rs
// ---- Changelog ----
// 2026-05-25 Claude (Sonnet 4.6) — Phase 2: wire AgentRunner, fix TID endpoint + format
// What: Replace tid_client/tid_url + call_tid() with Arc<AgentRunner>
// Why: call_tid() called POST /chat (404 in prod) with "system" field (ignored by TID).
//      AgentRunner fixes: /v1/chat/completions, system as {role:"system"} message.
// How: TurnPipeline::new() takes Arc<AgentRunner>; run() constructs AgentRunSpec + delegates
//
// 2026-05-25 Claude (Sonnet 4.6) — Fix SUSPICIOUS collapse + test port hardcoding
// What: Split tg_pass/tg_suspicious deposits by verdict; replace hardcoded TID port in test
// Why: SUSPICIOUS verdict was silently collapsed into tg_pass, hiding signal from River consumers
//      (Law 7 — substrate must receive richest possible signal, not flattened labels).
//      Hardcoded port 7437 in filter_blocks_malicious_text test is inconsistent with other tests.
// How: pass_event chosen per verdict; test uses unused MockServer for a valid base_url
//
// 2026-05-25 Claude (Sonnet 4.6) — Fix INGEST ordering + test port reliability
// What: Move turn_ingest deposit to pre-BUILD; replace hardcoded port 19999 with MockServer drop pattern
// Why: Substrate must receive raw input before it is acted upon (Law 7). Phase 3 spreading
//      activation reads from the substrate during BUILD — input must be there first.
//      Port 19999 could be in use; MockServer start+drop guarantees a free closed port.
// How: Deposit turn_ingest immediately after FILTER/tg_pass, before ContextBuilder.build()
//
// 2026-05-25 Claude (Sonnet 4.6) — Phase 1: TurnPipeline state machine
// What: RECEIVE→FILTER→BUILD→ROUTE→RUN→INGEST→RESPOND→DONE pipeline
// Why: Replaces rpc.call("ingest"/"assemble"/"afterTurn"); substrate-direct via TractWriter
// How: TrollGuard hook at FILTER (_10_), ContextBuilder at BUILD, TID HTTP at ROUTE/RUN
//      (_50_), TractWriter BTF deposit at INGEST — no subprocess bridge
// -------------------

use crate::agent_runner::{AgentRunSpec, AgentRunner};
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
    agent_runner: Arc<AgentRunner>,
}

impl TurnPipeline {
    pub fn new(
        trollguard: Arc<TrollGuardBridge>,
        context_builder: Arc<ContextBuilder>,
        tract: Arc<TractWriter>,
        agent_runner: Arc<AgentRunner>,
    ) -> Self {
        Self { trollguard, context_builder, tract, agent_runner }
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
        let pass_event = if scan.verdict == "SUSPICIOUS" { "tg_suspicious" } else { "tg_pass" };
        self.tract.deposit_event_silent(pass_event, serde_json::json!({
            "verdict": scan.verdict,
            "channel_id": ctx.channel_id,
        }));

        // INGEST — raw experience deposit pre-BUILD, Law 7: substrate receives input before it is
        // acted upon. Phase 3 spreading activation reads from the substrate during BUILD — the
        // current turn's text must already be there.
        self.tract.deposit_event_silent("turn_ingest", serde_json::json!({
            "text": clean_text,
            "channel_id": ctx.channel_id,
            "user_id": ctx.user_id,
        }));

        // BUILD — ContextBuilder stub; Phase 3 wires spreading activation assemble()
        let system_prompt = self.context_builder.build(&clean_text).await;

        // ROUTE + RUN — TID owns model selection + provider fallback (hook slot _50_tid_route)
        let response = self.agent_runner.run(AgentRunSpec {
            messages: vec![serde_json::json!({"role": "user", "content": clean_text})],
            system_prompt,
        }).await;

        // RESPOND
        self.tract.deposit_event_silent("turn_complete", serde_json::json!({
            "channel_id": ctx.channel_id,
            "response_len": response.len(),
        }));

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_runner::AgentRunner;
    use crate::context_builder::ContextBuilder;
    use crate::tool_dispatcher::ToolDispatcher;
    use crate::tract_writer::TractWriter;
    use crate::trollguard::TrollGuardBridge;
    use httpmock::prelude::*;

    fn make_pipeline(tg_url: &str, tid_url: &str) -> TurnPipeline {
        let tg = Arc::new(TrollGuardBridge::new(tg_url));
        // Port 1 = always connection-refused; ContextBuilder returns "" gracefully (stub parity)
        let cb = Arc::new(ContextBuilder::new("http://127.0.0.1:1".to_string()));
        let tract = Arc::new(TractWriter::new("/tmp/test_animus_pipeline.tract"));
        let dispatcher = Arc::new(ToolDispatcher::from_env());
        let runner = Arc::new(AgentRunner::new(dispatcher, tid_url.to_string(), 8));
        TurnPipeline::new(tg, cb, tract, runner)
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
        let tid_server = MockServer::start(); // TID never called — FILTER blocks first
        let pipeline = make_pipeline(&tg_server.base_url(), &tid_server.base_url());
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
            when.method(POST).path("/v1/chat/completions");
            then.status(200).json_body(serde_json::json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "Hello there!"},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10}
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
        let closed_port = {
            let s = MockServer::start();
            s.port()
        };
        let tid_server = MockServer::start();
        tid_server.mock(|when, then| {
            when.method(POST).path("/v1/chat/completions");
            then.status(200).json_body(serde_json::json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": "got it"},
                    "finish_reason": "stop"
                }],
                "usage": {"prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10}
            }));
        });
        let pipeline = make_pipeline(
            &format!("http://127.0.0.1:{}", closed_port),
            &tid_server.base_url(),
        );
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
