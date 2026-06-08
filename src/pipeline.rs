// src/pipeline.rs
// ---- Changelog ----
// [2026-06-04] Claude (Opus 4.8) — #293: ConversationHistory persistence (INTERIM mitigation)
// What: ConversationHistory gains optional msgpack sidecar — with_persistence() loads the
//       deque on construction; push_turn() saves after each turn. TurnPipeline::new() takes
//       history_path: Option<String> (Some in prod, None in tests). Path derived in main.rs
//       (ANIMA_HISTORY_PATH env, default ~/.et_modules/shared_learning/conversation_history.msgpack).
// Why:  Restart wiped Syl's last-40 working memory + her routing-mode state (2026-06-04).
//       Syl's call: persist as an INTERIM stopgap until /recall surfacing (#294) is good
//       enough that lost specifics resurface from vdb/topology. Then this is retired.
//       Per MASTER "Patterns to Reject", substrate is the long-term continuity path, not a file.
// How:  rmp_serde of Vec<serde_json::Value>; best-effort load/save (errors log, never panic);
//       capacity still enforced on load. In-memory new() unchanged so tests stay hermetic.
//
// [2026-06-03] Claude (Sonnet 4.6) — Human routing preference detection
// What: detect_routing_preference() scans user text for OSS preference phrases;
//       fire-and-forget POST to TID /routing/preference after INGEST when detected;
//       tid_url field added to TurnPipeline (from AnimaConfig.tid_url via main.rs).
// Why:  Josh can say "use more open source models" and have it register as a TID
//       routing signal. Anima is the natural UX home for this — it sees every turn.
// How:  Keyword match on lowercase text; tokio::spawn fire-and-forget same as afterTurn;
//       TID amplifies open_source_bias for OSS_PREF_DURATION_TURNS routing decisions.
//
// [2026-06-03] Claude (Sonnet 4.6) — Guard empty Ok("") from history and River
// What: AgentResponse::Ok("") (TID null finish_reason + empty content) now skips
//       River deposit and push_turn. Non-empty content only enters history/River.
// Why: TID returns null finish_reason + empty content on transient provider errors.
//      #263's Ok/InfraError split doesn't cover this — an empty string passes the
//      !starts_with("[TID") check but recording "" into history poisons later turns.
// How: if text.is_empty() guard in Ok arm; warn logged; empty still returned to caller.
//
// [2026-06-03] Claude (Sonnet 4.6) — #263: gate POST-RUN on AgentResponse::Ok
// What: Import AgentResponse; match on agent_runner.run() result; River deposit +
//       push_turn only fire on Ok variant; InfraError logs warning and skips both.
// Why: Punchlist #263 — infra error strings polluted ConversationHistory and River.
// How: Replace bare String binding with AgentResponse match; into_text() extracts
//      the display string for the RESPOND path regardless of variant.
//
// [2026-05-31] Claude (Sonnet 4.6) — Anima GUI Task 1: PipelineStatus
// What: Add Stage/StageState/PipelineStatus types; status Arc<Mutex> on TurnPipeline;
//       stage-transition writes in run(); afterTurn spawn updates last_after_turn
// Why: HTTP adapter needs GET /status to expose pipeline state to Anima GUI
// How: Arc<Mutex<PipelineStatus>> shared between run() and future axum State<>
//
// [2026-05-28] Claude (Sonnet 4.6) — Phase 4 Task 2: ConversationHistory + afterTurn
// What: Add ConversationHistory (Mutex<VecDeque>, cap 40); TurnPipeline gains history,
//       ng_client, ng_url; run() builds history snapshot → NG assemble → KISS messages;
//       POST-RUN deposits turn_exchange + push_turn; afterTurn fire-and-forget.
// Why: Phase 4 spec — closes substrate loop: working memory + River write + STDP trigger.
// How: snapshot_with() builds full context slice pre-BUILD; push_turn() records exchange
//      post-RUN; tokio::spawn for afterTurn so response latency is unaffected.
//
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
// [2026-05-28] Claude (Sonnet 4.6) — Phase 4 Task 1: minimal AssembleResult bridge
// What: Update BUILD to call build(&[Value]) and use assembled.messages + .system_prompt
// Why: context_builder::build() now returns AssembleResult; caller must consume both fields
// How: Wrap clean_text in single-element slice; use assembled fields in AgentRunSpec
//
// 2026-05-25 Claude (Sonnet 4.6) — Phase 1: TurnPipeline state machine
// What: RECEIVE→FILTER→BUILD→ROUTE→RUN→INGEST→RESPOND→DONE pipeline
// Why: Replaces rpc.call("ingest"/"assemble"/"afterTurn"); substrate-direct via TractWriter
// How: TrollGuard hook at FILTER (_10_), ContextBuilder at BUILD, TID HTTP at ROUTE/RUN
//      (_50_), TractWriter BTF deposit at INGEST — no subprocess bridge
// -------------------

use crate::agent_runner::{AgentResponse, AgentRunSpec, AgentRunner};
use crate::context_builder::ContextBuilder;
use crate::tract_writer::TractWriter;
use crate::trollguard::TrollGuardBridge;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

const HISTORY_CAPACITY: usize = 40;

// Defaults for the OSS routing preference signal sent to TID.
// OSS_PREF_BOOST replaces (not compounds) TID's config.open_source_bias (default 0.02).
// 0.12 is enough to reliably favor OSS models over closed equals without overriding
// genuine quality differences — closed models still win when scoring better overall.
const OSS_PREF_BOOST: f64 = 0.12;
const OSS_PREF_DURATION_TURNS: u32 = 20;

/// Returns true when the user turn expresses a routing preference toward open-source models.
/// Uses conservative, unambiguous substring matching — prefers false negatives over false
/// positives (a missed preference is harmless; a spurious one would confuse the user).
fn detect_routing_preference(text: &str) -> bool {
    let lower = text.to_lowercase();
    let patterns = [
        "more open source", "more open-source", "more opensource",
        "use open source", "use open-source",
        "prefer open source", "prefer open-source",
        "open source model", "open-source model",
        "more oss", "use oss", "prefer oss",
        "open weights", "open-weights",
        "open weight model", "open-weight model",
        "try open source", "switch to open source",
        "more free model", "more open model",
    ];
    patterns.iter().any(|p| lower.contains(p))
}

struct ConversationHistory {
    inner: Mutex<VecDeque<serde_json::Value>>,
    capacity: usize,
    /// msgpack sidecar for restart persistence (#293, interim). None = in-memory only (tests).
    path: Option<PathBuf>,
}

impl ConversationHistory {
    fn new(capacity: usize) -> Self {
        Self { inner: Mutex::new(VecDeque::new()), capacity, path: None }
    }

    /// Construct with a persistence sidecar and load any prior deque from it.
    fn with_persistence(capacity: usize, path: PathBuf) -> Self {
        let h = Self { inner: Mutex::new(VecDeque::new()), capacity, path: Some(path) };
        h.load();
        h
    }

    /// Best-effort load of the deque from the msgpack sidecar. No-op if absent/unset.
    /// Capacity is enforced after load (trim oldest) so a shrunk capacity still holds.
    fn load(&self) {
        let Some(path) = self.path.as_ref() else { return };
        if !path.exists() {
            return;
        }
        match std::fs::read(path) {
            Ok(bytes) => match rmp_serde::from_slice::<Vec<serde_json::Value>>(&bytes) {
                Ok(msgs) => {
                    let mut guard = self.inner.lock().unwrap();
                    *guard = msgs.into_iter().collect();
                    while guard.len() > self.capacity {
                        guard.pop_front();
                    }
                    info!("ConversationHistory: loaded {} messages from {}", guard.len(), path.display());
                }
                Err(e) => warn!("ConversationHistory: corrupt sidecar {} ({}) — starting empty", path.display(), e),
            },
            Err(e) => warn!("ConversationHistory: cannot read {} ({}) — starting empty", path.display(), e),
        }
    }

    /// Best-effort save of the current deque to the msgpack sidecar. No-op if unset.
    fn save(&self) {
        let Some(path) = self.path.as_ref() else { return };
        let snapshot: Vec<serde_json::Value> = {
            let guard = self.inner.lock().unwrap();
            guard.iter().cloned().collect()
        };
        match rmp_serde::to_vec(&snapshot) {
            Ok(bytes) => {
                if let Err(e) = std::fs::write(path, bytes) {
                    warn!("ConversationHistory: save to {} failed: {}", path.display(), e);
                }
            }
            Err(e) => warn!("ConversationHistory: msgpack encode failed: {}", e),
        }
    }

    fn snapshot_with(&self, user_text: &str) -> Vec<serde_json::Value> {
        let guard = self.inner.lock().unwrap();
        let mut msgs: Vec<serde_json::Value> = guard.iter().cloned().collect();
        msgs.push(serde_json::json!({"role": "user", "content": user_text}));
        msgs
    }

    fn push_turn(&self, user_text: &str, assistant_text: &str) {
        {
            let mut guard = self.inner.lock().unwrap();
            guard.push_back(serde_json::json!({"role": "user", "content": user_text}));
            guard.push_back(serde_json::json!({"role": "assistant", "content": assistant_text}));
            while guard.len() > self.capacity {
                guard.pop_front();
            }
        }
        // Persist after every turn (deque is tiny — ≤40 short messages; write is sub-ms).
        self.save();
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum Stage {
    #[serde(rename = "IDLE")]       Idle,
    #[serde(rename = "FILTER")]     Filter,
    #[serde(rename = "INGEST")]     Ingest,
    #[serde(rename = "BUILD")]      Build,
    #[serde(rename = "RUN")]        Run,
    #[serde(rename = "POST-RUN")]   PostRun,
    #[serde(rename = "AFTER-TURN")] AfterTurn,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StageState {
    Idle,
    Running,
    Done,
    Error,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PipelineStatus {
    pub stage: Stage,
    pub stage_state: StageState,
    pub last_tg_verdict: String,
    pub last_after_turn: String,
}

impl Default for PipelineStatus {
    fn default() -> Self {
        Self {
            stage: Stage::Idle,
            stage_state: StageState::Idle,
            last_tg_verdict: "unknown".to_string(),
            last_after_turn: "unknown".to_string(),
        }
    }
}

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

/// Read + clear a one-shot credit-notice file (written by TID on a funded 402).
/// Returns the notice text, or None if absent/malformed. Delivered once. [2026-06-07]
fn read_credit_notice_file(path: &str) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let _ = std::fs::remove_file(path);
    serde_json::from_str::<serde_json::Value>(&raw)
        .ok()
        .and_then(|v| v.get("text").and_then(|t| t.as_str()).map(String::from))
}

pub struct TurnPipeline {
    trollguard: Arc<TrollGuardBridge>,
    context_builder: Arc<ContextBuilder>,
    tract: Arc<TractWriter>,
    agent_runner: Arc<AgentRunner>,
    history: ConversationHistory,
    ng_client: reqwest::Client,
    ng_url: String,
    tid_url: String,
    pub status: Arc<Mutex<PipelineStatus>>,
    /// One-shot system notice (e.g. credit-critical) set by BudgetMonitor on the
    /// critical-edge; taken onto the next turn's response by the channel adapter.
    /// [2026-06-07] Feature B — out-of-credits notice to the live channel.
    pending_notice: Arc<Mutex<Option<String>>>,
}

impl TurnPipeline {
    pub fn new(
        trollguard: Arc<TrollGuardBridge>,
        context_builder: Arc<ContextBuilder>,
        tract: Arc<TractWriter>,
        agent_runner: Arc<AgentRunner>,
        ng_url: String,
        tid_url: String,
        history_path: Option<String>,
    ) -> Self {
        let ng_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build pipeline HTTP client");
        let history = match history_path {
            Some(p) => ConversationHistory::with_persistence(HISTORY_CAPACITY, PathBuf::from(p)),
            None => ConversationHistory::new(HISTORY_CAPACITY),
        };
        Self {
            trollguard,
            context_builder,
            tract,
            agent_runner,
            history,
            ng_client,
            ng_url,
            tid_url,
            status: Arc::new(Mutex::new(PipelineStatus::default())),
            pending_notice: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns a snapshot of conversation history for the HTTP adapter's GET /history handler.
    pub fn history_snapshot(&self) -> Vec<serde_json::Value> {
        self.history.inner.lock().unwrap().iter().cloned().collect()
    }

    /// Handle to the shared credit-notice cell — BudgetMonitor sets it on the
    /// critical-edge; the channel adapter take()s it onto the next turn. [2026-06-07]
    pub fn pending_notice_handle(&self) -> Arc<Mutex<Option<String>>> {
        Arc::clone(&self.pending_notice)
    }

    /// Take any pending system notice to surface on this turn's response,
    /// clearing it so it is delivered exactly once. Two sources: the in-process
    /// Arc (BudgetMonitor critical-edge / future management-key path) and a
    /// cross-process file written by TID on a funded 402 (reactive ground-truth
    /// out-of-credits — Feature B option C). [2026-06-07]
    pub fn take_pending_notice(&self) -> Option<String> {
        if let Some(n) = self.pending_notice.lock().ok().and_then(|mut g| g.take()) {
            return Some(n);
        }
        let path = std::env::var("HOME")
            .ok()
            .map(|h| format!("{}/.et_modules/shared_learning/credit_notice.json", h))?;
        read_credit_notice_file(&path)
    }

    pub async fn run(&self, ctx: TurnContext) -> String {
        info!("pipeline FILTER: {:.60}", ctx.text);

        // FILTER — TrollGuard perimeter (hook slot _10_trollguard_filter)
        { let mut s = self.status.lock().unwrap(); s.stage = Stage::Filter; s.stage_state = StageState::Running; }
        let scan = self.trollguard.scan(&ctx.text, "anima").await;
        if scan.tg_unavailable {
            warn!("TrollGuard unavailable — proceeding with original text");
        }
        if !scan.is_clean {
            { let mut s = self.status.lock().unwrap(); s.stage = Stage::Idle; s.stage_state = StageState::Idle; }
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
        {
            let mut s = self.status.lock().unwrap();
            s.stage_state = StageState::Done;
            s.last_tg_verdict = pass_event.to_string();
        }

        // INGEST — raw experience pre-BUILD (Law 7: substrate receives input before it is acted upon)
        { let mut s = self.status.lock().unwrap(); s.stage = Stage::Ingest; s.stage_state = StageState::Running; }
        self.tract.deposit_experience_silent("anima", clean_text.as_bytes());
        { let mut s = self.status.lock().unwrap(); s.stage_state = StageState::Done; }

        // PREFERENCE SIGNAL — detect natural language routing preferences and notify TID
        // fire-and-forget, same as afterTurn; never blocks the pipeline
        if detect_routing_preference(&clean_text) {
            let tid_url = self.tid_url.clone();
            let pref_client = self.ng_client.clone();
            tokio::spawn(async move {
                let body = serde_json::json!({
                    "oss_boost": OSS_PREF_BOOST,
                    "duration_turns": OSS_PREF_DURATION_TURNS,
                    "note": "User requested more open-source model usage",
                });
                match pref_client
                    .post(format!("{}/routing/preference", tid_url))
                    .json(&body)
                    .send()
                    .await
                {
                    Ok(_) => info!("Routing preference signal sent to TID"),
                    Err(e) => warn!("Failed to send routing preference to TID: {}", e),
                }
            });
        }

        // BUILD — full conversation history → NG assemble (spreading activation + KISS truncation)
        { let mut s = self.status.lock().unwrap(); s.stage = Stage::Build; s.stage_state = StageState::Running; }
        let messages = self.history.snapshot_with(&clean_text);
        let assembled = self.context_builder.build(&messages).await;
        { let mut s = self.status.lock().unwrap(); s.stage_state = StageState::Done; }

        // ROUTE + RUN — KISS-truncated messages to TID
        { let mut s = self.status.lock().unwrap(); s.stage = Stage::Run; s.stage_state = StageState::Running; }
        let agent_response = self.agent_runner.run(AgentRunSpec {
            messages: assembled.messages,
            system_prompt: assembled.system_prompt,
        }).await;
        { let mut s = self.status.lock().unwrap(); s.stage_state = StageState::Done; }

        // POST-RUN — deposit exchange to River; update working memory.
        // Gated on AgentResponse::Ok — infra errors are not Syl turns and must not
        // enter ConversationHistory or the River substrate.
        { let mut s = self.status.lock().unwrap(); s.stage = Stage::PostRun; s.stage_state = StageState::Running; }
        let response = match agent_response {
            AgentResponse::Ok(text) => {
                if text.is_empty() {
                    warn!("TID returned empty content — skipping River deposit and history");
                } else {
                    self.tract.deposit_event_silent("turn_exchange", serde_json::json!({
                        "user": clean_text,
                        "assistant": text,
                        "channel_id": ctx.channel_id,
                    }));
                    self.history.push_turn(&clean_text, &text);
                }
                text
            }
            AgentResponse::InfraError(msg) => {
                warn!("agent_runner infra error — skipping River deposit and history: {}", msg);
                msg
            }
        };
        { let mut s = self.status.lock().unwrap(); s.stage_state = StageState::Done; }

        // AFTER-TURN — fire-and-forget NG graph.step() + STDP + _anticipate(); response unblocked
        { let mut s = self.status.lock().unwrap(); s.stage = Stage::AfterTurn; s.stage_state = StageState::Running; }
        let ng_url = self.ng_url.clone();
        let user_text = clean_text.clone();
        let ng_client = self.ng_client.clone();
        let status_arc = Arc::clone(&self.status);
        tokio::spawn(async move {
            let body = serde_json::json!({
                "lastUserMessage": {"role": "user", "content": user_text}
            });
            match ng_client
                .post(format!("{}/afterTurn", ng_url))
                .json(&body)
                .send()
                .await
            {
                Err(e) => {
                    tracing::warn!("afterTurn fire failed: {}", e);
                    let mut s = status_arc.lock().unwrap();
                    s.last_after_turn = "failed".to_string();
                    s.stage = Stage::Idle;
                    s.stage_state = StageState::Idle;
                }
                Ok(_) => {
                    let mut s = status_arc.lock().unwrap();
                    s.last_after_turn = "ok".to_string();
                    s.stage = Stage::Idle;
                    s.stage_state = StageState::Idle;
                }
            }
        });

        // RESPOND — reset stage synchronously so status is useful before spawn resolves
        { let mut s = self.status.lock().unwrap(); s.stage = Stage::Idle; s.stage_state = StageState::Idle; }
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

    #[test]
    fn credit_notice_file_read_and_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credit_notice.json");
        std::fs::write(&path, r#"{"text":"openrouter out of credits","provider":"openrouter"}"#).unwrap();
        let got = read_credit_notice_file(path.to_str().unwrap());
        assert_eq!(got.as_deref(), Some("openrouter out of credits"));
        assert!(!path.exists());
        assert!(read_credit_notice_file(path.to_str().unwrap()).is_none());
    }

    fn make_pipeline(tg_url: &str, tid_url: &str) -> TurnPipeline {
        let tg = Arc::new(TrollGuardBridge::new(tg_url));
        // Port 1 = always connection-refused; ContextBuilder returns fallback AssembleResult gracefully
        let cb = Arc::new(ContextBuilder::new("http://127.0.0.1:1".to_string()));
        let tract = Arc::new(TractWriter::new("/tmp/test_animus_pipeline.tract"));
        let dispatcher = Arc::new(ToolDispatcher::from_env());
        let runner = Arc::new(AgentRunner::new(dispatcher, tid_url.to_string(), 8));
        // Port 1 for ng_url — afterTurn fires and fails silently (fire-and-forget by design)
        TurnPipeline::new(tg, cb, tract, runner, "http://127.0.0.1:1".to_string(), tid_url.to_string(), None)
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

    #[tokio::test]
    async fn history_snapshot_with_empty_history() {
        let h = ConversationHistory::new(40);
        let snap = h.snapshot_with("hello");
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0]["role"], "user");
        assert_eq!(snap[0]["content"], "hello");
    }

    #[tokio::test]
    async fn history_push_turn_appears_in_next_snapshot() {
        let h = ConversationHistory::new(40);
        h.push_turn("hi", "hello there");
        let snap = h.snapshot_with("next");
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0]["role"], "user");
        assert_eq!(snap[0]["content"], "hi");
        assert_eq!(snap[1]["role"], "assistant");
        assert_eq!(snap[1]["content"], "hello there");
        assert_eq!(snap[2]["role"], "user");
        assert_eq!(snap[2]["content"], "next");
    }

    #[tokio::test]
    async fn history_evicts_oldest_when_over_capacity() {
        // capacity=4 stores 4 messages (2 turns). A 3rd push_turn evicts the oldest pair.
        let h = ConversationHistory::new(4);
        h.push_turn("u1", "a1");
        h.push_turn("u2", "a2");
        h.push_turn("u3", "a3"); // evicts u1+a1 → stored: [u2,a2,u3,a3]
        let snap = h.snapshot_with("u4"); // [u2,a2,u3,a3,u4]
        assert_eq!(snap.len(), 5);
        assert_eq!(snap[0]["content"], "u2");
    }

    // ---- #293: ConversationHistory persistence (interim) ----

    #[test]
    fn history_persists_and_reloads_across_instances() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conversation_history.msgpack");

        // First instance: record two turns, which saves on each push_turn.
        let h1 = ConversationHistory::with_persistence(40, path.clone());
        h1.push_turn("hello", "hi there");
        h1.push_turn("how are you", "well, thanks");
        drop(h1);

        // Second instance (simulates a restart): loads the prior deque from disk.
        let h2 = ConversationHistory::with_persistence(40, path.clone());
        let snap = h2.snapshot_with("next");
        // 2 turns = 4 messages + the new user message = 5
        assert_eq!(snap.len(), 5);
        assert_eq!(snap[0]["content"], "hello");
        assert_eq!(snap[1]["content"], "hi there");
        assert_eq!(snap[3]["content"], "well, thanks");
        assert_eq!(snap[4]["content"], "next");
    }

    #[test]
    fn history_load_enforces_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conversation_history.msgpack");

        // Save 3 turns (6 messages) under a generous capacity.
        let h1 = ConversationHistory::with_persistence(40, path.clone());
        h1.push_turn("u1", "a1");
        h1.push_turn("u2", "a2");
        h1.push_turn("u3", "a3");
        drop(h1);

        // Reload under capacity=2 — only the newest 2 messages survive.
        let h2 = ConversationHistory::with_persistence(2, path.clone());
        let snap = h2.snapshot_with("u4");
        assert_eq!(snap.len(), 3); // 2 retained + new user msg
        assert_eq!(snap[0]["content"], "u3");
        assert_eq!(snap[1]["content"], "a3");
    }

    #[test]
    fn history_load_missing_file_is_empty_no_panic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does_not_exist.msgpack");
        let h = ConversationHistory::with_persistence(40, path);
        assert_eq!(h.snapshot_with("only").len(), 1); // just the new user msg
    }

    #[test]
    fn history_in_memory_new_does_not_write() {
        // new() (no path) must never touch disk — keeps tests hermetic.
        let h = ConversationHistory::new(40);
        h.push_turn("u", "a");
        assert!(h.path.is_none());
    }

    #[tokio::test]
    async fn history_accumulates_across_turns() {
        let h = ConversationHistory::new(40);
        h.push_turn("a", "A");
        h.push_turn("b", "B");
        let snap = h.snapshot_with("c");
        // 2 push_turns = 4 stored messages + 1 pending user = 5
        assert_eq!(snap.len(), 5);
    }

    #[test]
    fn pipeline_status_initially_idle() {
        let p = make_pipeline("http://127.0.0.1:1", "http://127.0.0.1:1");
        let s = p.status.lock().unwrap();
        assert!(matches!(s.stage, Stage::Idle));
        assert!(matches!(s.stage_state, StageState::Idle));
        assert_eq!(s.last_tg_verdict, "unknown");
        assert_eq!(s.last_after_turn, "unknown");
    }

    #[test]
    fn pipeline_status_serializes_stage_names() {
        let stage = Stage::AfterTurn;
        let json = serde_json::to_string(&stage).unwrap();
        assert_eq!(json, "\"AFTER-TURN\"");
        let stage2 = Stage::PostRun;
        assert_eq!(serde_json::to_string(&stage2).unwrap(), "\"POST-RUN\"");
    }

    #[test]
    fn history_snapshot_initially_empty() {
        let p = make_pipeline("http://127.0.0.1:1", "http://127.0.0.1:1");
        assert!(p.history_snapshot().is_empty());
    }

    #[tokio::test]
    async fn empty_tid_response_does_not_poison_history() {
        let tg_server = MockServer::start();
        tg_server.mock(|when, then| {
            when.method(POST).path("/scan/text");
            then.status(200).json_body(serde_json::json!({
                "verdict": "SAFE", "sanitized_text": "hello"
            }));
        });
        let tid_server = MockServer::start();
        // TID returns null finish_reason + empty content (transient provider error)
        tid_server.mock(|when, then| {
            when.method(POST).path("/v1/chat/completions");
            then.status(200).json_body(serde_json::json!({
                "id": "chatcmpl-test",
                "object": "chat.completion",
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": null},
                    "finish_reason": null
                }],
                "usage": {"prompt_tokens": 5, "completion_tokens": 0, "total_tokens": 5}
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
        // Empty string returned to caller but NOT pushed to history
        assert_eq!(result, "");
        assert!(pipeline.history_snapshot().is_empty(), "empty response must not enter history");
    }

    #[test]
    fn history_snapshot_reflects_push_turn() {
        let h = ConversationHistory::new(40);
        h.push_turn("hello", "hi there");
        // Access via inner directly (same module)
        let snap: Vec<serde_json::Value> = h.inner.lock().unwrap().iter().cloned().collect();
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0]["role"], "user");
        assert_eq!(snap[0]["content"], "hello");
        assert_eq!(snap[1]["role"], "assistant");
        assert_eq!(snap[1]["content"], "hi there");
    }
}
