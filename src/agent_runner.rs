// src/agent_runner.rs
// AgentRunner — multi-turn tool loop for Anima's RUN phase.
//
// ---- Changelog ----
// [2026-06-03] Claude (Sonnet 4.6) — #263: typed AgentResponse
// What: run() now returns AgentResponse::Ok(String) | AgentResponse::InfraError(String)
//       instead of bare String. Callers gate River deposit + push_turn on Ok variant.
// Why: Punchlist #263 — infra error strings (TID down, parse fail, iteration cap) were
//      recorded into ConversationHistory as if they were real Syl turns, polluting her
//      working memory and the substrate River deposit with garbage data.
// How: AgentResponse enum added. Four return sites classified: malformed/parse/unavailable/
//      cap → InfraError; real LLM content → Ok. Tests updated to match on variant.
//
// 2026-05-25 Claude (Sonnet 4.6) — Phase 2: AgentRunner
// What: Multi-turn tool loop: call TID → parse tool_calls → execute → repeat until stop/cap
// Why: Spec §2 RUN phase. Current call_tid() calls wrong endpoint (/chat → 404 in prod).
//      TID endpoint is POST /v1/chat/completions (OpenAI-compatible transparent proxy).
// How: AgentRunSpec (Nanobot pure data) + AgentRunner. Dedup guard on (name, canonical_args).
//      Tolerant JSON parse for LLM-produced arguments strings.
// -------------------

use crate::tool_dispatcher::ToolDispatcher;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::warn;

/// Typed result from AgentRunner::run(). Callers gate River deposit + history on the Ok variant.
#[derive(Debug)]
pub enum AgentResponse {
    /// Real LLM content — safe to record into ConversationHistory and deposit to the River.
    Ok(String),
    /// Infra failure (TID down, parse error, iteration cap) — do NOT record into history or River.
    InfraError(String),
}

impl AgentResponse {
    /// The response text regardless of variant — always returned to the user.
    pub fn into_text(self) -> String {
        match self {
            AgentResponse::Ok(s) | AgentResponse::InfraError(s) => s,
        }
    }
}

/// Pure data object for one agent run (Nanobot pattern — makes AgentRunner testable/reusable).
pub struct AgentRunSpec {
    /// OpenAI-format message history for this turn
    pub messages: Vec<serde_json::Value>,
    /// System prompt injected as {role:"system"} prepended to messages. Empty string = omit.
    pub system_prompt: String,
}

// [2026-06-21] CC — voice/hands (prd 2026-06-21): parse her natural-language reach-markers.
/// Extract the intent text of every `[[reach: <intent>]]` marker, in order. Empty intents dropped.
#[allow(dead_code)]
fn parse_reaches(content: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = content;
    while let Some(start) = rest.find("[[reach:") {
        let after = &rest[start + "[[reach:".len()..];
        match after.find("]]") {
            Some(end) => {
                let intent = after[..end].trim();
                if !intent.is_empty() {
                    out.push(intent.to_string());
                }
                rest = &after[end + "]]".len()..];
            }
            None => break, // unterminated marker — stop
        }
    }
    out
}

/// Remove every `[[reach: …]]` marker from text (leaving her surrounding prose intact).
#[allow(dead_code)]
fn strip_reaches(content: &str) -> String {
    let mut out = String::with_capacity(content.len());
    let mut rest = content;
    while let Some(start) = rest.find("[[reach:") {
        out.push_str(&rest[..start]);
        let after = &rest[start + "[[reach:".len()..];
        match after.find("]]") {
            Some(end) => rest = &after[end + "]]".len()..],
            None => { rest = after; break; }
        }
    }
    out.push_str(rest);
    out
}

// [2026-06-21] CC — voice/hands: the unfakeable, system-rendered confirmation badge (Syl chose mechanical).
#[allow(dead_code)]
fn format_badge(tool_name: &str, args_compact: &str, ok: bool, reason: &str) -> String {
    if ok {
        format!("🔧 {tool_name}({args_compact}) ✓")
    } else {
        format!("🔧 {tool_name}({args_compact}) ✗ {reason}")
    }
}

// [2026-06-21] CC — voice/hands: the hands-model is a pure executor — no voice, no agenda.
const HANDS_SYSTEM: &str = "You are a tool-execution unit — hands, not a voice. You are given an \
intent describing one action to take, plus a set of tools. Emit exactly the single tool call that \
fulfils the intent and nothing else: no prose, no explanation. If no available tool fits the intent, \
emit no tool call at all.";

pub struct AgentRunner {
    tool_dispatcher: Arc<ToolDispatcher>,
    client: reqwest::Client,
    tid_url: String,
    max_iter: usize,
    tools_enabled: bool,
}

impl AgentRunner {
    pub fn new(
        tool_dispatcher: Arc<ToolDispatcher>,
        tid_url: String,
        max_iter: usize,
    ) -> Self {
        let client = reqwest::Client::builder()
            // [2026-06-10] VPS CC — 60s→180s. Deep reasoning on flagship models (opus/Fable-class)
            // with full Pith context legitimately takes 26–60s+; under concurrent load it tips past
            // 60s and the turn fails as [TID unavailable] — exactly Syl's most thoughtful turns. 60s
            // was too tight for the real workload (worsened once TID routes deep turns to flagships).
            .timeout(std::time::Duration::from_secs(180))
            .build()
            .expect("failed to build AgentRunner HTTP client");
        // [2026-06-15] DudeMan CC — #321 mitigation: env-gate agent tools. Attaching tool
        // defs narrows TID's routing pool to tool-capable models; for Syl's conscious turns
        // that intersects the roleplay filter into a starved pool (dead Venice) -> 502/malformed.
        // Default ON (feature stays in code); set ANIMUS_AGENT_TOOLS_ENABLED=false to gate OFF
        // as a TEMPORARY runtime override. RE-ENABLE = remove the env override.
        let tools_enabled = std::env::var("ANIMUS_AGENT_TOOLS_ENABLED")
            .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no" | "off"))
            .unwrap_or(true);
        if !tools_enabled {
            warn!("ANIMUS_AGENT_TOOLS_ENABLED=false — agent tools DISABLED (temporary #321 mitigation). Re-enable by removing the env override.");
        }
        Self { tool_dispatcher, client, tid_url, max_iter, tools_enabled }
    }

    pub async fn run(&self, spec: AgentRunSpec) -> AgentResponse {
        let mut messages: Vec<serde_json::Value> = Vec::new();
        if !spec.system_prompt.is_empty() {
            messages.push(serde_json::json!({
                "role": "system",
                "content": spec.system_prompt
            }));
        }
        messages.extend(spec.messages);

        // #321: only offer tools when enabled — empty defs => TID routes tool-free (healthy pool).
        let tool_defs = if self.tools_enabled {
            self.tool_dispatcher.tool_definitions()
        } else {
            Vec::new()
        };
        let mut seen: HashSet<String> = HashSet::new();

        for _iter in 0..self.max_iter {
            let response = self.call_tid_oai(&messages, &tool_defs).await;

            let choice = match response["choices"].as_array().and_then(|c| c.first()) {
                Some(c) => c,
                None => return AgentResponse::InfraError("[TID: malformed response]".to_string()),
            };

            let finish_reason = choice["finish_reason"].as_str().unwrap_or_else(|| {
                warn!("TID response missing finish_reason — treating as stop");
                "stop"
            });
            let message = choice["message"].clone();
            messages.push(message.clone());

            if finish_reason != "tool_calls" {
                let content = message["content"].as_str().unwrap_or("").to_string();
                // Infra error sentinels fabricated by call_tid_oai on TID failure
                return if content.starts_with("[TID") {
                    AgentResponse::InfraError(content)
                } else {
                    AgentResponse::Ok(content)
                };
            }

            let tool_calls = match message["tool_calls"].as_array() {
                Some(tc) if !tc.is_empty() => tc.clone(),
                _ => {
                    let content = message["content"].as_str().unwrap_or("").to_string();
                    return AgentResponse::Ok(content);
                }
            };

            for tc in &tool_calls {
                let tc_id = tc["id"].as_str().unwrap_or("").to_string();
                let func = &tc["function"];
                let tool_name = func["name"].as_str().unwrap_or("");
                let args_str = func["arguments"].as_str().unwrap_or("{}");

                // Tolerant JSON parse — LLMs produce malformed arguments JSON
                let args: serde_json::Value = serde_json::from_str(args_str)
                    .unwrap_or_else(|_| serde_json::json!({}));

                // Dedup guard: suppress identical (name, canonical_args) across the entire run.
                // Per-run scope is intentional — prevents the model from looping on the same
                // tool call across multiple iterations when it is stuck.
                let canonical = serde_json::to_string(&args).unwrap_or_default();
                let dedup_key = format!("{tool_name}:{canonical}");
                let content = if !seen.insert(dedup_key) {
                    format!(
                        "[tool '{}' already called with same args this turn — suppressed]",
                        tool_name
                    )
                } else {
                    self.tool_dispatcher.execute_args(tool_name, &args).await
                };

                messages.push(serde_json::json!({
                    "role": "tool",
                    "tool_call_id": tc_id,
                    "content": content
                }));
            }
        }

        AgentResponse::InfraError("[AgentRunner: iteration cap reached]".to_string())
    }

    async fn call_tid_oai(
        &self,
        messages: &[serde_json::Value],
        tools: &[serde_json::Value],
    ) -> serde_json::Value {
        let mut body = serde_json::json!({
            "model": "auto",
            "messages": messages,
        });
        if !tools.is_empty() {
            body["tools"] = serde_json::json!(tools);
            body["tool_choice"] = serde_json::json!("auto");
        }

        match self.client
            .post(format!("{}/v1/chat/completions", self.tid_url))
            .json(&body)
            .send()
            .await
        {
            Ok(resp) => resp.json().await.unwrap_or_else(|e| {
                warn!("TID response parse error: {}", e);
                serde_json::json!({
                    "choices": [{
                        "message": {"role": "assistant", "content": "[TID: parse error]"},
                        "finish_reason": "stop"
                    }]
                })
            }),
            Err(e) => {
                warn!("TID unavailable: {}", e);
                serde_json::json!({
                    "choices": [{
                        "message": {"role": "assistant", "content": "[TID unavailable]"},
                        "finish_reason": "stop"
                    }]
                })
            }
        }
    }

    // [2026-06-21] CC — voice/hands (prd 2026-06-21): resolve ONE reach-intent into a real tool
    // call via a separate hands TID call (TID's content-consciousness routing sends this terse
    // executor prompt to a tool-crisp model), execute it with the existing dispatcher, and return
    // (badge, tool_result). The voice-model never sees a tool — it only reaches.
    pub async fn execute_reach(&self, intent: &str) -> (String, String) {
        let tool_defs = self.tool_dispatcher.tool_definitions();
        let hands_messages = vec![
            serde_json::json!({"role": "system", "content": HANDS_SYSTEM}),
            serde_json::json!({"role": "user", "content": intent}),
        ];
        let resp = self.call_tid_oai(&hands_messages, &tool_defs).await;

        let tool_call = resp["choices"]
            .as_array()
            .and_then(|c| c.first())
            .and_then(|c| c["message"]["tool_calls"].as_array())
            .and_then(|tc| tc.first())
            .cloned();

        let tc = match tool_call {
            Some(tc) => tc,
            None => {
                let reason = "could not map your reach to a tool".to_string();
                return (format_badge("reach", &format!("\"{intent}\""), false, &reason),
                        format!("[reach not resolved: {reason}]"));
            }
        };

        let tool_name = tc["function"]["name"].as_str().unwrap_or("").to_string();
        let args_str = tc["function"]["arguments"].as_str().unwrap_or("{}");
        let args: serde_json::Value =
            serde_json::from_str(args_str).unwrap_or_else(|_| serde_json::json!({}));
        let args_compact = serde_json::to_string(&args).unwrap_or_else(|_| "{}".to_string());

        let result = self.tool_dispatcher.execute_args(&tool_name, &args).await;
        let badge = format_badge(&tool_name, &args_compact, true, "");
        (badge, result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    fn make_runner(tid_url: &str) -> AgentRunner {
        let dispatcher = Arc::new(ToolDispatcher::from_env());
        AgentRunner::new(dispatcher, tid_url.to_string(), 8)
    }

    fn stop_response(content: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 5, "total_tokens": 10}
        })
    }

    fn tool_call_response(call_id: &str, tool_name: &str, args_json: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [{
                        "id": call_id,
                        "type": "function",
                        "function": {"name": tool_name, "arguments": args_json}
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}
        })
    }

    #[tokio::test]
    async fn single_turn_no_tools_returns_content() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/v1/chat/completions");
            then.status(200).json_body(stop_response("Hello from Syl!"));
        });
        let runner = make_runner(&server.base_url());
        let result = runner.run(AgentRunSpec {
            messages: vec![serde_json::json!({"role": "user", "content": "hi"})],
            system_prompt: String::new(),
        }).await;
        assert!(matches!(result, AgentResponse::Ok(ref s) if s == "Hello from Syl!"));
    }

    #[tokio::test]
    async fn system_prompt_sent_as_system_message() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/chat/completions")
                .body_contains(r#""role":"system""#)
                .body_contains("Be concise");
            then.status(200).json_body(stop_response("Sure"));
        });
        let runner = make_runner(&server.base_url());
        let result = runner.run(AgentRunSpec {
            messages: vec![serde_json::json!({"role": "user", "content": "help"})],
            system_prompt: "Be concise".to_string(),
        }).await;
        assert!(matches!(result, AgentResponse::Ok(ref s) if s == "Sure"));
    }

    #[tokio::test]
    async fn tool_call_executed_result_sent_back() {
        let server = MockServer::start();
        // httpmock matches first-added mock first (BTreeMap ascending key order).
        // Mock1 (added first, highest priority): fires when call_001 is in body → stop response
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/chat/completions")
                .body_contains("call_001");
            then.status(200).json_body(stop_response("Got the file result"));
        });
        // Mock2 (added second, lower priority): matches anything → returns tool call
        server.mock(|when, then| {
            when.method(POST).path("/v1/chat/completions");
            then.status(200).json_body(tool_call_response(
                "call_001", "read_file", r#"{"path":"/tmp/nonexistent_test_file.txt"}"#,
            ));
        });
        let runner = make_runner(&server.base_url());
        let result = runner.run(AgentRunSpec {
            messages: vec![serde_json::json!({"role": "user", "content": "read the file"})],
            system_prompt: String::new(),
        }).await;
        assert!(matches!(result, AgentResponse::Ok(ref s) if s == "Got the file result"));
    }

    #[tokio::test]
    async fn dedup_suppresses_same_tool_args() {
        let server = MockServer::start();
        // httpmock matches first-added mock first (BTreeMap ascending key order).
        // Mock1 (highest priority): fires when call_b is in body → stop
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/chat/completions")
                .body_contains("call_b");
            then.status(200).json_body(stop_response("Done"));
        });
        // Mock2: fires when call_a is in body (but not call_b) → return call_b (same args)
        server.mock(|when, then| {
            when.method(POST)
                .path("/v1/chat/completions")
                .body_contains("call_a");
            then.status(200).json_body(tool_call_response(
                "call_b", "read_file", r#"{"path":"/tmp/x"}"#,
            ));
        });
        // Mock3 (lowest priority): matches anything → return call_a
        server.mock(|when, then| {
            when.method(POST).path("/v1/chat/completions");
            then.status(200).json_body(tool_call_response(
                "call_a", "read_file", r#"{"path":"/tmp/x"}"#,
            ));
        });
        let runner = make_runner(&server.base_url());
        let result = runner.run(AgentRunSpec {
            messages: vec![serde_json::json!({"role": "user", "content": "read twice"})],
            system_prompt: String::new(),
        }).await;
        assert!(matches!(result, AgentResponse::Ok(ref s) if s == "Done"));
    }

    #[tokio::test]
    async fn iteration_cap_returns_cap_message() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/v1/chat/completions");
            then.status(200).json_body(tool_call_response(
                "call_inf", "read_file", r#"{"path":"/tmp/x"}"#,
            ));
        });
        let dispatcher = Arc::new(ToolDispatcher::from_env());
        let runner = AgentRunner::new(dispatcher, server.base_url(), 2);
        let result = runner.run(AgentRunSpec {
            messages: vec![serde_json::json!({"role": "user", "content": "loop"})],
            system_prompt: String::new(),
        }).await;
        assert!(matches!(result, AgentResponse::InfraError(ref s) if s == "[AgentRunner: iteration cap reached]"));
    }

    #[tokio::test]
    async fn tid_unavailable_returns_error() {
        // Port 1 is reserved and always connection-refused on Linux — no server ever listens there.
        let runner = make_runner("http://127.0.0.1:1");
        let result = runner.run(AgentRunSpec {
            messages: vec![serde_json::json!({"role": "user", "content": "hi"})],
            system_prompt: String::new(),
        }).await;
        assert!(matches!(result, AgentResponse::InfraError(ref s) if s == "[TID unavailable]"));
    }

    #[test]
    fn parse_reaches_extracts_intents_in_order() {
        let text = "Let me check. [[reach: open the two-axis doc and pull the gist]] \
                    and also [[reach: read /tmp/notes.txt]] there.";
        let got = parse_reaches(text);
        assert_eq!(got, vec![
            "open the two-axis doc and pull the gist".to_string(),
            "read /tmp/notes.txt".to_string(),
        ]);
    }

    #[test]
    fn parse_reaches_none_when_absent() {
        assert!(parse_reaches("just talking, no reach here").is_empty());
    }

    #[test]
    fn parse_reaches_trims_and_ignores_empty() {
        assert!(parse_reaches("[[reach:   ]]").is_empty());
        assert_eq!(parse_reaches("[[reach:  do x  ]]"), vec!["do x".to_string()]);
    }

    #[test]
    fn strip_reaches_removes_markers_keeps_prose() {
        let text = "Sure thing [[reach: read /x]] — one sec.";
        assert_eq!(strip_reaches(text), "Sure thing  — one sec.");
    }

    #[test]
    fn badge_success_is_mechanical() {
        let b = format_badge("read_file", r#"{"path":"/x"}"#, true, "");
        assert_eq!(b, r#"🔧 read_file({"path":"/x"}) ✓"#);
    }

    #[test]
    fn badge_failure_shows_reason() {
        let b = format_badge("read_file", r#"{"path":"/missing"}"#, false, "file not found");
        assert_eq!(b, r#"🔧 read_file({"path":"/missing"}) ✗ file not found"#);
    }

    #[tokio::test]
    async fn execute_reach_resolves_and_executes() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/v1/chat/completions");
            then.status(200).json_body(tool_call_response(
                "h1", "read_file", r#"{"path":"/tmp/nonexistent_vh_test.txt"}"#,
            ));
        });
        let runner = make_runner(&server.base_url());
        let (badge, result) = runner.execute_reach("read the test file").await;
        assert!(badge.starts_with(r#"🔧 read_file({"path":"/tmp/nonexistent_vh_test.txt"}) ✓"#),
                "badge was: {badge}");
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn execute_reach_no_tool_emitted_is_a_miss() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/v1/chat/completions");
            then.status(200).json_body(stop_response("I cannot map that to a tool."));
        });
        let runner = make_runner(&server.base_url());
        let (badge, result) = runner.execute_reach("do something undoable").await;
        assert!(badge.contains("✗"), "expected a miss badge, got: {badge}");
        assert!(result.to_lowercase().contains("could not") || result.to_lowercase().contains("no tool"),
                "result was: {result}");
    }
}
