// src/agent_runner.rs
// AgentRunner — multi-turn tool loop for Anima's RUN phase.
//
// ---- Changelog ----
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

/// Pure data object for one agent run (Nanobot pattern — makes AgentRunner testable/reusable).
pub struct AgentRunSpec {
    /// OpenAI-format message history for this turn
    pub messages: Vec<serde_json::Value>,
    /// System prompt injected as {role:"system"} prepended to messages. Empty string = omit.
    pub system_prompt: String,
}

pub struct AgentRunner {
    tool_dispatcher: Arc<ToolDispatcher>,
    client: reqwest::Client,
    pub tid_url: String,
    pub max_iter: usize,
}

impl AgentRunner {
    pub fn new(
        tool_dispatcher: Arc<ToolDispatcher>,
        tid_url: String,
        max_iter: usize,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .expect("failed to build AgentRunner HTTP client");
        Self { tool_dispatcher, client, tid_url, max_iter }
    }

    pub async fn run(&self, spec: AgentRunSpec) -> String {
        let mut messages: Vec<serde_json::Value> = Vec::new();
        if !spec.system_prompt.is_empty() {
            messages.push(serde_json::json!({
                "role": "system",
                "content": spec.system_prompt
            }));
        }
        messages.extend(spec.messages);

        let tool_defs = self.tool_dispatcher.tool_definitions();
        let mut seen: HashSet<String> = HashSet::new();

        for _iter in 0..self.max_iter {
            let response = self.call_tid_oai(&messages, &tool_defs).await;

            let choice = match response["choices"].as_array().and_then(|c| c.first()) {
                Some(c) => c.clone(),
                None => return "[TID: malformed response]".to_string(),
            };

            let finish_reason = choice["finish_reason"].as_str().unwrap_or("stop");
            let message = choice["message"].clone();
            messages.push(message.clone());

            if finish_reason != "tool_calls" {
                return message["content"].as_str().unwrap_or("").to_string();
            }

            let tool_calls = match message["tool_calls"].as_array() {
                Some(tc) if !tc.is_empty() => tc.clone(),
                _ => return message["content"].as_str().unwrap_or("").to_string(),
            };

            for tc in &tool_calls {
                let tc_id = tc["id"].as_str().unwrap_or("").to_string();
                let func = &tc["function"];
                let tool_name = func["name"].as_str().unwrap_or("");
                let args_str = func["arguments"].as_str().unwrap_or("{}");

                // Tolerant JSON parse — LLMs produce malformed arguments JSON
                let args: serde_json::Value = serde_json::from_str(args_str)
                    .unwrap_or_else(|_| serde_json::json!({}));

                // Dedup guard: suppress identical (name, canonical_args) within one turn
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

        "[AgentRunner: iteration cap reached]".to_string()
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
        assert_eq!(result, "Hello from Syl!");
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
        assert_eq!(result, "Sure");
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
        assert_eq!(result, "Got the file result");
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
        assert_eq!(result, "Done");
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
        assert_eq!(result, "[AgentRunner: iteration cap reached]");
    }

    #[tokio::test]
    async fn tid_unavailable_returns_error() {
        // Port 1 is reserved and always connection-refused on Linux — no server ever listens there.
        let runner = make_runner("http://127.0.0.1:1");
        let result = runner.run(AgentRunSpec {
            messages: vec![serde_json::json!({"role": "user", "content": "hi"})],
            system_prompt: String::new(),
        }).await;
        assert_eq!(result, "[TID unavailable]");
    }
}
