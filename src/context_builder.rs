// src/context_builder.rs
// ---- Changelog ----
// [2026-05-28] Claude (Sonnet 4.6) — Phase 4 Task 1: AssembleResult + multi-message build()
// What: Add AssembleResult { system_prompt, messages }; build() takes &[Value] and returns
//       AssembleResult. messages field falls back to input slice when NG omits it.
// Why: Phase 4 spec — KISS-truncated message array must flow back to the RUN stage.
// How: build() serializes full message slice; extracts systemPromptAddition + messages from
//      NG response; graceful fallback (system_prompt="", messages=input) on any failure.
//
// [2026-05-26] Claude (Sonnet 4.6) — HTTP status check + test hardening
// What: Warn and return empty on non-200; add non-200 test; tighten body assertion
// Why: Code review issues 2, 3, 4 — silent wrong-body success on server errors
// How: resp.status().is_success() guard before JSON parse; new mock test; body_contains("test input")
//
// [2026-05-25] Claude (Sonnet 4.6) — Phase 3: wire spreading activation
// What: Replace stub with reqwest HTTP client calling NeuroGraph POST /assemble
// Why: Spec Phase 3 — system prompt built from live substrate associations
// How: 10s-timeout reqwest::Client, graceful degradation (warn! + "" on any failure)
//
// [2026-05-25] Claude (Sonnet 4.6) — Phase 1: ContextBuilder stub
// What: Stub returning empty system prompt; real spreading activation in Phase 3
// Why: Replaces rpc.call("assemble") without the subprocess bridge
// How: Single async build() method, zero network calls, always returns empty String
// -------------------

use tracing::warn;

pub struct AssembleResult {
    pub system_prompt: String,
    pub messages: Vec<serde_json::Value>,
}

pub struct ContextBuilder {
    client: reqwest::Client,
    ng_url: String,
}

impl ContextBuilder {
    pub fn new(ng_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("failed to build ContextBuilder HTTP client");
        Self { client, ng_url }
    }

    pub async fn build(&self, messages: &[serde_json::Value]) -> AssembleResult {
        let fallback = AssembleResult {
            system_prompt: String::new(),
            messages: messages.to_vec(),
        };
        let body = serde_json::json!({ "messages": messages });
        match self.client
            .post(format!("{}/assemble", self.ng_url))
            .json(&body)
            .send()
            .await
        {
            Ok(resp) => {
                if !resp.status().is_success() {
                    warn!("NG assemble HTTP {}", resp.status());
                    return fallback;
                }
                match resp.json::<serde_json::Value>().await {
                    Ok(j) => {
                        let system_prompt = j["systemPromptAddition"]
                            .as_str()
                            .unwrap_or("")
                            .to_string();
                        let msgs_out = match j.get("messages") {
                            Some(serde_json::Value::Array(arr)) if !arr.is_empty() => arr.clone(),
                            _ => messages.to_vec(),
                        };
                        AssembleResult { system_prompt, messages: msgs_out }
                    }
                    Err(e) => {
                        warn!("NG assemble parse error: {}", e);
                        fallback
                    }
                }
            }
            Err(e) => {
                warn!("NG assemble unavailable: {}", e);
                fallback
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    fn make_cb(ng_url: &str) -> ContextBuilder {
        ContextBuilder::new(ng_url.to_string())
    }

    fn single_user_msg(text: &str) -> Vec<serde_json::Value> {
        vec![serde_json::json!({"role": "user", "content": text})]
    }

    #[tokio::test]
    async fn assemble_returns_system_prompt() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/assemble");
            then.status(200).json_body(serde_json::json!({
                "systemPromptAddition": "Syl context"
            }));
        });
        let cb = make_cb(&server.base_url());
        let result = cb.build(&single_user_msg("hello")).await;
        assert_eq!(result.system_prompt, "Syl context");
    }

    #[tokio::test]
    async fn assemble_null_returns_empty() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/assemble");
            then.status(200).json_body(serde_json::json!({
                "systemPromptAddition": null
            }));
        });
        let cb = make_cb(&server.base_url());
        let result = cb.build(&single_user_msg("hello")).await;
        assert!(result.system_prompt.is_empty());
    }

    #[tokio::test]
    async fn assemble_ng_unavailable_returns_empty() {
        // Port 1 is always connection-refused on Linux — no server ever listens there.
        let cb = make_cb("http://127.0.0.1:1");
        let result = cb.build(&single_user_msg("hello")).await;
        assert!(result.system_prompt.is_empty());
        assert_eq!(result.messages.len(), 1);
    }

    #[tokio::test]
    async fn assemble_sends_correct_body() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST)
                .path("/assemble")
                .body_contains(r#""role""#)
                .body_contains(r#""user""#)
                .body_contains(r#""messages""#)
                .body_contains("test input");
            then.status(200).json_body(serde_json::json!({
                "systemPromptAddition": "ok"
            }));
        });
        let cb = make_cb(&server.base_url());
        let result = cb.build(&single_user_msg("test input")).await;
        assert_eq!(result.system_prompt, "ok");
    }

    #[tokio::test]
    async fn assemble_non_200_returns_empty() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/assemble");
            then.status(500).json_body(serde_json::json!({"error": "internal"}));
        });
        let cb = make_cb(&server.base_url());
        let result = cb.build(&single_user_msg("hello")).await;
        assert!(result.system_prompt.is_empty());
    }

    #[tokio::test]
    async fn assemble_returns_kiss_messages() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/assemble");
            then.status(200).json_body(serde_json::json!({
                "systemPromptAddition": "Syl context",
                "messages": [
                    {"role": "user", "content": "trimmed by KISS"}
                ]
            }));
        });
        let cb = make_cb(&server.base_url());
        let input = vec![serde_json::json!({"role": "user", "content": "hello"})];
        let result = cb.build(&input).await;
        assert_eq!(result.system_prompt, "Syl context");
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.messages[0]["content"], "trimmed by KISS");
    }

    #[tokio::test]
    async fn assemble_falls_back_to_input_when_no_messages_field() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST).path("/assemble");
            then.status(200).json_body(serde_json::json!({
                "systemPromptAddition": "Syl context"
            }));
        });
        let cb = make_cb(&server.base_url());
        let input = vec![serde_json::json!({"role": "user", "content": "original"})];
        let result = cb.build(&input).await;
        assert_eq!(result.system_prompt, "Syl context");
        assert_eq!(result.messages.len(), 1);
        assert_eq!(result.messages[0]["content"], "original");
    }
}
