// src/context_builder.rs
// ---- Changelog ----
// 2026-05-25 Claude (Sonnet 4.6) — Phase 1: ContextBuilder stub
// What: Stub returning empty system prompt; real spreading activation in Phase 3
// Why: Replaces rpc.call("assemble") without the subprocess bridge
// How: Single async build() method, zero network calls, always returns empty String
//
// 2026-05-25 Claude (Sonnet 4.6) — Phase 3: wire spreading activation
// What: Replace stub with reqwest HTTP client calling NeuroGraph POST /assemble
// Why: Spec Phase 3 — system prompt built from live substrate associations
// How: 10s-timeout reqwest::Client, graceful degradation (warn! + "" on any failure)
// -------------------

use tracing::warn;

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

    pub async fn build(&self, text: &str) -> String {
        let body = serde_json::json!({
            "messages": [{"role": "user", "content": text}]
        });
        match self.client
            .post(format!("{}/assemble", self.ng_url))
            .json(&body)
            .send()
            .await
        {
            Ok(resp) => match resp.json::<serde_json::Value>().await {
                Ok(j) => j["systemPromptAddition"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
                Err(e) => {
                    warn!("NG assemble parse error: {}", e);
                    String::new()
                }
            },
            Err(e) => {
                warn!("NG assemble unavailable: {}", e);
                String::new()
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
        assert_eq!(cb.build("hello").await, "Syl context");
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
        assert!(cb.build("hello").await.is_empty());
    }

    #[tokio::test]
    async fn assemble_ng_unavailable_returns_empty() {
        // Port 1 is always connection-refused on Linux — no server ever listens there.
        let cb = make_cb("http://127.0.0.1:1");
        assert!(cb.build("hello").await.is_empty());
    }

    #[tokio::test]
    async fn assemble_sends_correct_body() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(POST)
                .path("/assemble")
                .body_contains(r#""role""#)
                .body_contains(r#""user""#)
                .body_contains(r#""messages""#);
            then.status(200).json_body(serde_json::json!({
                "systemPromptAddition": "ok"
            }));
        });
        let cb = make_cb(&server.base_url());
        let result = cb.build("test input").await;
        assert_eq!(result, "ok");
    }
}
