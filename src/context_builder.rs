// src/context_builder.rs
// ---- Changelog ----
// 2026-05-25 Claude (Sonnet 4.6) — Phase 1: ContextBuilder stub
// What: Stub returning empty system prompt; real spreading activation in Phase 3
// Why: Replaces rpc.call("assemble") without the subprocess bridge
// How: Single async build() method, zero network calls, always returns empty String
// -------------------

pub struct ContextBuilder;

impl ContextBuilder {
    pub fn new() -> Self {
        ContextBuilder
    }

    pub async fn build(&self, _text: &str) -> String {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_returns_empty_string() {
        let cb = ContextBuilder::new();
        assert!(cb.build("any input").await.is_empty());
    }
}
