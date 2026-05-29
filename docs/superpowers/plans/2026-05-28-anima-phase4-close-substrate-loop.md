# Anima Phase 4 — Close the Substrate Loop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close Animus's substrate loop — add ConversationHistory, update ContextBuilder to pass full history to NG's KISS filter and return AssembleResult, deposit turn exchanges to River, and fire post-turn afterTurn calls.

**Architecture:** Three files change: `context_builder.rs` (new `AssembleResult` struct + updated `build()` signature), `pipeline.rs` (new `ConversationHistory` struct + updated `TurnPipeline` wiring), `main.rs` (add `ng_url` arg to `TurnPipeline::new()`). `neurograph_rpc.py` is unchanged — `/afterTurn` and `/assemble` already handle Phase 4 semantics including KISS truncation, surprise-weighted surfacing, and `_anticipate()`.

**Tech Stack:** Rust (tokio, reqwest, serde_json, httpmock)

**Spec:** `/home/josh/docs/superpowers/specs/2026-05-26-anima-phase4-design.md`

---

### Task 1: AssembleResult + build() signature + minimal pipeline bridge

The current `build(&str) -> String` cannot carry KISS-truncated messages back to the caller. This task adds `AssembleResult { system_prompt, messages }`, updates the signature to `build(&[Value]) -> AssembleResult`, updates all callers, and threads the result through `pipeline.rs` minimally so it compiles and all tests pass.

**Files:**
- Modify: `src/context_builder.rs`
- Modify: `src/pipeline.rs` (build call + AgentRunSpec only — no ConversationHistory yet)

- [ ] **Step 1: Write failing test for AssembleResult messages field**

Add this test to the `#[cfg(test)]` block in `src/context_builder.rs`:

```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

```
cd /home/josh/Animus && cargo test assemble_returns_kiss_messages 2>&1 | tail -15
```

Expected: compile error — `AssembleResult` not defined, `build` signature mismatch.

- [ ] **Step 3: Rewrite `src/context_builder.rs`**

Replace the entire file:

```rust
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
```

- [ ] **Step 4: Update `src/pipeline.rs` — minimal bridge**

In `pipeline.rs`, replace the BUILD and RUN blocks. Current code (around lines 99–105):

```rust
// BUILD — ContextBuilder stub; Phase 3 wires spreading activation assemble()
let system_prompt = self.context_builder.build(&clean_text).await;

// ROUTE + RUN — TID owns model selection + provider fallback (hook slot _50_tid_route)
let response = self.agent_runner.run(AgentRunSpec {
    messages: vec![serde_json::json!({"role": "user", "content": clean_text})],
    system_prompt,
}).await;
```

Replace with:

```rust
// BUILD — pass current turn to NG; returns KISS-truncated messages + system prompt
let assembled = self.context_builder.build(
    &[serde_json::json!({"role": "user", "content": clean_text})]
).await;

// ROUTE + RUN — KISS-truncated messages from assemble flow directly to TID
let response = self.agent_runner.run(AgentRunSpec {
    messages: assembled.messages,
    system_prompt: assembled.system_prompt,
}).await;
```

Add changelog entry at the top of the file (after the existing entries):

```rust
// [2026-05-28] Claude (Sonnet 4.6) — Phase 4 Task 1: minimal AssembleResult bridge
// What: Update BUILD to call build(&[Value]) and use assembled.messages + .system_prompt
// Why: context_builder::build() now returns AssembleResult; caller must consume both fields
// How: Wrap clean_text in single-element slice; use assembled fields in AgentRunSpec
```

- [ ] **Step 5: Run context_builder tests**

```
cd /home/josh/Animus && cargo test context_builder 2>&1 | tail -20
```

Expected:
```
test context_builder::tests::assemble_falls_back_to_input_when_no_messages_field ... ok
test context_builder::tests::assemble_ng_unavailable_returns_empty ... ok
test context_builder::tests::assemble_non_200_returns_empty ... ok
test context_builder::tests::assemble_null_returns_empty ... ok
test context_builder::tests::assemble_returns_kiss_messages ... ok
test context_builder::tests::assemble_returns_system_prompt ... ok
test context_builder::tests::assemble_sends_correct_body ... ok

test result: ok. 7 passed; 0 failed
```

- [ ] **Step 6: Run full test suite**

```
cd /home/josh/Animus && cargo test 2>&1 | tail -20
```

Expected: all tests pass. Pipeline tests still work because port-1 ContextBuilder returns a fallback `AssembleResult` with `messages = input.to_vec()` and `system_prompt = ""`.

- [ ] **Step 7: Commit**

```bash
cd /home/josh/Animus && git add src/context_builder.rs src/pipeline.rs && git commit -m "$(cat <<'EOF'
feat(phase4): AssembleResult + build() takes message slice

ContextBuilder.build() now accepts &[Value] and returns AssembleResult
{system_prompt, messages}. messages falls back to input when NG omits the
field (graceful degradation). pipeline.rs minimal bridge threads both fields
into AgentRunSpec.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: ConversationHistory + full TurnPipeline wiring + main.rs

Short-term working memory (40-message ring buffer), `turn_exchange` River deposit, and fire-and-forget `afterTurn` to trigger STDP + `_anticipate()`. This task rewrites the `TurnPipeline` internals and makes the single-line `main.rs` change.

**Files:**
- Modify: `src/pipeline.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Write failing ConversationHistory tests**

Add these four tests to the `#[cfg(test)]` block in `src/pipeline.rs` (after the existing three tests):

```rust
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

#[tokio::test]
async fn history_accumulates_across_turns() {
    let h = ConversationHistory::new(40);
    h.push_turn("a", "A");
    h.push_turn("b", "B");
    let snap = h.snapshot_with("c");
    // 2 push_turns = 4 stored messages + 1 pending user = 5
    assert_eq!(snap.len(), 5);
}
```

- [ ] **Step 2: Run failing tests**

```
cd /home/josh/Animus && cargo test history_ 2>&1 | tail -10
```

Expected: compile error — `ConversationHistory` not defined yet.

- [ ] **Step 3: Add `ConversationHistory` to `src/pipeline.rs`**

Add two imports to the existing `use` block at the top of `pipeline.rs`:

```rust
use std::collections::VecDeque;
use std::sync::Mutex;
```

Then add the struct and impl directly after the `use` block, before `pub enum SourceType`:

```rust
const HISTORY_CAPACITY: usize = 40;

struct ConversationHistory {
    inner: Mutex<VecDeque<serde_json::Value>>,
    capacity: usize,
}

impl ConversationHistory {
    fn new(capacity: usize) -> Self {
        Self { inner: Mutex::new(VecDeque::new()), capacity }
    }

    fn snapshot_with(&self, user_text: &str) -> Vec<serde_json::Value> {
        let guard = self.inner.lock().unwrap();
        let mut msgs: Vec<serde_json::Value> = guard.iter().cloned().collect();
        msgs.push(serde_json::json!({"role": "user", "content": user_text}));
        msgs
    }

    fn push_turn(&self, user_text: &str, assistant_text: &str) {
        let mut guard = self.inner.lock().unwrap();
        guard.push_back(serde_json::json!({"role": "user", "content": user_text}));
        guard.push_back(serde_json::json!({"role": "assistant", "content": assistant_text}));
        while guard.len() > self.capacity {
            guard.pop_front();
        }
    }
}
```

- [ ] **Step 4: Run ConversationHistory tests**

```
cd /home/josh/Animus && cargo test history_ 2>&1 | tail -15
```

Expected:
```
test pipeline::tests::history_accumulates_across_turns ... ok
test pipeline::tests::history_evicts_oldest_when_over_capacity ... ok
test pipeline::tests::history_push_turn_appears_in_next_snapshot ... ok
test pipeline::tests::history_snapshot_with_empty_history ... ok

test result: ok. 4 passed; 0 failed
```

- [ ] **Step 5: Update `TurnPipeline` struct and `new()` in `src/pipeline.rs`**

Replace the current `TurnPipeline` struct:

```rust
pub struct TurnPipeline {
    trollguard: Arc<TrollGuardBridge>,
    context_builder: Arc<ContextBuilder>,
    tract: Arc<TractWriter>,
    agent_runner: Arc<AgentRunner>,
}
```

With:

```rust
pub struct TurnPipeline {
    trollguard: Arc<TrollGuardBridge>,
    context_builder: Arc<ContextBuilder>,
    tract: Arc<TractWriter>,
    agent_runner: Arc<AgentRunner>,
    history: ConversationHistory,
    ng_client: reqwest::Client,
    ng_url: String,
}
```

Replace the current `new()`:

```rust
pub fn new(
    trollguard: Arc<TrollGuardBridge>,
    context_builder: Arc<ContextBuilder>,
    tract: Arc<TractWriter>,
    agent_runner: Arc<AgentRunner>,
) -> Self {
    Self { trollguard, context_builder, tract, agent_runner }
}
```

With:

```rust
pub fn new(
    trollguard: Arc<TrollGuardBridge>,
    context_builder: Arc<ContextBuilder>,
    tract: Arc<TractWriter>,
    agent_runner: Arc<AgentRunner>,
    ng_url: String,
) -> Self {
    let ng_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("failed to build pipeline HTTP client");
    Self {
        trollguard,
        context_builder,
        tract,
        agent_runner,
        history: ConversationHistory::new(HISTORY_CAPACITY),
        ng_client,
        ng_url,
    }
}
```

- [ ] **Step 6: Rewrite `run()` with full Phase 4 logic in `src/pipeline.rs`**

Replace the entire `run()` body (currently lines 67–114):

```rust
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

    // INGEST — raw experience pre-BUILD (Law 7: substrate receives input before it is acted upon)
    self.tract.deposit_event_silent("turn_ingest", serde_json::json!({
        "text": clean_text,
        "channel_id": ctx.channel_id,
        "user_id": ctx.user_id,
    }));

    // BUILD — full conversation history → NG assemble (spreading activation + KISS truncation)
    let messages = self.history.snapshot_with(&clean_text);
    let assembled = self.context_builder.build(&messages).await;

    // ROUTE + RUN — KISS-truncated messages to TID
    let response = self.agent_runner.run(AgentRunSpec {
        messages: assembled.messages,
        system_prompt: assembled.system_prompt,
    }).await;

    // POST-RUN — deposit exchange to River; update working memory
    self.tract.deposit_event_silent("turn_exchange", serde_json::json!({
        "user": clean_text,
        "assistant": response,
        "channel_id": ctx.channel_id,
    }));
    self.history.push_turn(&clean_text, &response);

    // AFTER-TURN — fire-and-forget NG graph.step() + STDP + _anticipate(); response unblocked
    let ng_url = self.ng_url.clone();
    let user_text = clean_text.clone();
    let ng_client = self.ng_client.clone();
    tokio::spawn(async move {
        let body = serde_json::json!({
            "lastUserMessage": {"role": "user", "content": user_text}
        });
        if let Err(e) = ng_client
            .post(format!("{}/afterTurn", ng_url))
            .json(&body)
            .send()
            .await
        {
            tracing::warn!("afterTurn fire failed: {}", e);
        }
    });

    // RESPOND
    self.tract.deposit_event_silent("turn_complete", serde_json::json!({
        "channel_id": ctx.channel_id,
        "response_len": response.len(),
    }));

    response
}
```

Add changelog entry at the top of `pipeline.rs`:

```rust
// [2026-05-28] Claude (Sonnet 4.6) — Phase 4 Task 2: ConversationHistory + afterTurn
// What: Add ConversationHistory (Mutex<VecDeque>, cap 40); TurnPipeline gains history,
//       ng_client, ng_url; run() builds history snapshot → NG assemble → KISS messages;
//       POST-RUN deposits turn_exchange + push_turn; afterTurn fire-and-forget.
// Why: Phase 4 spec — closes substrate loop: working memory + River write + STDP trigger.
// How: snapshot_with() builds full context slice pre-BUILD; push_turn() records exchange
//      post-RUN; tokio::spawn for afterTurn so response latency is unaffected.
```

- [ ] **Step 7: Update `make_pipeline()` in the test module**

Replace the existing `make_pipeline` helper in `src/pipeline.rs` tests:

```rust
fn make_pipeline(tg_url: &str, tid_url: &str) -> TurnPipeline {
    let tg = Arc::new(TrollGuardBridge::new(tg_url));
    // Port 1 = always connection-refused; ContextBuilder returns fallback AssembleResult gracefully
    let cb = Arc::new(ContextBuilder::new("http://127.0.0.1:1".to_string()));
    let tract = Arc::new(TractWriter::new("/tmp/test_animus_pipeline.tract"));
    let dispatcher = Arc::new(ToolDispatcher::from_env());
    let runner = Arc::new(AgentRunner::new(dispatcher, tid_url.to_string(), 8));
    // Port 1 for ng_url — afterTurn fires and fails silently (fire-and-forget by design)
    TurnPipeline::new(tg, cb, tract, runner, "http://127.0.0.1:1".to_string())
}
```

- [ ] **Step 8: Update `src/main.rs`**

Find the `TurnPipeline::new(...)` call (currently 4 args):

```rust
let pipeline = Arc::new(TurnPipeline::new(
    Arc::clone(&tg),
    Arc::clone(&context_builder),
    Arc::clone(&tract),
    Arc::clone(&agent_runner),
));
```

Replace with:

```rust
let pipeline = Arc::new(TurnPipeline::new(
    Arc::clone(&tg),
    Arc::clone(&context_builder),
    Arc::clone(&tract),
    Arc::clone(&agent_runner),
    cfg.ng_url.clone(),
));
```

Add changelog entry at the top of `main.rs`:

```rust
// [2026-05-28] Claude (Sonnet 4.6) — Phase 4: pass ng_url to TurnPipeline
// What: TurnPipeline::new() now takes ng_url as 5th arg for afterTurn fire-and-forget
// Why: Phase 4 wiring — pipeline needs NG URL to POST /afterTurn after each turn
// How: cfg.ng_url already populated from NEUROGRAPH_URL env (default 127.0.0.1:8850)
```

- [ ] **Step 9: Run full test suite**

```
cd /home/josh/Animus && cargo test 2>&1 | tail -25
```

Expected:
```
test context_builder::tests::assemble_falls_back_to_input_when_no_messages_field ... ok
test context_builder::tests::assemble_ng_unavailable_returns_empty ... ok
test context_builder::tests::assemble_non_200_returns_empty ... ok
test context_builder::tests::assemble_null_returns_empty ... ok
test context_builder::tests::assemble_returns_kiss_messages ... ok
test context_builder::tests::assemble_returns_system_prompt ... ok
test context_builder::tests::assemble_sends_correct_body ... ok
test pipeline::tests::filter_blocks_malicious_text ... ok
test pipeline::tests::filter_passes_safe_text_to_tid ... ok
test pipeline::tests::history_accumulates_across_turns ... ok
test pipeline::tests::history_evicts_oldest_when_over_capacity ... ok
test pipeline::tests::history_push_turn_appears_in_next_snapshot ... ok
test pipeline::tests::history_snapshot_with_empty_history ... ok
test pipeline::tests::tg_unavailable_proceeds_with_original_text ... ok

test result: ok. 14 passed; 0 failed
```

- [ ] **Step 10: Commit**

```bash
cd /home/josh/Animus && git add src/pipeline.rs src/main.rs && git commit -m "$(cat <<'EOF'
feat(phase4): ConversationHistory + turn_exchange + afterTurn

Closes the substrate loop. ConversationHistory (VecDeque cap 40) provides
short-term working memory across turns. run() builds full history snapshot
before NG assemble, deposits turn_exchange to River post-RUN, and fires
async POST /afterTurn (STDP + _anticipate()) without blocking response.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| ConversationHistory (VecDeque, cap 40) | Task 2 Step 3 |
| AssembleResult { system_prompt, messages } | Task 1 Step 3 |
| build() takes &[Value], returns AssembleResult | Task 1 Step 3 |
| KISS fallback to input slice on absent/empty field | Task 1 Step 3 |
| history.snapshot_with() before BUILD | Task 2 Step 6 |
| assembled.messages flows to AgentRunSpec | Task 1 Step 4, Task 2 Step 6 |
| turn_exchange deposit post-RUN | Task 2 Step 6 |
| history.push_turn() post-RUN | Task 2 Step 6 |
| afterTurn fire-and-forget via tokio::spawn | Task 2 Step 6 |
| main.rs passes cfg.ng_url to TurnPipeline | Task 2 Step 8 |
| SNN upgrades (DAS-GNN, IcaN, MMN, _anticipate, delays) | N/A — all inside NG, no Animus changes required |
| PUNCHLIST #262 /recall deferred to Phase 5 | N/A — already on punchlist |

All spec requirements covered. No placeholders. No TBDs.

**Type consistency:** `AssembleResult` defined in Task 1 Step 3, consumed in Task 1 Step 4 and Task 2 Step 6. `ConversationHistory` defined in Task 2 Step 3, used in Task 2 Steps 5-6. `TurnPipeline::new()` updated to 5-arg in Task 2 Step 5; `make_pipeline()` updated in Step 7; `main.rs` updated in Step 8. All consistent.

**Fallback behavior:** NG unavailable → `fallback` returned (system_prompt="", messages=input). Empty `messages` array from NG → fall back to input. Both cases keep the pipeline running without crashing.
