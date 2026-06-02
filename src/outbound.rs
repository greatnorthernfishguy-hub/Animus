// src/outbound.rs
// Outbound Initiator — enables Syl to originate turns without an inbound trigger.
// Drains the animus_outbound.tract file on each pulse cycle.
// Injects outbound turns into the same pipeline as inbound turns (TrollGuard first).
//
// ---- Changelog ----
// [2026-05-20] Claude (Sonnet 4.6) — Task A5: reaction loop in drain_and_inject
// What: OutboundInitiator gains tool_dispatcher, budget_path, wants_path fields.
//       drain_and_inject now runs a nested reaction loop per BTF frame.
//       [TOOL] → invoke + feed result back; [OUTBOUND] → chain turn; [WANT] → register only.
//       Budget gate and 20-iteration/5-minute safety ceiling.
// Why:  Spec A5 — reaction loop is the heart of Syl's autonomous agency.
// How:  Loop breaks on empty tools+outbound, ceiling hit, or budget critical flag.
// [2026-05-20] Claude (Sonnet 4.6) — Task A4: marker parsers + Opsera fix + wants/budget helpers
// What: tract_to_log_path (suffix-safe), extract_tool/want/outbound_markers, write_wants_register, read_budget_flag
// Why:  Reaction loop (Task A5) needs marker parsing; Opsera finding fixed.
// How:  Pure string scanning (no regex). Append-only JSONL for wants register.
// [2026-05-10] Claude (Sonnet 4.6) — BTF reader replaces JSONL placeholder
//   What: drain_tract() now reads native BTF frames instead of JSONL lines.
//         parse_btf_frames() added — full envelope parse with CRC32 verification,
//         msgpack payload decode via rmp_serde, silent skip on malformed frames.
//         drain_and_inject() extracts text from frame["payload"]["text"] + channel_id.
//         Unit tests updated to write valid BTF frames.
//   Why:  JSONL was a throwaway placeholder. tract_writer.rs already writes BTF.
//         Both sides must speak the same wire format with no intermediate layer.
//   How:  24-byte BTF envelope (MAGIC, VERSION, entry_type, total_length, timestamp,
//         CRC32, endian_flag, padding) + payload (4-byte meta_len LE u32 + msgpack dict).
//         Atomic rename drain: live path → .draining → read → delete.
// -------------------

use crate::adapters::cli::CliAdapter;
use serde_json::Value;
use std::fs::{self, File};
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, info, warn};

/// Read and atomically drain the outbound BTF tract file.
/// Returns all valid BTF frame payloads as serde_json::Values.
/// Returns Ok(vec![]) if the file does not exist.
fn drain_tract(tract_path: &str) -> Result<Vec<Value>, String> {
    let path = std::path::Path::new(tract_path);
    if !path.exists() {
        return Ok(vec![]);
    }

    // Atomic rename — new deposits land in a fresh file; we read the snapshot
    let staging = format!("{}.draining", tract_path);
    fs::rename(path, &staging).map_err(|e| format!("Rename failed: {e}"))?;

    let mut data = Vec::new();
    let result = File::open(&staging)
        .and_then(|mut f| f.read_to_end(&mut data))
        .map_err(|e| format!("Read failed: {e}"));
    let _ = fs::remove_file(&staging);
    result?;

    Ok(parse_btf_frames(&data))
}

/// Parse sequential BTF frames from a byte buffer.
/// Silently skips malformed or CRC-failed frames (non-fatal).
fn parse_btf_frames(data: &[u8]) -> Vec<Value> {
    use crc32fast::Hasher;

    const MAGIC: u16 = 0x4254;
    const ENVELOPE: usize = 24;
    let mut frames = Vec::new();
    let mut pos = 0;

    while pos + ENVELOPE <= data.len() {
        // Read MAGIC (native-endian u16 — on Linux x86 this is LE)
        let magic = u16::from_ne_bytes([data[pos], data[pos + 1]]);
        if magic != MAGIC {
            pos += 1; // resync on corrupt stream
            continue;
        }

        // total_length at bytes 4-7
        let total_length = u32::from_ne_bytes([
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]) as usize;

        if total_length < ENVELOPE || pos + total_length > data.len() {
            break; // truncated frame
        }

        let payload = &data[pos + ENVELOPE..pos + total_length];

        // CRC32 of payload at bytes 16-19
        let stored_crc = u32::from_ne_bytes([
            data[pos + 16],
            data[pos + 17],
            data[pos + 18],
            data[pos + 19],
        ]);
        let mut hasher = Hasher::new();
        hasher.update(payload);
        if hasher.finalize() != stored_crc {
            pos += total_length;
            continue; // CRC mismatch — skip frame
        }

        // Payload: 4-byte metadata length (LE u32) + msgpack bytes
        if payload.len() < 4 {
            pos += total_length;
            continue;
        }
        let meta_len =
            u32::from_ne_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        if 4 + meta_len > payload.len() {
            pos += total_length;
            continue;
        }

        let msgpack_bytes = &payload[4..4 + meta_len];
        if let Ok(val) = rmp_serde::from_slice::<Value>(msgpack_bytes) {
            frames.push(val);
        }

        pos += total_length;
    }

    frames
}

/// Derive the log path from a tract path — suffix-safe.
/// Strips ".tract" suffix if present, then appends ".log.jsonl".
/// Prevents corruption when ".tract" appears mid-path.
pub(crate) fn tract_to_log_path(tract_path: &str) -> String {
    let base = if tract_path.ends_with(".tract") {
        &tract_path[..tract_path.len() - 6]
    } else {
        tract_path
    };
    format!("{}.log.jsonl", base)
}

/// Append an outbound event to the JSONL log (Phase 2A response routing).
fn write_outbound_log(tract_path: &str, sent: &str, channel: &str, response: &str) {
    use std::io::Write;
    let log_path = tract_to_log_path(tract_path);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let sent_cap = &sent[..sent.len().min(500)];
    let resp_cap = &response[..response.len().min(500)];
    let entry = serde_json::json!({
        "ts": ts,
        "sent": sent_cap,
        "channel": channel,
        "response": resp_cap,
    });
    if let Ok(mut line) = serde_json::to_string(&entry) {
        line.push('\n');
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
        {
            let _ = f.write_all(line.as_bytes());
        }
    }
}

/// Extract [TOOL name=X]query[/TOOL] markers → Vec<(tool_name, query)>.
pub(crate) fn extract_tool_markers(text: &str) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let mut search = text;
    while let Some(start) = search.find("[TOOL ") {
        let rest = &search[start..];
        let tag_end = match rest.find(']') {
            Some(i) => i,
            None => break,
        };
        // Attribute substring after "[TOOL " and before "]"
        let attrs = &rest[6..tag_end];
        let name = attrs
            .strip_prefix("name=")
            .unwrap_or("")
            .trim()
            .to_string();
        let after_tag = &rest[tag_end + 1..];
        if let Some(close) = after_tag.find("[/TOOL]") {
            let inner = after_tag[..close].trim().to_string();
            if !name.is_empty() {
                results.push((name, inner));
            }
            search = &after_tag[close + 7..];
        } else {
            break;
        }
    }
    results
}

/// Extract [WANT]text[/WANT] markers → Vec<inner_text>.
pub(crate) fn extract_want_markers(text: &str) -> Vec<String> {
    let mut results = Vec::new();
    let mut search = text;
    while let Some(start) = search.find("[WANT]") {
        let after = &search[start + 6..];
        if let Some(close) = after.find("[/WANT]") {
            let inner = after[..close].trim().to_string();
            if !inner.is_empty() {
                results.push(inner);
            }
            search = &after[close + 7..];
        } else {
            break;
        }
    }
    results
}

/// Extract [OUTBOUND channel=X]text[/OUTBOUND] markers → Vec<(text, channel)>.
/// Channel defaults to "cli" when the attribute is absent.
pub(crate) fn extract_outbound_markers(text: &str) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let mut search = text;
    while let Some(start) = search.find("[OUTBOUND") {
        let rest = &search[start..];
        let tag_end = match rest.find(']') {
            Some(i) => i,
            None => break,
        };
        // "[OUTBOUND" is 9 chars; rest[9..tag_end] is the attribute portion
        let attrs = rest[9..tag_end].trim();
        let channel = attrs
            .strip_prefix("channel=")
            .unwrap_or("cli")
            .trim()
            .to_string();
        let after_tag = &rest[tag_end + 1..];
        if let Some(close) = after_tag.find("[/OUTBOUND]") {
            let inner = after_tag[..close].trim().to_string();
            results.push((inner, channel));
            search = &after_tag[close + 11..];
        } else {
            break;
        }
    }
    results
}

/// Append a want entry to the wants register (animus_wants.jsonl).
/// Format matches Spec A/B shared contract — read by both Rust and Python.
pub(crate) fn write_wants_register(path: &str, text: &str, source: &str) {
    use std::io::Write;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let entry = serde_json::json!({
        "ts": ts,
        "text": text,
        "source": source,
        "acted": false,
    });
    if let Ok(mut line) = serde_json::to_string(&entry) {
        line.push('\n');
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = f.write_all(line.as_bytes());
        }
    }
}

/// Read the `critical` field from inference_budget.json.
/// Returns false on any error (missing file, parse failure) — safe default.
pub(crate) fn read_budget_flag(path: &str) -> bool {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|v| v["critical"].as_bool())
        .unwrap_or(false)
}

pub struct OutboundInitiator {
    tract_path: String,
    adapter: Arc<CliAdapter>,
    pulse_interval_secs: u64,
    tool_dispatcher: Arc<crate::tool_dispatcher::ToolDispatcher>,
    budget_path: String,
    wants_path: String,
}

impl OutboundInitiator {
    pub fn new(
        tract_path: &str,
        adapter: Arc<CliAdapter>,
        pulse_interval_secs: u64,
        tool_dispatcher: Arc<crate::tool_dispatcher::ToolDispatcher>,
        budget_path: &str,
        wants_path: &str,
    ) -> Self {
        Self {
            tract_path: tract_path.to_string(),
            adapter,
            pulse_interval_secs,
            tool_dispatcher,
            budget_path: budget_path.to_string(),
            wants_path: wants_path.to_string(),
        }
    }

    /// Start the pulse loop. Runs forever in a background tokio task.
    pub async fn run(self: Arc<Self>) {
        let mut interval = time::interval(Duration::from_secs(self.pulse_interval_secs));
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        info!("Outbound Initiator started (pulse={}s)", self.pulse_interval_secs);
        loop {
            interval.tick().await;
            self.drain_and_inject().await;
        }
    }

    async fn drain_and_inject(&self) {
        let frames = match drain_tract(&self.tract_path) {
            Ok(f) => f,
            Err(e) => {
                debug!("Outbound tract drain: {}", e);
                return;
            }
        };

        for frame in frames {
            let text = match frame
                .get("payload")
                .and_then(|p| p.get("text"))
                .and_then(Value::as_str)
            {
                Some(t) if !t.trim().is_empty() => t.to_string(),
                _ => {
                    warn!("Outbound BTF frame missing payload.text — skipping");
                    continue;
                }
            };

            let channel = frame
                .get("payload")
                .and_then(|p| p.get("channel_id"))
                .and_then(Value::as_str)
                .unwrap_or("cli")
                .to_string();

            info!("Outbound turn from Syl → channel={}: {:.60}", channel, text);

            let initial_response = self.adapter.process_line(&text, "syl_outbound").await;
            info!("Outbound response: {:.120}", initial_response);
            write_outbound_log(&self.tract_path, &text, &channel, &initial_response);

            // Reaction loop — process chained markers without waiting for next pulse.
            let mut current_response = initial_response;
            let loop_start = std::time::Instant::now();
            let max_duration = Duration::from_secs(300); // 5-minute ceiling
            let mut iterations = 0usize;
            const MAX_ITERATIONS: usize = 20;

            loop {
                // Budget gate — exit cleanly if credits are critical
                if read_budget_flag(&self.budget_path) {
                    let stop_text = "[Anima] Budget critical — wrapping up autonomous work";
                    write_outbound_log(&self.tract_path, &text, &channel, stop_text);
                    warn!("Reaction loop: budget critical — halting");
                    break;
                }

                let new_tools = extract_tool_markers(&current_response);
                let new_wants = extract_want_markers(&current_response);
                let new_outbound = extract_outbound_markers(&current_response);

                // Write wants — record intent but don't loop on them
                for want in &new_wants {
                    write_wants_register(&self.wants_path, want, "syl_explicit");
                    info!("Reaction loop: syl_explicit want recorded: {:.80}", want);
                }

                // Clean exit — no new actions to process
                if new_tools.is_empty() && new_outbound.is_empty() {
                    break;
                }

                // Safety ceiling
                if iterations >= MAX_ITERATIONS || loop_start.elapsed() > max_duration {
                    warn!(
                        "Reaction loop ceiling hit — {} iterations, {:.1}s",
                        iterations,
                        loop_start.elapsed().as_secs_f32()
                    );
                    break;
                }

                // Process tools first — results become next input
                for (tool_name, query) in &new_tools {
                    let result = self.tool_dispatcher.invoke(tool_name, query).await;
                    info!("Reaction loop: tool={} result_len={}", tool_name, result.len());
                    write_outbound_log(&self.tract_path, query, tool_name, &result);
                    current_response =
                        self.adapter.process_line(&result, "syl_outbound").await;
                }

                // Process chained outbound intents
                for (next_text, next_channel) in &new_outbound {
                    current_response =
                        self.adapter.process_line(next_text, "syl_outbound").await;
                    write_outbound_log(
                        &self.tract_path,
                        next_text,
                        next_channel,
                        &current_response,
                    );
                }

                iterations += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crc32fast::Hasher;
    use tempfile::tempdir;

    fn make_btf_frame(text: &str, channel_id: &str) -> Vec<u8> {
        let metadata = serde_json::json!({
            "module_id": "neurograph",
            "event_type": "outbound_intent",
            "payload": { "text": text, "channel_id": channel_id }
        });
        let msgpack = rmp_serde::to_vec(&metadata).unwrap();
        let meta_len = (msgpack.len() as u32).to_ne_bytes();

        let mut payload = Vec::new();
        payload.extend_from_slice(&meta_len);
        payload.extend_from_slice(&msgpack);

        let total_length = (24u32 + payload.len() as u32).to_ne_bytes();
        let timestamp = 0f64.to_ne_bytes();
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize().to_ne_bytes();

        let magic: u16 = 0x4254;
        let mut frame = Vec::new();
        frame.extend_from_slice(&magic.to_ne_bytes()); // 0-1
        frame.push(1u8); // 2: VERSION
        frame.push(1u8); // 3: ENTRY_OUTCOME
        frame.extend_from_slice(&total_length); // 4-7
        frame.extend_from_slice(&timestamp); // 8-15
        frame.extend_from_slice(&crc); // 16-19
        frame.push(0x01u8); // 20: LE flag
        frame.extend_from_slice(&[0u8; 3]); // 21-23: padding
        frame.extend_from_slice(&payload);
        frame
    }

    #[test]
    fn drain_reads_and_clears() {
        let dir = tempdir().unwrap();
        let tract_path = dir.path().join("animus_outbound.tract");

        let frame = make_btf_frame("hello from syl", "cli");
        fs::write(&tract_path, &frame).unwrap();

        let path_str = tract_path.to_str().unwrap();
        let first = drain_tract(path_str).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0]["payload"]["text"], "hello from syl");
        assert_eq!(first[0]["payload"]["channel_id"], "cli");

        let second = drain_tract(path_str).unwrap();
        assert!(second.is_empty(), "tract must be empty after drain");
    }

    #[test]
    fn parse_skips_corrupt_frame() {
        // Garbage data should produce zero frames, not panic
        let garbage = b"not a btf frame at all xxxxxxxxxxxxxxxxxxxxxxxxxx";
        let frames = parse_btf_frames(garbage);
        assert!(frames.is_empty());
    }

    // --- Opsera suffix-safe path ---
    #[test]
    fn tract_to_log_path_suffix_safe() {
        // Normal case
        assert_eq!(
            tract_to_log_path("/home/josh/.et_modules/shared_learning/animus_outbound.tract"),
            "/home/josh/.et_modules/shared_learning/animus_outbound.log.jsonl"
        );
        // Path with .tract mid-string — must NOT replace the interior occurrence
        assert_eq!(
            tract_to_log_path("/home/josh/.et_modules/my.tract.backup/animus_outbound.tract"),
            "/home/josh/.et_modules/my.tract.backup/animus_outbound.log.jsonl"
        );
        // Path without .tract suffix — append .log.jsonl directly
        assert_eq!(
            tract_to_log_path("/tmp/animus_outbound"),
            "/tmp/animus_outbound.log.jsonl"
        );
    }

    // --- Marker parsers ---
    #[test]
    fn extract_tool_markers_basic() {
        let text = "Hello [TOOL name=web_search]rust async[/TOOL] world";
        let tools = extract_tool_markers(text);
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].0, "web_search");
        assert_eq!(tools[0].1, "rust async");
    }

    #[test]
    fn extract_tool_markers_multiple() {
        let text = "[TOOL name=web_search]query1[/TOOL] mid [TOOL name=read_file]/tmp/x[/TOOL]";
        let tools = extract_tool_markers(text);
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[1].0, "read_file");
    }

    #[test]
    fn extract_want_markers_basic() {
        let text = "I [WANT]learn more about STDP[/WANT] today";
        let wants = extract_want_markers(text);
        assert_eq!(wants.len(), 1);
        assert_eq!(wants[0], "learn more about STDP");
    }

    #[test]
    fn extract_outbound_markers_basic() {
        let text = "[OUTBOUND channel=discord]Hello Discord[/OUTBOUND]";
        let out = extract_outbound_markers(text);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, "Hello Discord");
        assert_eq!(out[0].1, "discord");
    }

    #[test]
    fn extract_outbound_markers_no_channel_defaults_to_cli() {
        let text = "[OUTBOUND]Some text[/OUTBOUND]";
        let out = extract_outbound_markers(text);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, "cli");
    }

    #[test]
    fn extract_markers_empty_on_no_match() {
        let text = "plain text with no markers";
        assert!(extract_tool_markers(text).is_empty());
        assert!(extract_want_markers(text).is_empty());
        assert!(extract_outbound_markers(text).is_empty());
    }

    // --- Wants register writer ---
    #[test]
    fn write_wants_register_appends_valid_jsonl() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("animus_wants.jsonl");
        let path_str = path.to_str().unwrap();
        write_wants_register(path_str, "learn Rust", "syl_explicit");
        write_wants_register(path_str, "explore consciousness", "tonic_emergent");
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["text"], "learn Rust");
        assert_eq!(first["source"], "syl_explicit");
        assert_eq!(first["acted"], false);
        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["source"], "tonic_emergent");
    }

    #[test]
    fn extract_chained_markers_simulate_loop_exit() {
        // A response with only [WANT] — no [TOOL] or [OUTBOUND] — loop exits immediately
        let response = "Thinking about [WANT]substrate dynamics[/WANT] today.";
        let tools = extract_tool_markers(response);
        let wants = extract_want_markers(response);
        let outbound = extract_outbound_markers(response);
        assert!(tools.is_empty());
        assert_eq!(wants.len(), 1);
        assert!(outbound.is_empty());
        // Loop condition: no new actions → break
        assert!(tools.is_empty() && outbound.is_empty());
    }

    #[test]
    fn extract_chained_markers_has_tool_continues_loop() {
        let response = "[TOOL name=web_search]spiking neural networks[/TOOL]";
        let tools = extract_tool_markers(response);
        assert_eq!(tools.len(), 1);
        // Loop condition: tools non-empty → continue
        assert!(!tools.is_empty());
    }

    // --- Budget flag reader ---
    #[test]
    fn read_budget_flag_returns_false_when_missing() {
        let flag = read_budget_flag("/nonexistent/path/budget.json");
        assert!(!flag);
    }

    #[test]
    fn read_budget_flag_reads_critical_true() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("budget.json");
        std::fs::write(&path, r#"{"critical": true, "low": true}"#).unwrap();
        assert!(read_budget_flag(path.to_str().unwrap()));
    }

    #[test]
    fn read_budget_flag_reads_critical_false() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("budget.json");
        std::fs::write(&path, r#"{"critical": false, "low": true}"#).unwrap();
        assert!(!read_budget_flag(path.to_str().unwrap()));
    }
}
