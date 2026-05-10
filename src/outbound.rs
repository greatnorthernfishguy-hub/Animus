// src/outbound.rs
// Outbound Initiator — enables Syl to originate turns without an inbound trigger.
// Drains the animus_outbound.tract file on each pulse cycle.
// Injects outbound turns into the same pipeline as inbound turns (TrollGuard first).
//
// ---- Changelog ----
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

pub struct OutboundInitiator {
    tract_path: String,
    adapter: Arc<CliAdapter>,
    pulse_interval_secs: u64,
}

impl OutboundInitiator {
    pub fn new(tract_path: &str, adapter: Arc<CliAdapter>, pulse_interval_secs: u64) -> Self {
        Self {
            tract_path: tract_path.to_string(),
            adapter,
            pulse_interval_secs,
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
            // Extract text from BTF metadata payload
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
                .unwrap_or("cli");

            info!("Outbound turn from Syl → channel={}: {:.60}", channel, text);

            // Same pipeline as inbound — TrollGuard perimeter applies to Syl too
            let response = self.adapter.process_line(&text, "syl_outbound").await;
            info!("Outbound response: {:.120}", response);
            // TODO(Phase3): route response to target channel by channel_id
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
}
