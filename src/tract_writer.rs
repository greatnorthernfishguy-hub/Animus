// ---- Changelog ----
// 2026-05-10 Task8/tract_writer — BTF River writes
// What: Writes Binary Tract Format frames to .tract files for NeuroGraph River consumption
// Why: Anima must deposit its own module events (channel_connection, tg_outcome, etc.) to the River
// How: 24-byte BTF envelope (MAGIC + VERSION + type + length + timestamp + CRC32) + msgpack payload
// -------------------

use crc32fast::Hasher;
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

pub const MAGIC: u16 = 0x4254;
pub const VERSION: u8 = 1;
pub const ENTRY_OUTCOME: u8 = 1;
const ENVELOPE_SIZE: usize = 24;

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn make_envelope(entry_type: u8, total_length: u32, timestamp: f64, checksum: u32) -> [u8; 24] {
    let mut buf = [0u8; ENVELOPE_SIZE];
    buf[0..2].copy_from_slice(&MAGIC.to_ne_bytes());
    buf[2] = VERSION;
    buf[3] = entry_type;
    buf[4..8].copy_from_slice(&total_length.to_ne_bytes());
    buf[8..16].copy_from_slice(&timestamp.to_ne_bytes());
    buf[16..20].copy_from_slice(&checksum.to_ne_bytes());
    buf[20] = if cfg!(target_endian = "little") { 0x01 } else { 0x02 };
    buf[21..24].copy_from_slice(&[0u8; 3]);
    buf
}

pub struct TractWriter {
    path: String,
}

impl TractWriter {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string() }
    }

    /// Deposit a named event as a BTF outcome entry.
    /// event_type and payload are serialized as MessagePack.
    /// Uses zero-embedding (no vector) — Anima events are structural, not semantic.
    pub fn deposit_event(&self, event_type: &str, payload: Value) -> Result<(), String> {
        let metadata = serde_json::json!({
            "module_id": "anima",
            "event_type": event_type,
            "payload": payload,
        });
        let metadata_bytes = rmp_serde::to_vec(&metadata)
            .map_err(|e| format!("msgpack encode failed: {e}"))?;

        // Payload: 4-byte metadata length + metadata bytes (no embedding for structural events)
        let mut payload_bytes = Vec::new();
        payload_bytes.extend_from_slice(&(metadata_bytes.len() as u32).to_ne_bytes());
        payload_bytes.extend_from_slice(&metadata_bytes);

        let total_length = (ENVELOPE_SIZE + payload_bytes.len()) as u32;
        let timestamp = now_secs();

        let mut hasher = Hasher::new();
        hasher.update(&payload_bytes);
        let checksum = hasher.finalize();

        let envelope = make_envelope(ENTRY_OUTCOME, total_length, timestamp, checksum);

        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to open tract {}: {e}", self.path))?;

        f.write_all(&envelope).map_err(|e| format!("Write envelope failed: {e}"))?;
        f.write_all(&payload_bytes).map_err(|e| format!("Write payload failed: {e}"))?;

        Ok(())
    }

    /// Log tract write failure without crashing the turn pipeline.
    pub fn deposit_event_silent(&self, event_type: &str, payload: Value) {
        if let Err(e) = self.deposit_event(event_type, payload) {
            warn!("Tract write failed ({}): {}", event_type, e);
        }
    }
}
