// ---- Changelog ----
// 2026-05-10 Task8/tract_writer — BTF River writes
// What: Writes Binary Tract Format frames to .tract files for NeuroGraph River consumption
// Why: Anima must deposit its own module events (channel_connection, tg_outcome, etc.) to the River
// How: 24-byte BTF envelope (MAGIC + VERSION + type + length + timestamp + CRC32) + msgpack payload
// -------------------

use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub struct TractWriter {
    path: String,
}

impl TractWriter {
    pub fn new(path: &str) -> Self {
        Self { path: path.to_string() }
    }

    /// Deposit a named event as a BTF OUTCOME entry via the canonical ng_tract
    /// framing. [2026-06-08] The old hand-rolled make_envelope wrote magic in
    /// NATIVE-endian (`MAGIC.to_ne_bytes()` = 54 42 'TB' on x86) instead of the
    /// crate's 42 54 'BT', so NG's TractReader (dispatch: first byte must == 0x42)
    /// bailed at the stream head and never reached the conversation EXPERIENCE
    /// frames. Routing through write_outcome guarantees byte-identical framing.
    pub fn deposit_event(&self, event_type: &str, payload: Value) -> Result<(), String> {
        use ng_tract::write::{deposit_to_file, write_outcome};
        use ng_tract::OutcomeEntry;

        let metadata = serde_json::json!({
            "module_id": "anima",
            "event_type": event_type,
            "payload": payload,
        });
        let metadata_bytes = rmp_serde::to_vec(&metadata)
            .map_err(|e| format!("msgpack encode failed: {e}"))?;

        let entry = OutcomeEntry {
            timestamp: now_secs(),
            module_id: "anima".to_string(),
            target_id: event_type.to_string(),
            success: true,
            embedding_dim: 0,
            embedding: Vec::new(),
            metadata: metadata_bytes,
        };
        let bytes = write_outcome(&entry);
        deposit_to_file(&self.path, &bytes).map_err(|e| format!("deposit_event failed: {e}"))
    }

    /// Log tract write failure without crashing the turn pipeline.
    pub fn deposit_event_silent(&self, event_type: &str, payload: Value) {
        if let Err(e) = self.deposit_event(event_type, payload) {
            warn!("Tract write failed ({}): {}", event_type, e);
        }
    }

    /// Deposit raw bytes as a BTF ExperienceEntry to the tract.
    /// source: originating module (e.g. "anima"), content is raw bytes (e.g. UTF-8 text).
    /// content_type is always "text" for conversation turns.
    pub fn deposit_experience(&self, source: &str, content: &[u8]) -> Result<(), String> {
        use ng_tract::ExperienceEntry;
        use ng_tract::write::{write_experience, deposit_to_file};
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let entry = ExperienceEntry {
            timestamp,
            source: source.to_string(),
            content_type: "text".to_string(),
            content: content.to_vec(),
        };

        let bytes = write_experience(&entry);
        deposit_to_file(&self.path, &bytes)
            .map_err(|e| format!("deposit_experience failed: {e}"))
    }

    /// deposit_experience without crashing the turn pipeline on failure.
    pub fn deposit_experience_silent(&self, source: &str, content: &[u8]) {
        if let Err(e) = self.deposit_experience(source, content) {
            warn!("Tract experience write failed: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ng_tract::read::{TractReader, ReadResult};
    use ng_tract::TractEntry;
    use tempfile::NamedTempFile;

    #[test]
    fn deposit_event_writes_canonical_outcome() {
        let tmp = NamedTempFile::new().expect("tempfile");
        let path = tmp.path().to_str().expect("utf8 path").to_string();
        let writer = TractWriter::new(&path);
        writer
            .deposit_event("turn_complete", serde_json::json!({"channel_id": "gui"}))
            .expect("deposit_event returned Err");
        let data = std::fs::read(&path).expect("read tract file");
        assert_eq!(&data[0..2], &[0x42, 0x54], "outcome frame must carry canonical BT magic, not 54 42");
        let mut reader = TractReader::new(&data);
        let result = reader.next_entry().expect("no entry").expect("entry parse error");
        match result {
            ReadResult::Entry(TractEntry::Outcome(e)) => {
                assert_eq!(e.module_id, "anima");
                assert_eq!(e.target_id, "turn_complete");
            }
            other => panic!("expected Outcome entry, got {:?}", other),
        }
        assert!(reader.next_entry().is_none(), "expected exactly one entry");
    }

    #[test]
    fn deposit_experience_writes_readable() {
        let tmp = NamedTempFile::new().expect("tempfile");
        let path = tmp.path().to_str().expect("utf8 path").to_string();

        let writer = TractWriter::new(&path);
        let text = b"hello from anima";
        writer.deposit_experience("anima", text).expect("deposit_experience returned Err");

        let data = std::fs::read(&path).expect("read tract file");
        let mut reader = TractReader::new(&data);
        let result = reader.next_entry()
            .expect("no entry in file")
            .expect("entry parse error");

        match result {
            ReadResult::Entry(TractEntry::Experience(e)) => {
                assert_eq!(e.source, "anima");
                assert_eq!(e.content_type, "text");
                assert_eq!(e.content, text);
            }
            other => panic!("expected Experience entry, got {:?}", other),
        }

        assert!(reader.next_entry().is_none(), "expected exactly one entry");
    }
}
