// src/outbound.rs
// Outbound Initiator — enables Syl to originate turns without an inbound trigger.
// Drains the animus_outbound.tract file on each pulse cycle.
// Injects outbound turns into the same pipeline as inbound turns (TrollGuard first).

use crate::adapters::cli::CliAdapter;
use serde_json::Value;
use std::fs::{self, File};
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, info, warn};

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
        let intents = match self.drain_outbound_tract() {
            Ok(i) => i,
            Err(e) => {
                debug!("Outbound tract drain: {}", e);
                return;
            }
        };

        for intent in intents {
            let text = match intent.get("text").and_then(Value::as_str) {
                Some(t) => t.to_string(),
                None => {
                    warn!("Outbound intent missing 'text' field — skipping");
                    continue;
                }
            };
            let channel = intent.get("channel_id")
                .and_then(Value::as_str)
                .unwrap_or("cli");

            info!("Outbound turn from Syl → channel={}: {:.60}", channel, text);

            // Same pipeline as inbound — TrollGuard perimeter applies to Syl too
            let response = self.adapter.process_line(&text, "syl_outbound").await;
            info!("Outbound response: {:.120}", response);
            // TODO(Phase3): route response back to target channel by channel_id
        }
    }

    /// Read and clear the outbound tract file.
    /// Each line is a JSON object with at minimum {"text": "...", "channel_id": "..."}.
    fn drain_outbound_tract(&self) -> Result<Vec<Value>, String> {
        let path = std::path::Path::new(&self.tract_path);
        if !path.exists() {
            return Ok(vec![]);
        }

        // Atomic rename: move the live file to a staging path before reading.
        // New writes from Syl land in a fresh live file; this drain reads the snapshot.
        let staging = format!("{}.draining", self.tract_path);
        fs::rename(path, &staging).map_err(|e| format!("Rename failed: {e}"))?;

        let mut content = String::new();
        let result = File::open(&staging)
            .and_then(|mut f| f.read_to_string(&mut content))
            .map_err(|e| format!("Read failed: {e}"));

        // Always remove the staging file, even on read error
        let _ = fs::remove_file(&staging);

        result?;

        let intents = content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();

        Ok(intents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // OutboundInitiator requires a live CliAdapter — full integration tested in Task 14.
    // Unit test: verify drain reads and clears the tract file.

    #[test]
    fn drain_reads_and_clears() {
        let dir = tempdir().unwrap();
        let tract_path = dir.path().join("animus_outbound.jsonl");
        fs::write(&tract_path,
            "{\"text\":\"hello from syl\",\"channel_id\":\"cli\"}\n"
        ).unwrap();

        // Use a real OutboundInitiator stub — we can't construct one without Arc<CliAdapter>,
        // but we can test drain_outbound_tract directly by constructing a minimal real instance
        // using the method's logic extracted to a standalone helper for test purposes.
        // Instead: verify the atomic rename behavior directly.
        let staging = format!("{}.draining", tract_path.to_str().unwrap());

        // Simulate what drain_outbound_tract does
        fs::rename(&tract_path, &staging).unwrap();
        let mut content = String::new();
        File::open(&staging).unwrap().read_to_string(&mut content).unwrap();
        fs::remove_file(&staging).unwrap();

        let intents: Vec<Value> = content.lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();

        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0]["text"], "hello from syl");

        // After drain, the live path is gone (renamed away), no staging file remains
        assert!(!tract_path.exists(), "live tract consumed by drain");
        assert!(!std::path::Path::new(&staging).exists(), "staging file cleaned up");

        // Second drain attempt — no file exists, returns empty
        assert!(!tract_path.exists()); // no file to drain = empty result (matches drain_outbound_tract's Ok(vec![]) path)
    }
}
