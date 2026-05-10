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

/// Atomically drain a JSONL tract file. Renames to a staging path before reading,
/// removes staging after. Returns Ok(vec![]) if the file does not exist.
fn drain_tract(tract_path: &str) -> Result<Vec<Value>, String> {
    let path = std::path::Path::new(tract_path);
    if !path.exists() {
        return Ok(vec![]);
    }

    let staging = format!("{}.draining", tract_path);
    fs::rename(path, &staging).map_err(|e| format!("Rename failed: {e}"))?;

    let mut content = String::new();
    let result = File::open(&staging)
        .and_then(|mut f| f.read_to_string(&mut content))
        .map_err(|e| format!("Read failed: {e}"));

    let _ = fs::remove_file(&staging);
    result?;

    let intents = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();

    Ok(intents)
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
        drain_tract(&self.tract_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn drain_reads_and_clears() {
        let dir = tempdir().unwrap();
        let tract_path = dir.path().join("animus_outbound.jsonl");
        fs::write(&tract_path,
            "{\"text\":\"hello from syl\",\"channel_id\":\"cli\"}\n"
        ).unwrap();

        let path_str = tract_path.to_str().unwrap();

        // Call the real drain_tract function — not a copy of its logic
        let first = drain_tract(path_str).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0]["text"], "hello from syl");

        // After drain, the live path is gone; second call returns empty
        let second = drain_tract(path_str).unwrap();
        assert!(second.is_empty(), "tract must return empty after drain");
    }
}
