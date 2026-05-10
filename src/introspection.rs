// ---- Changelog ----
// 2026-05-10 Task9/introspection — IntrospectionRelay
// What: Polls CES :8847/stats and reads Bunyan JSONL for assembly-time self-context
// Why: Syl's window into her own state, injected alongside spreading-activation output (spec §2)
// How: reqwest GET for CES; fs::read_dir + JSONL parse for Bunyan; graceful empty on failure
// -------------------

use serde_json::Value;
use std::fs;
use std::path::Path;
use tracing::debug;

pub struct IntrospectionRelay {
    ces_url: String,
    shared_learning_dir: String,
    client: reqwest::Client,
}

impl IntrospectionRelay {
    pub fn new(ces_url: &str, shared_learning_dir: &str) -> Self {
        Self {
            ces_url: ces_url.to_string(),
            shared_learning_dir: shared_learning_dir.to_string(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .expect("failed to build http client"),
        }
    }

    /// Fetch CES stats snapshot. Returns Value::Null on failure.
    pub async fn fetch_ces_snapshot(&self) -> Value {
        let url = format!("{}/stats", self.ces_url);
        match self.client.get(&url).send().await {
            Ok(resp) => resp.json::<Value>().await.unwrap_or(Value::Null),
            Err(e) => {
                debug!("CES unavailable: {}", e);
                Value::Null
            }
        }
    }

    /// Read last N lines from bunyan*.jsonl files. Returns empty vec on failure.
    pub fn read_bunyan_events(&self, n: usize) -> Vec<Value> {
        let dir = Path::new(&self.shared_learning_dir);
        let mut events: Vec<Value> = Vec::new();

        let Ok(entries) = fs::read_dir(dir) else { return events; };

        let mut files: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name().to_string_lossy().starts_with("bunyan")
                    && e.file_name().to_string_lossy().ends_with(".jsonl")
            })
            .collect();

        files.sort_by_key(|e| e.file_name());

        for file in files.iter().rev() {
            let Ok(content) = fs::read_to_string(file.path()) else { continue; };
            let mut file_events: Vec<Value> = content
                .lines()
                .rev()
                .filter_map(|l| serde_json::from_str(l).ok())
                .collect();  // No .take(n) here
            events.append(&mut file_events);
            if events.len() >= n {
                break;
            }
        }

        events.truncate(n);
        events.reverse();  // oldest-to-newest
        events
    }

    /// Format CES snapshot + Bunyan events as a human-readable context block.
    pub async fn format_context(&self, n: usize) -> String {
        let ces = self.fetch_ces_snapshot().await;
        let bunyan = self.read_bunyan_events(n);

        let mut parts: Vec<String> = Vec::new();

        if !ces.is_null() {
            let nodes = ces.get("nodes").and_then(Value::as_u64).unwrap_or(0);
            let synapses = ces.get("synapses").and_then(Value::as_u64).unwrap_or(0);
            parts.push(format!("[Syl substrate] {} nodes, {} synapses", nodes, synapses));
        }

        if !bunyan.is_empty() {
            let narratives: Vec<String> = bunyan.iter()
                .filter_map(|e| e.get("summary").and_then(Value::as_str).map(String::from))
                .collect();
            if !narratives.is_empty() {
                parts.push(format!("[Recent activity] {}", narratives.join(" | ")));
            }
        }

        parts.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_when_no_bunyan_files() {
        let relay = IntrospectionRelay::new(
            "http://127.0.0.1:8847",
            "/tmp/animus_no_such_dir_12345",
        );
        let events = relay.read_bunyan_events(5);
        assert!(events.is_empty());
    }

    #[test]
    fn reads_bunyan_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl_path = dir.path().join("bunyan_test.jsonl");
        std::fs::write(&jsonl_path,
            "{\"summary\":\"Syl greeted Josh\"}\n{\"summary\":\"Concept formed: memory\"}\n"
        ).unwrap();

        let relay = IntrospectionRelay::new(
            "http://127.0.0.1:8847",
            dir.path().to_str().unwrap(),
        );
        let events = relay.read_bunyan_events(5);
        assert_eq!(events.len(), 2);
    }
}
