// src/tool_dispatcher.rs
// ToolDispatcher — routes [TOOL name=X]query[/TOOL] markers to registered handlers.
// Initial tools: web_search (SearXNG), read_file (path-gated).
//
// ---- Changelog ----
// [2026-05-15] Claude (Sonnet 4.6) — Task 3: ToolDispatcher
// What: ToolHandler trait + ToolDispatcher registry. WebSearchTool hits SearXNG.
//       ReadFileTool reads from ANIMUS_ALLOWED_PATHS only. Shell disabled.
// Why:  Spec A — tool dispatch for the reaction loop.
// How:  async-trait for dyn-safe async; from_env() builds registry from env config.
// -------------------

use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

#[async_trait]
pub trait ToolHandler: Send + Sync {
    async fn invoke(&self, query: &str) -> String;
}

pub struct ToolDispatcher {
    tools: HashMap<String, Box<dyn ToolHandler>>,
}

impl ToolDispatcher {
    /// Build the dispatcher from environment config.
    /// `web_search` registered only when ANIMUS_SEARCH_URL is set.
    /// `read_file` always registered, paths from ANIMUS_ALLOWED_PATHS.
    pub fn from_env() -> Self {
        let mut tools: HashMap<String, Box<dyn ToolHandler>> = HashMap::new();
        let home = std::env::var("HOME").unwrap_or_default();

        if let Ok(url) = std::env::var("ANIMUS_SEARCH_URL") {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default();
            tools.insert("web_search".into(), Box::new(WebSearchTool { endpoint: url, client }));
        }

        let allowed_paths = std::env::var("ANIMUS_ALLOWED_PATHS")
            .unwrap_or_else(|_| format!("{}/.et_modules,{}/docs", home, home));
        tools.insert(
            "read_file".into(),
            Box::new(ReadFileTool { allowed_paths }),
        );

        Self { tools }
    }

    pub async fn invoke(&self, name: &str, query: &str) -> String {
        match self.tools.get(name) {
            Some(handler) => handler.invoke(query).await,
            None => {
                let available = {
                    let mut names: Vec<&str> =
                        self.tools.keys().map(String::as_str).collect();
                    names.sort();
                    names.join(", ")
                };
                format!(
                    "[Tool '{}' not registered — available: {}]",
                    name, available
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WebSearchTool — SearXNG-compatible endpoint
// ---------------------------------------------------------------------------

struct WebSearchTool {
    endpoint: String,
    client: reqwest::Client,
}

#[async_trait]
impl ToolHandler for WebSearchTool {
    async fn invoke(&self, query: &str) -> String {
        let result = self
            .client
            .get(&self.endpoint)
            .query(&[("q", query), ("format", "json")])
            .send()
            .await;

        match result {
            Ok(resp) => match resp.json::<serde_json::Value>().await {
                Ok(data) => {
                    let results = data["results"]
                        .as_array()
                        .map(|arr| {
                            arr.iter()
                                .take(3)
                                .map(|r| {
                                    let title =
                                        r["title"].as_str().unwrap_or("(no title)");
                                    let url = r["url"].as_str().unwrap_or("");
                                    format!("• {} — {}", title, url)
                                })
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "(no results)".to_string());
                    format!("[web_search results for {:?}]\n{}", query, results)
                }
                Err(e) => format!("[web_search parse error: {}]", e),
            },
            Err(e) => format!("[web_search error: {}]", e),
        }
    }
}

// ---------------------------------------------------------------------------
// ReadFileTool — path-gated file reader
// ---------------------------------------------------------------------------

struct ReadFileTool {
    allowed_paths: String,
}

#[async_trait]
impl ToolHandler for ReadFileTool {
    async fn invoke(&self, query: &str) -> String {
        let path = query.trim();
        let path_obj = match Path::new(path).canonicalize() {
            Ok(p) => p,
            Err(e) => {
                return format!(
                    "[read_file: cannot resolve path '{}': {}]",
                    path, e
                )
            }
        };

        let allowed: Vec<&str> = self.allowed_paths.split(',').map(str::trim).collect();
        let permitted = allowed.iter().any(|prefix| {
            Path::new(prefix)
                .canonicalize()
                .map(|p| path_obj.starts_with(&p))
                .unwrap_or(false)
        });

        if !permitted {
            return format!(
                "[read_file: path '{}' not in allowed list — allowed prefixes: {}]",
                path, self.allowed_paths
            );
        }

        match std::fs::read_to_string(&path_obj) {
            Ok(content) => {
                // Cap at 4000 chars — avoid overwhelming context
                let cap = content
                    .char_indices()
                    .nth(4000)
                    .map(|(i, _)| i)
                    .unwrap_or(content.len());
                content[..cap].to_string()
            }
            Err(e) => format!("[read_file error: {}]", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_dispatcher_with_read(allowed: &str) -> ToolDispatcher {
        let mut tools: HashMap<String, Box<dyn ToolHandler>> = HashMap::new();
        tools.insert(
            "read_file".into(),
            Box::new(ReadFileTool {
                allowed_paths: allowed.to_string(),
            }),
        );
        ToolDispatcher { tools }
    }

    #[tokio::test]
    async fn unregistered_tool_returns_helpful_message() {
        let d = make_dispatcher_with_read("/tmp");
        let result = d.invoke("web_search", "hello").await;
        assert!(result.contains("not registered"));
        assert!(result.contains("read_file"));
    }

    #[tokio::test]
    async fn read_file_blocked_outside_allowed_paths() {
        let d = make_dispatcher_with_read("/tmp/safe");
        let result = d.invoke("read_file", "/etc/passwd").await;
        assert!(result.contains("not in allowed list"));
    }

    #[tokio::test]
    async fn read_file_reads_allowed_path() {
        let dir = tempdir().unwrap();
        let allowed = dir.path().to_str().unwrap().to_string();
        let file = dir.path().join("test.txt");
        std::fs::write(&file, "hello content").unwrap();

        let d = make_dispatcher_with_read(&allowed);
        let result = d.invoke("read_file", file.to_str().unwrap()).await;
        assert_eq!(result, "hello content");
    }

    #[tokio::test]
    async fn read_file_caps_at_4000_chars() {
        let dir = tempdir().unwrap();
        let allowed = dir.path().to_str().unwrap().to_string();
        let file = dir.path().join("big.txt");
        std::fs::write(&file, "x".repeat(5000)).unwrap();

        let d = make_dispatcher_with_read(&allowed);
        let result = d.invoke("read_file", file.to_str().unwrap()).await;
        assert_eq!(result.len(), 4000);
    }

    #[tokio::test]
    async fn read_file_missing_file_error() {
        let dir = tempdir().unwrap();
        let allowed = dir.path().to_str().unwrap().to_string();
        let d = make_dispatcher_with_read(&allowed);
        let result = d
            .invoke("read_file", &format!("{}/nonexistent.txt", allowed))
            .await;
        // canonicalize() fails on nonexistent paths
        assert!(result.contains("cannot resolve path"));
    }
}
