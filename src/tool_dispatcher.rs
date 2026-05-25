// src/tool_dispatcher.rs
// ToolDispatcher — routes [TOOL name=X]query[/TOOL] markers to registered handlers.
// Initial tools: web_search (SearXNG), read_file (path-gated).
//
// ---- Changelog ----
// [2026-05-25] Claude (Sonnet 4.6) — Phase 2: capability introspection + JSON-args dispatch
// What: Add name/description/parameters_schema to ToolHandler trait; tool_definitions() +
//       execute_args() on ToolDispatcher
// Why: AgentRunner needs OpenAI tool schemas for TID requests; needs JSON-args dispatch
//      for structured tool_calls responses. Trait methods pre-implement Phase 4 TurnTool.
// How: Each handler returns its own schema; execute_args extracts primary arg by field name
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
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters_schema(&self) -> serde_json::Value;
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

    /// Returns OpenAI-format tool schema array for all registered tools.
    /// Passed to TID in every AgentRunner request so the LLM knows available tools.
    pub fn tool_definitions(&self) -> Vec<serde_json::Value> {
        let mut defs: Vec<serde_json::Value> = self.tools.values().map(|h| {
            serde_json::json!({
                "type": "function",
                "function": {
                    "name": h.name(),
                    "description": h.description(),
                    "parameters": h.parameters_schema()
                }
            })
        }).collect();
        defs.sort_by_key(|d| d["function"]["name"].as_str().unwrap_or("").to_string());
        defs
    }

    /// Dispatch a tool call with structured JSON arguments from an LLM tool_calls response.
    /// Extracts the primary string argument by trying "query", then "path", then the first
    /// string value in the object. Falls back to the raw JSON string.
    pub async fn execute_args(&self, name: &str, args: &serde_json::Value) -> String {
        let query = if let Some(q) = args.get("query").and_then(|v| v.as_str()) {
            q.to_string()
        } else if let Some(p) = args.get("path").and_then(|v| v.as_str()) {
            p.to_string()
        } else if let Some(obj) = args.as_object() {
            obj.values()
                .find_map(|v| v.as_str())
                .map(String::from)
                .unwrap_or_else(|| args.to_string())
        } else {
            args.to_string()
        };
        self.invoke(name, &query).await
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
    fn name(&self) -> &str { "web_search" }

    fn description(&self) -> &str {
        "Search the web for current information via SearXNG."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query"}
            },
            "required": ["query"]
        })
    }

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
    fn name(&self) -> &str { "read_file" }

    fn description(&self) -> &str {
        "Read a file from the allowed filesystem paths (up to 4000 characters)."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Absolute file path (must be within allowed paths)"
                }
            },
            "required": ["path"]
        })
    }

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

    #[test]
    fn tool_definitions_contains_read_file() {
        let d = ToolDispatcher::from_env();
        let defs = d.tool_definitions();
        let names: Vec<&str> = defs
            .iter()
            .filter_map(|d| d["function"]["name"].as_str())
            .collect();
        assert!(names.contains(&"read_file"));
    }

    #[test]
    fn tool_definition_has_required_openai_shape() {
        let d = ToolDispatcher::from_env();
        let defs = d.tool_definitions();
        let rf = defs.iter().find(|d| d["function"]["name"] == "read_file").unwrap();
        assert_eq!(rf["type"], "function");
        assert!(rf["function"]["description"].as_str().is_some());
        let params = &rf["function"]["parameters"];
        assert_eq!(params["type"], "object");
        assert!(params["properties"]["path"].is_object());
    }

    #[tokio::test]
    async fn execute_args_read_file_extracts_path() {
        let dir = tempdir().unwrap();
        let allowed = dir.path().to_str().unwrap().to_string();
        let file = dir.path().join("hello.txt");
        std::fs::write(&file, "world").unwrap();
        let d = make_dispatcher_with_read(&allowed);
        let args = serde_json::json!({"path": file.to_str().unwrap()});
        let result = d.execute_args("read_file", &args).await;
        assert_eq!(result, "world");
    }

    #[tokio::test]
    async fn execute_args_unknown_tool_returns_message() {
        let d = make_dispatcher_with_read("/tmp");
        let result = d.execute_args("nonexistent", &serde_json::json!({"query": "x"})).await;
        assert!(result.contains("not registered"));
    }
}
