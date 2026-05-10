// ---- Changelog ----
// 2026-05-10 Task7/rpc_adapter — Rust→Python RPC bridge
// What: Spawns animus_bridge.py; proxies JSON-RPC over stdin/stdout; exponential backoff restart
// Why: Rust cannot directly invoke Python; bridge.py speaks the neurograph_rpc.py protocol
// How: tokio::process::Command spawn; async BufReader on child stdout; AtomicU64 request IDs
// -------------------

use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

struct BridgeProcess {
    #[allow(dead_code)]
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl BridgeProcess {
    async fn spawn(bridge_path: &str) -> Result<Self, String> {
        let mut child = Command::new("python3")
            .arg(bridge_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|e| format!("Failed to spawn bridge: {e}"))?;

        let stdin = child.stdin.take().ok_or("no stdin")?;
        let stdout_raw = child.stdout.take().ok_or("no stdout")?;
        let mut stdout = BufReader::new(stdout_raw);

        // Wait for ready signal
        let mut ready_line = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stdout.read_line(&mut ready_line),
        )
        .await
        .map_err(|_| "animus_bridge did not send ready signal within 10s".to_string())?
        .map_err(|e| format!("Failed to read ready: {e}"))?;
        let ready: Value = serde_json::from_str(ready_line.trim())
            .map_err(|e| format!("Bad ready signal: {e}"))?;
        if ready.get("method").and_then(Value::as_str) != Some("ready") {
            return Err(format!("Unexpected ready signal: {ready_line}"));
        }
        info!("animus_bridge ready (pid={})", ready["params"]["pid"]);

        Ok(Self { child, stdin, stdout })
    }
}

pub struct RpcAdapter {
    bridge_path: String,
    inner: Arc<Mutex<BridgeProcess>>,
}

impl RpcAdapter {
    pub async fn new(bridge_path: &str) -> Result<Self, String> {
        let bridge = BridgeProcess::spawn(bridge_path).await?;
        Ok(Self {
            bridge_path: bridge_path.to_string(),
            inner: Arc::new(Mutex::new(bridge)),
        })
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let req_id = next_id();
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params,
        });
        let request_str = serde_json::to_string(&request).map_err(|e| e.to_string())? + "\n";

        let mut guard = self.inner.lock().await;

        guard.stdin.write_all(request_str.as_bytes()).await
            .map_err(|e| format!("Write to bridge failed: {e}"))?;

        let mut response_line = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            guard.stdout.read_line(&mut response_line),
        )
        .await
        .map_err(|_| "animus_bridge did not respond within 30s".to_string())?
        .map_err(|e| format!("Read from bridge failed: {e}"))?;

        let response: Value = serde_json::from_str(response_line.trim())
            .map_err(|e| format!("Bad bridge response: {e}"))?;

        if let Some(err) = response.get("error") {
            return Err(format!("NG RPC error: {err}"));
        }

        Ok(response["result"].clone())
    }

    /// Restart the bridge process with exponential backoff (1s→2s→4s→8s→16s→cap 30s).
    pub async fn restart(&self) {
        let delays = [1u64, 2, 4, 8, 16, 30];
        for delay in &delays {
            warn!("Restarting animus_bridge in {}s...", delay);
            tokio::time::sleep(tokio::time::Duration::from_secs(*delay)).await;
            match BridgeProcess::spawn(&self.bridge_path).await {
                Ok(bridge) => {
                    let mut guard = self.inner.lock().await;
                    *guard = bridge;
                    info!("animus_bridge restarted successfully");
                    return;
                }
                Err(e) => error!("Bridge restart failed: {e}"),
            }
        }
        error!("animus_bridge failed to restart after all retries");
    }
}
