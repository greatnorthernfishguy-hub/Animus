// src/main.rs
// Animus entry point — starts the agentic gateway.
// ---- Changelog ----
// [2026-05-10] Claude (Sonnet 4.6) — Task 1: Initial scaffold
// What: Placeholder main.rs for initial cargo build verification
// Why: Project scaffold requires a compiling binary target
// How: Minimal main() function — real entry point wired in subsequent tasks
// [2026-05-10] Claude (Sonnet 4.6) — Task 2: Envelope module
// What: Added TurnEnvelope + ChannelContext types in lib.rs
// Why: Core types needed for channel adapter ↔ RPC pipeline handoff
// How: Removed duplicate pub mod envelope; lib.rs is canonical
// [2026-05-10] Claude (Sonnet 4.6) — Task 11: CLI pipeline wiring
// What: Wires AnimusConfig → TrollGuard → RpcAdapter → TractWriter →
//       IntrospectionRelay → CliAdapter into a stdin/stdout turn loop
// Why: Provides a working end-to-end CLI pipeline for testing before WebSocket server
// How: All components constructed from env config, passed as Arc to CliAdapter
// -------------------

use animus::adapters::cli::CliAdapter;
use animus::config::AnimusConfig;
use animus::introspection::IntrospectionRelay;
use animus::rpc_adapter::RpcAdapter;
use animus::tract_writer::TractWriter;
use animus::trollguard::TrollGuardBridge;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("animus=info".parse()?))
        .with_target(false)
        .init();

    let cfg = AnimusConfig::from_env()
        .map_err(|e| format!("Config error: {}", e))?;

    info!("Animus starting — bridge: {}", cfg.bridge_path);

    let tg = Arc::new(TrollGuardBridge::new(&cfg.trollguard_url));
    let rpc = Arc::new(RpcAdapter::new(&cfg.bridge_path).await
        .map_err(|e| format!("Bridge spawn failed: {}", e))?);
    let tract_path = format!("{}/animus.tract", cfg.tract_dir);
    let tract = Arc::new(TractWriter::new(&tract_path));

    // Bunyan shared_learning_dir: expand $HOME at runtime
    let shared_learning_dir = std::env::var("HOME")
        .map(|h| format!("{}/.et_modules/shared_learning", h))
        .unwrap_or_else(|_| "/home/josh/.et_modules/shared_learning".to_string());
    let relay = Arc::new(IntrospectionRelay::new(&cfg.ces_url, &shared_learning_dir));

    let cli = CliAdapter::new(
        Arc::clone(&tg), Arc::clone(&rpc), Arc::clone(&tract), Arc::clone(&relay));

    info!("Animus ready — reading from stdin (CLI mode)");

    // Bootstrap NeuroGraph
    let _ = rpc.call("bootstrap", serde_json::json!({})).await;

    // CLI turn loop
    let stdin = tokio::io::stdin();
    let mut reader = tokio::io::BufReader::new(stdin);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            info!("stdin closed — shutting down");
            break;
        }
        let input = line.trim().to_string();
        if input.is_empty() { continue; }

        let response = cli.process_line(&input, "josh").await;
        println!("{}", response);
    }

    Ok(())
}
