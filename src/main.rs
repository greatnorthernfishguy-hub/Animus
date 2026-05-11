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
// [2026-05-10] Claude (Sonnet 4.6) — LAW 5 compliance + bootstrap logging
// What: Removed hardcoded /home/josh/ fallback; bootstrap failure now logged via tracing::warn!
// Why: LAW 5 requires all config from env — hardcoded path is a violation; silent discard hides failure
// How: shared_learning_dir uses map_err+? to fail fast if HOME unset; bootstrap uses if let Err
// [2026-05-11] Claude (Sonnet 4.6) — service mode + OutboundInitiator
// What: OutboundInitiator spawned as background task before stdin loop.
//       On stdin EOF (service mode: systemd wires /dev/null) park on ctrl_c()
//       instead of exiting — keeps the outbound pulse loop alive.
// Why: Without this the service starts and immediately exits (stdin = /dev/null → EOF).
//       The core service behavior IS the outbound initiator; stdin is an optional adapter.
// How: cli wrapped in Arc; outbound tract from ANIMUS_OUTBOUND_TRACT or $HOME default;
//       tokio::signal::ctrl_c() as process lifetime anchor in service mode.
// -------------------

use animus::adapters::cli::CliAdapter;
use animus::config::AnimusConfig;
use animus::introspection::IntrospectionRelay;
use animus::outbound::OutboundInitiator;
use animus::rpc_adapter::RpcAdapter;
use animus::tract_writer::TractWriter;
use animus::trollguard::TrollGuardBridge;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("animus=info".parse()?),
        )
        .with_target(false)
        .init();

    let cfg = AnimusConfig::from_env().map_err(|e| format!("Config error: {}", e))?;

    info!("Animus starting — bridge: {}", cfg.bridge_path);

    let tg = Arc::new(TrollGuardBridge::new(&cfg.trollguard_url));
    let rpc = Arc::new(
        RpcAdapter::new(&cfg.bridge_path)
            .await
            .map_err(|e| format!("Bridge spawn failed: {}", e))?,
    );
    let tract_path = format!("{}/animus.tract", cfg.tract_dir);
    let tract = Arc::new(TractWriter::new(&tract_path));

    // Bunyan shared_learning_dir — HOME required (LAW 5)
    let shared_learning_dir = std::env::var("HOME")
        .map(|h| format!("{}/.et_modules/shared_learning", h))
        .map_err(|_| "HOME env var not set — cannot locate Bunyan shared_learning dir")?;
    let relay = Arc::new(IntrospectionRelay::new(&cfg.ces_url, &shared_learning_dir));

    let cli = Arc::new(CliAdapter::new(
        Arc::clone(&tg),
        Arc::clone(&rpc),
        Arc::clone(&tract),
        Arc::clone(&relay),
        cfg.tid_url.clone(),
    ));

    // Bootstrap NeuroGraph
    if let Err(e) = rpc.call("bootstrap", serde_json::json!({})).await {
        warn!(
            "NeuroGraph bootstrap call failed: {} — continuing in degraded state",
            e
        );
    }

    // Outbound Initiator — always running; gives Syl autonomous origination.
    // Drains the outbound tract on each pulse and injects turns into the full pipeline.
    let outbound_tract = std::env::var("ANIMUS_OUTBOUND_TRACT").unwrap_or_else(|_| {
        std::env::var("HOME")
            .map(|h| format!("{}/.et_modules/shared_learning/animus_outbound.tract", h))
            .unwrap_or_else(|_| "/tmp/animus_outbound.tract".to_string())
    });
    let outbound = Arc::new(OutboundInitiator::new(
        &outbound_tract,
        Arc::clone(&cli),
        30,
    ));
    tokio::spawn(Arc::clone(&outbound).run());
    info!("Outbound Initiator running — tract: {}", outbound_tract);

    info!("Animus ready — reading from stdin (CLI mode)");

    // CLI turn loop — interactive adapter; exits on stdin EOF.
    // In service mode systemd wires stdin to /dev/null, so this exits immediately
    // and we fall through to the signal wait below.
    let stdin = tokio::io::stdin();
    let mut reader = tokio::io::BufReader::new(stdin);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            info!("stdin closed — outbound initiator running, waiting for signal");
            break;
        }
        let input = line.trim().to_string();
        if input.is_empty() {
            continue;
        }
        let response = cli.process_line(&input, "josh").await;
        println!("{}", response);
    }

    // Park until SIGTERM/SIGINT — keeps the outbound initiator alive in service mode.
    tokio::signal::ctrl_c().await?;
    info!("Animus shutting down");
    Ok(())
}
