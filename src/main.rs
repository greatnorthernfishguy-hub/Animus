// src/main.rs
// Animus entry point — starts the agentic gateway.
// ---- Changelog ----
// [2026-05-31] Claude (Sonnet 4.6) — Anima GUI Task 4: spawn HttpAdapter
// What: HttpAdapter spawned alongside CLI adapter; serves GET /status /history /channels + POST /turn
// Why: GUI needs observable HTTP interface; CLI-only had no external status surface
// How: Arc<TurnPipeline> shared via State<>; port from cfg.gui_port (ANIMUS_GUI_PORT, default 8848)
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
// [2026-05-15] Claude (Sonnet 4.6) — Task A6: wire BudgetMonitor + ToolDispatcher
// What: Construct ToolDispatcher::from_env(), conditionally spawn BudgetMonitor,
//       update OutboundInitiator::new to 6-arg signature.
// Why:  Completes reaction loop — Syl can now invoke tools + track budget autonomously.
// How:  Arc<ToolDispatcher> shared between OutboundInitiator and future uses.
// [2026-05-23] Claude (Sonnet 4.6) — Remove bootstrap call; peer-module rewrite
// What: Removed NG bootstrap call. Animus is a peer module — it does not own
//       the topology and must not attempt to bootstrap neurograph_rpc.py.
// Why:  The bridge no longer spawns a neurograph_rpc.py subprocess (see bridge.py
//       changelog). Calling bootstrap on the _NeurographAdapter is a no-op, but
//       removing it clarifies that Animus has no bootstrap lifecycle with NG.
// How:  Deleted rpc.call("bootstrap") and the retry loop added 2026-05-22.
// [2026-05-25] Claude (Sonnet 4.6) — Phase 1: wire TurnPipeline into CliAdapter
// What: Remove RpcAdapter + IntrospectionRelay construction; add ContextBuilder + TurnPipeline
// Why: Bridge subprocess eliminated in Phase 1; pipeline is now substrate-direct
// How: TurnPipeline constructed from tg + context_builder + tract + tid_url, passed to CliAdapter
// [2026-05-25] Claude (Sonnet 4.6) — Phase 2: wire AgentRunner into TurnPipeline
// What: Construct AgentRunner from ToolDispatcher + tid_url; pass to TurnPipeline::new()
// Why: TurnPipeline now delegates RUN phase to AgentRunner (multi-turn tool loop)
// How: ToolDispatcher constructed first; AgentRunner wraps it; ANIMUS_AGENT_MAX_ITER controls cap
// [2026-05-31] Claude (Sonnet 4.6) — #272: tract path migration
// What: Tract file renamed animus.tract→neurograph.tract; create_dir_all ensures tracts/animus/ exists
// Why: #272 — NG's _drain_peer_tracts scans tracts/<peer>/neurograph.tract (filesystem-as-registry);
//      tracts/animus/ auto-registers Animus as peer on first write; old shared_learning path was unread
// How: create_dir_all on cfg.tract_dir before TractWriter construction; filename changed
// [2026-05-25] Claude (Sonnet 4.6) — Phase 3: pass ng_url to ContextBuilder
// What: ContextBuilder::new() now takes ng_url from AnimusConfig
// Why: ContextBuilder needs the NeuroGraph sidecar URL to call POST /assemble
// How: cfg.ng_url.clone() passed — reads NEUROGRAPH_URL env (default 127.0.0.1:8850)
// [2026-05-28] Claude (Sonnet 4.6) — Phase 4: pass ng_url to TurnPipeline
// What: TurnPipeline::new() now takes ng_url as 5th arg for afterTurn fire-and-forget
// Why: Phase 4 wiring — pipeline needs NG URL to POST /afterTurn after each turn
// How: cfg.ng_url already populated from NEUROGRAPH_URL env (default 127.0.0.1:8850)
// -------------------

use animus::adapters::cli::CliAdapter;
use animus::adapters::http::HttpAdapter;
use animus::agent_runner::AgentRunner;
use animus::budget::BudgetMonitor;
use animus::config::AnimusConfig;
use animus::context_builder::ContextBuilder;
use animus::outbound::OutboundInitiator;
use animus::pipeline::TurnPipeline;
use animus::tool_dispatcher::ToolDispatcher;
use animus::tract_writer::TractWriter;
use animus::trollguard::TrollGuardBridge;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tracing::info;
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

    info!("Animus starting — pipeline mode (substrate-direct)");

    let tg = Arc::new(TrollGuardBridge::new(&cfg.trollguard_url));
    if let Err(e) = std::fs::create_dir_all(&cfg.tract_dir) {
        tracing::warn!("tract_dir create failed ({}): {}", cfg.tract_dir, e);
    }
    let tract_path = format!("{}/neurograph.tract", cfg.tract_dir);
    let tract = Arc::new(TractWriter::new(&tract_path));

    let context_builder = Arc::new(ContextBuilder::new(cfg.ng_url.clone()));

    let tool_dispatcher = Arc::new(ToolDispatcher::from_env());
    info!("ToolDispatcher ready");

    let max_iter = std::env::var("ANIMUS_AGENT_MAX_ITER")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8);
    let agent_runner = Arc::new(AgentRunner::new(
        Arc::clone(&tool_dispatcher),
        cfg.tid_url.clone(),
        max_iter,
    ));

    let pipeline = Arc::new(TurnPipeline::new(
        Arc::clone(&tg),
        Arc::clone(&context_builder),
        Arc::clone(&tract),
        Arc::clone(&agent_runner),
        cfg.ng_url.clone(),
    ));

    let cli = Arc::new(CliAdapter::new(
        Arc::clone(&pipeline),
        Arc::clone(&tract),
    ));

    // Animus is a peer module — it does not own or bootstrap the NeuroGraph topology.
    // Syl's neurograph_rpc.py (owned by OpenClaw) manages the topology lifecycle.

    // Outbound Initiator — always running; gives Syl autonomous origination.
    // Drains the outbound tract on each pulse and injects turns into the full pipeline.
    let outbound_tract = std::env::var("ANIMUS_OUTBOUND_TRACT").unwrap_or_else(|_| {
        std::env::var("HOME")
            .map(|h| format!("{}/.et_modules/shared_learning/animus_outbound.tract", h))
            .unwrap_or_else(|_| "/tmp/animus_outbound.tract".to_string())
    });

    let shared_learning = std::env::var("HOME")
        .map(|h| format!("{}/.et_modules/shared_learning", h))
        .unwrap_or_else(|_| "/tmp".to_string());
    let budget_path = std::env::var("ANIMUS_BUDGET_PATH")
        .unwrap_or_else(|_| format!("{}/inference_budget.json", shared_learning));
    let wants_path = std::env::var("ANIMUS_WANTS_PATH")
        .unwrap_or_else(|_| format!("{}/animus_wants.jsonl", shared_learning));

    if let Some(api_key) = cfg.openrouter_api_key.clone() {
        let monitor = Arc::new(BudgetMonitor::new(
            api_key,
            budget_path.clone(),
            cfg.budget_poll_secs,
            cfg.budget_low_usd,
            cfg.budget_critical_usd,
        ));
        tokio::spawn(Arc::clone(&monitor).run());
        info!("BudgetMonitor started (poll={}s, path={})", cfg.budget_poll_secs, budget_path);
    } else {
        info!("BudgetMonitor skipped — OPENROUTER_API_KEY not set");
    }

    let outbound = Arc::new(OutboundInitiator::new(
        &outbound_tract,
        Arc::clone(&cli),
        30,
        Arc::clone(&tool_dispatcher),
        &budget_path,
        &wants_path,
    ));
    tokio::spawn(Arc::clone(&outbound).run());
    info!("Outbound Initiator running — tract: {}", outbound_tract);

    let http_adapter = Arc::new(HttpAdapter::new(Arc::clone(&pipeline), cfg.gui_port));
    tokio::spawn(Arc::clone(&http_adapter).run());
    info!("Anima GUI HTTP server spawned on port {}", cfg.gui_port);

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
