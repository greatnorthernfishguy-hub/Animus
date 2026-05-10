#!/usr/bin/env bash
# ---- Changelog ----
# [2026-05-10] Claude (Sonnet 4.6) — Task 1: Initial scaffold
# What: install.sh — vendors NG files, builds Rust binary
# Why: Standard ecosystem install pattern (mirrors other modules)
# How: cp canonical NG files, then cargo build --release
# -------------------

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Animus install ==="

# Vendor canonical NG files
echo "Vendoring NG files..."
for f in ng_lite.py ng_tract_bridge.py ng_ecosystem.py ng_autonomic.py openclaw_adapter.py ng_embed.py ng_peer_bridge.py; do
    cp /home/josh/NeuroGraph/$f "$SCRIPT_DIR/$f"
done

# Build Rust binary
echo "Building animus-rs..."
cd "$SCRIPT_DIR"
cargo build --release

echo "=== Done. Binary at target/release/animus ==="
echo "Add to .bashrc: export ANIMUS_AUTH_TOKEN=<your_token>"
