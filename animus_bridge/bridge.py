#!/usr/bin/env python3
"""
animus_bridge/bridge.py — Law-compliant NeuroGraph peer adapter.

Spawned by animus-rs RpcAdapter. Reads JSON-RPC calls from stdin, handles
each via the substrate/River pattern, and writes responses to stdout.

Does NOT spawn neurograph_rpc.py. Does NOT create a second NG process.
Animus is a peer module — it writes experience via the per-feeder drop zone
and Syl's NG drains it on its own afterTurn cadence.

# ---- Changelog ----
# [2026-05-10] Claude (Haiku 4.5) — Add readline timeout to _NgSubprocess
# What: Added select.select() timeout helper to prevent indefinite hangs
# Why: Prevent deadlock if NG hangs or crashes mid-call
# How: select.select() with timeouts; TimeoutError caught + converted to error
# [2026-05-10] Claude (Sonnet 4.6) — Initial implementation
# What: Python RPC proxy — stdin/stdout JSON-RPC passthrough to neurograph_rpc.py
# Why: Rust cannot speak NeuroGraph's Python RPC protocol directly
# How: subprocess.Popen for NG, threading.Lock for serialized calls, mock mode
# [2026-05-23] Claude (Sonnet 4.6) — Remove _NgSubprocess; peer-module rewrite
# What: Replace _NgSubprocess (spawned neurograph_rpc.py child) with
#       _NeurographAdapter (River write via per-feeder experience tract).
# Why:  _NgSubprocess caused a 37GB tract flood — Animus's autonomous afterTurn
#       loop triggered topology River deposits to ALL peer tracts on every turn.
#       Law 1: Animus is a peer module and communicates through the substrate,
#       not through a duplicate NG process that runs the full afterTurn pipeline.
# How:  bootstrap → no-op (Animus doesn't own the topology);
#       ingest → ng_tract.deposit_experience to ~/.et_modules/experience/animus.tract
#         (same per-feeder drop zone as TID, #141 — drained by Syl's NG on heartbeat);
#       assemble → empty systemPromptAddition (seed text carries substrate context);
#       afterTurn → no-op (Syl's NG runs its own step).
# -------------------
"""
from __future__ import annotations

import argparse
import json
import os
import sys


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mock-ng", action="store_true",
                   help="Use in-process mock instead of River adapter (for tests)")
    return p.parse_args()


# ── Mock for testing ────────────────────────────────────────────────────

class _MockNg:
    """Minimal in-process mock that satisfies the JSON-RPC protocol."""

    RESPONSES: dict = {
        "bootstrap": {"status": "ok", "already_initialized": False},
        "ingest": {"ingested": True},
        "assemble": {"systemPromptAddition": "mock substrate context", "messages": []},
        "afterTurn": None,
        "stats": {"nodes": 0, "synapses": 0},
        "dispose": None,
    }

    def call(self, method: str, params: dict, req_id) -> dict:
        if method not in self.RESPONSES:
            return {"jsonrpc": "2.0", "id": req_id,
                    "error": {"code": -32601, "message": f"Method not found: {method}"}}
        result = self.RESPONSES[method]
        return {"jsonrpc": "2.0", "id": req_id, "result": result}


# ── River-based NeuroGraph peer adapter ────────────────────────────────

_EXPERIENCE_DROP_ZONE = os.path.expanduser("~/.et_modules/experience")
_ANIMUS_TRACT = os.path.join(_EXPERIENCE_DROP_ZONE, "animus.tract")


class _NeurographAdapter:
    """Law-compliant peer-module interface to Syl's substrate.

    Animus is a peer module. It does NOT spawn neurograph_rpc.py.
    Experience is deposited to the per-feeder drop zone; Syl's
    _scan_drain_pulse_loop drains it on a 2s heartbeat.
    """

    def call(self, method: str, params: dict, req_id) -> dict:
        try:
            result = self._dispatch(method, params)
            return {"jsonrpc": "2.0", "id": req_id, "result": result}
        except Exception as exc:
            return {"jsonrpc": "2.0", "id": req_id,
                    "error": {"code": -32000, "message": str(exc)}}

    def _dispatch(self, method: str, params: dict) -> dict:
        if method == "bootstrap":
            # Animus does not own the topology. Syl's process bootstraps NG.
            return {"bootstrapped": True, "reason": "animus_peer_mode"}
        if method == "assemble":
            # Peer modules read context from the River. TonicBridge seeds already
            # carry substrate-derived intent. River-read assemble is a future pass.
            return {"systemPromptAddition": "", "messages": []}
        if method == "ingest":
            self._ingest(params)
            return {"ingested": True}
        if method == "afterTurn":
            # Syl's NG runs its own afterTurn (SNN step + River deposits).
            # Animus must not trigger a duplicate cycle.
            return {}
        if method == "dispose":
            return {}
        return {}

    def _ingest(self, params: dict) -> None:
        """Deposit turn text to Syl's experience tract drop zone.

        ~/.et_modules/experience/animus.tract is drained by Syl's NG on its
        2s heartbeat (_scan_drain_pulse_loop). Same path as TID (#141).
        """
        msg = params.get("message", {})
        text = msg.get("content", "") if isinstance(msg, dict) else str(msg)
        if not text.strip():
            return
        try:
            import ng_tract
            os.makedirs(_EXPERIENCE_DROP_ZONE, exist_ok=True)
            ng_tract.deposit_experience(text, "animus", _ANIMUS_TRACT)
        except Exception as exc:
            import logging
            logging.getLogger("animus_bridge").debug("deposit_experience failed: %s", exc)


# ── Main RPC loop ───────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    ng: _MockNg | _NeurographAdapter
    if args.mock_ng:
        ng = _MockNg()
    else:
        ng = _NeurographAdapter()

    # Signal readiness to the Rust parent
    ready = json.dumps({"jsonrpc": "2.0", "method": "ready",
                        "params": {"pid": os.getpid()}})
    sys.stdout.write(ready + "\n")
    sys.stdout.flush()

    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue

        try:
            request = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            sys.stdout.write(json.dumps({
                "jsonrpc": "2.0", "id": None,
                "error": {"code": -32700, "message": f"Parse error: {exc}"}
            }) + "\n")
            sys.stdout.flush()
            continue

        req_id = request.get("id")
        method = request.get("method", "")
        params = request.get("params", {})

        # Inject channel context from Animus metadata into params
        channel_ctx = params.pop("_animus_channel_context", None)
        if channel_ctx and "message" in params:
            params.setdefault("metadata", {})["channel_context"] = channel_ctx

        response = ng.call(method, params, req_id)
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
