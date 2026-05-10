#!/usr/bin/env python3
"""
animus_bridge/bridge.py — Python RPC proxy between animus-rs and neurograph_rpc.py.

This process is spawned by the Rust RPC adapter. It:
  1. Optionally spawns neurograph_rpc.py as a child process (unless --mock-ng).
  2. Reads JSON-RPC calls from stdin (from Rust).
  3. Forwards them to neurograph_rpc.py's stdin (or mock).
  4. Writes neurograph_rpc.py's response to stdout (back to Rust).

It does NOT import NeuroGraphMemory or touch substrate bytes. It is a protocol
proxy with channel-context metadata injection.

# ---- Changelog ----
# [2026-05-10] Claude (Sonnet 4.6) — Initial implementation
# What: Python RPC proxy — stdin/stdout JSON-RPC passthrough to neurograph_rpc.py
# Why: Rust cannot speak NeuroGraph's Python RPC protocol directly
# How: subprocess.Popen for NG, threading.Lock for serialized calls, mock mode for tests
# -------------------
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mock-ng", action="store_true",
                   help="Use an in-process mock NG instead of spawning neurograph_rpc.py")
    p.add_argument("--ng-path", default=None,
                   help="Path to neurograph_rpc.py (default: NEUROGRAPH_RPC_PATH env)")
    return p.parse_args()


# ── Mock NG for testing ─────────────────────────────────────────────────

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


# ── Live NG subprocess ──────────────────────────────────────────────────

class _NgSubprocess:
    """Manages the neurograph_rpc.py child process and its stdin/stdout pipe."""

    def __init__(self, ng_path: str) -> None:
        self._ng_path = ng_path
        self._proc: subprocess.Popen | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        self._proc = subprocess.Popen(
            [sys.executable, self._ng_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
            text=True,
            bufsize=1,
        )
        ready_line = self._proc.stdout.readline()
        try:
            ready = json.loads(ready_line)
            if ready.get("method") != "ready":
                raise RuntimeError(f"Unexpected first message from NG: {ready_line!r}")
        except json.JSONDecodeError:
            raise RuntimeError(f"NG did not send ready signal, got: {ready_line!r}")

    def call(self, method: str, params: dict, req_id) -> dict:
        with self._lock:
            if self._proc is None or self._proc.poll() is not None:
                return {"jsonrpc": "2.0", "id": req_id,
                        "error": {"code": -32000, "message": "NG process not running"}}
            request = json.dumps({"jsonrpc": "2.0", "id": req_id,
                                  "method": method, "params": params})
            self._proc.stdin.write(request + "\n")
            self._proc.stdin.flush()
            response_line = self._proc.stdout.readline()
            if not response_line:
                return {"jsonrpc": "2.0", "id": req_id,
                        "error": {"code": -32000, "message": "NG process closed"}}
            return json.loads(response_line)

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()


# ── Main RPC loop ───────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()

    if args.mock_ng:
        ng: _MockNg | _NgSubprocess = _MockNg()
    else:
        ng_path = args.ng_path or os.environ.get(
            "NEUROGRAPH_RPC_PATH", "/home/josh/NeuroGraph/neurograph_rpc.py")
        ng = _NgSubprocess(ng_path)
        ng.start()

    # Signal readiness to the Rust parent (same protocol as neurograph_rpc.py)
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
            error_resp = json.dumps({
                "jsonrpc": "2.0", "id": None,
                "error": {"code": -32700, "message": f"Parse error: {exc}"}
            })
            sys.stdout.write(error_resp + "\n")
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

    if hasattr(ng, "stop"):
        ng.stop()


if __name__ == "__main__":
    main()
