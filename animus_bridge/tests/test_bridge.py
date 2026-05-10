import json
import subprocess
import sys
import os

BRIDGE_PATH = os.path.join(os.path.dirname(__file__), "..", "bridge.py")


def test_bridge_handles_bad_json():
    """Bridge must not crash on malformed input — return parse error."""
    proc = subprocess.Popen(
        [sys.executable, BRIDGE_PATH, "--mock-ng"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )
    # Consume the ready signal first (bridge sends this before reading stdin)
    proc.stdout.readline()
    proc.stdin.write("not valid json\n")
    proc.stdin.flush()
    line = proc.stdout.readline()
    proc.terminate()
    resp = json.loads(line)
    assert resp.get("error") is not None
    assert resp["error"]["code"] == -32700


def test_bridge_forwards_ingest():
    """Bridge must forward ingest call and return result from mock NG."""
    proc = subprocess.Popen(
        [sys.executable, BRIDGE_PATH, "--mock-ng"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1,
    )
    ready = proc.stdout.readline()
    ready_msg = json.loads(ready)
    assert ready_msg["method"] == "ready"

    req = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ingest",
                      "params": {"message": {"role": "user", "content": "hello"}}})
    proc.stdin.write(req + "\n")
    proc.stdin.flush()
    resp_line = proc.stdout.readline()
    proc.terminate()
    resp = json.loads(resp_line)
    assert resp["id"] == 1
    assert "result" in resp
    assert resp["result"].get("ingested") == True
