use animus::rpc_adapter::RpcAdapter;
use std::path::PathBuf;

fn mock_bridge_path() -> PathBuf {
    // A tiny Python script that immediately emits "ready" then echoes ingest as ingested=true
    let script = r#"
import json, sys, os
ready = json.dumps({"jsonrpc":"2.0","method":"ready","params":{"pid":os.getpid()}})
sys.stdout.write(ready + "\n"); sys.stdout.flush()
for line in sys.stdin:
    req = json.loads(line.strip())
    resp = json.dumps({"jsonrpc":"2.0","id":req.get("id"),"result":{"ingested":True}})
    sys.stdout.write(resp + "\n"); sys.stdout.flush()
"#;
    let path = std::path::PathBuf::from("/tmp/animus_mock_bridge.py");
    std::fs::write(&path, script).unwrap();
    path
}

#[tokio::test]
async fn rpc_adapter_ingest_roundtrip() {
    let bridge_path = mock_bridge_path();
    let adapter = RpcAdapter::new(bridge_path.to_str().unwrap()).await.unwrap();

    let result = adapter.call("ingest",
        serde_json::json!({"message": {"role": "user", "content": "hello"}})).await;
    assert!(result.is_ok());
    let resp = result.unwrap();
    assert_eq!(resp["ingested"], serde_json::Value::Bool(true));
}
