use animus::rpc_adapter::RpcAdapter;
use std::io::Write;

fn mock_bridge_path() -> tempfile::NamedTempFile {
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
    let mut file = tempfile::Builder::new()
        .suffix(".py")
        .tempfile()
        .unwrap();
    file.write_all(script.as_bytes()).unwrap();
    file
}

#[tokio::test]
async fn rpc_adapter_ingest_roundtrip() {
    let bridge_file = mock_bridge_path();
    let bridge_path = bridge_file.path().to_str().unwrap();
    let adapter = RpcAdapter::new(bridge_path).await.unwrap();

    let result = adapter.call("ingest",
        serde_json::json!({"message": {"role": "user", "content": "hello"}})).await;
    assert!(result.is_ok());
    let resp = result.unwrap();
    assert_eq!(resp["ingested"], serde_json::Value::Bool(true));
    // bridge_file drops here, cleaning up the temp file
}
