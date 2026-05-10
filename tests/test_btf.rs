use animus::tract_writer::{TractWriter, MAGIC, VERSION, ENTRY_OUTCOME};
use std::fs;
use std::io::Read;

#[test]
fn btf_frame_starts_with_magic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("animus.tract");
    let writer = TractWriter::new(path.to_str().unwrap());

    writer.deposit_event("channel_connection", serde_json::json!({
        "channel_id": "test-chan",
        "user_id": "josh",
        "channel_type": "cli",
    })).unwrap();

    let mut buf = vec![0u8; 2];
    let mut f = fs::File::open(&path).unwrap();
    f.read_exact(&mut buf).unwrap();

    // First 2 bytes must be BTF magic 0x4254 ("BT")
    let magic = u16::from_ne_bytes([buf[0], buf[1]]);
    assert_eq!(magic, MAGIC);
}

#[test]
fn btf_frame_version_and_type() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("animus2.tract");
    let writer = TractWriter::new(path.to_str().unwrap());

    writer.deposit_event("tg_outcome", serde_json::json!({"verdict": "SAFE"})).unwrap();

    let mut buf = vec![0u8; 4];
    let mut f = fs::File::open(&path).unwrap();
    f.read_exact(&mut buf).unwrap();

    assert_eq!(buf[2], VERSION);
    assert_eq!(buf[3], ENTRY_OUTCOME);
}
