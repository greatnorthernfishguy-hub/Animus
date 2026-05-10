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

#[test]
fn btf_frame_total_length_matches_actual_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("animus_len.tract");
    let writer = TractWriter::new(path.to_str().unwrap());

    writer.deposit_event("test_event", serde_json::json!({"key": "value"})).unwrap();

    let file_bytes = fs::read(&path).unwrap();
    // total_length field is at bytes [4..8], native endian u32
    let total_length = u32::from_ne_bytes([file_bytes[4], file_bytes[5], file_bytes[6], file_bytes[7]]);
    // Must match actual file size (envelope + payload)
    assert_eq!(total_length as usize, file_bytes.len(), "total_length field must equal actual frame size");
}

#[test]
fn btf_frame_checksum_matches_payload() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("animus_crc.tract");
    let writer = TractWriter::new(path.to_str().unwrap());

    writer.deposit_event("test_event", serde_json::json!({"key": "value"})).unwrap();

    let file_bytes = fs::read(&path).unwrap();
    // checksum field is at bytes [16..20]
    let stored_checksum = u32::from_ne_bytes([
        file_bytes[16], file_bytes[17], file_bytes[18], file_bytes[19]
    ]);
    // payload starts at byte 24
    let payload_bytes = &file_bytes[24..];
    let computed = crc32fast::hash(payload_bytes);
    assert_eq!(stored_checksum, computed, "CRC32 checksum must match payload");
}

#[test]
fn btf_frame_endian_flag_is_correct() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("animus_endian.tract");
    let writer = TractWriter::new(path.to_str().unwrap());

    writer.deposit_event("test_event", serde_json::json!({})).unwrap();

    let file_bytes = fs::read(&path).unwrap();
    // endian flag is at byte [20]
    #[cfg(target_endian = "little")]
    assert_eq!(file_bytes[20], 0x01, "Should be 0x01 on little-endian");
    #[cfg(target_endian = "big")]
    assert_eq!(file_bytes[20], 0x02, "Should be 0x02 on big-endian");
}

#[test]
fn btf_multi_event_file_parses_sequentially() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("animus_multi.tract");
    let writer = TractWriter::new(path.to_str().unwrap());

    writer.deposit_event("event_a", serde_json::json!({"n": 1})).unwrap();
    writer.deposit_event("event_b", serde_json::json!({"n": 2})).unwrap();

    let file_bytes = fs::read(&path).unwrap();

    // Read first frame: total_length at bytes [4..8]
    let first_len = u32::from_ne_bytes([file_bytes[4], file_bytes[5], file_bytes[6], file_bytes[7]]) as usize;
    assert!(first_len < file_bytes.len(), "First frame length must be less than total file size");

    // Second frame starts at first_len
    let second_frame = &file_bytes[first_len..];
    let second_magic = u16::from_ne_bytes([second_frame[0], second_frame[1]]);
    assert_eq!(second_magic, MAGIC, "Second frame must also start with BTF magic");
}
