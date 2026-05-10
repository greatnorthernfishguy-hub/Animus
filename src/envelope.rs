// src/envelope.rs
// Turn envelope and channel context types for the Animus gateway
// Part of the native agentic gateway for E-T Systems / NeuroGraph Ecosystem
//
// ---- Changelog ----
// 2026-05-10 Task2/envelope — TurnEnvelope + ChannelContext types
// What: Normalized turn envelope and channel context structs with serde
// Why: Common currency between channel adapters and the RPC adapter (spec §2)
// How: Plain structs + serde derive; tested with roundtrip JSON
// -------------------

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Identifies which channel an inbound turn arrived on (or should be delivered to outbound).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChannelKind {
    Cli,
    Discord,
}

/// Per-channel context that adapters fill in before handing a turn to the core pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChannelContext {
    pub channel_kind: ChannelKind,
    /// Opaque channel-specific ID (e.g. Discord guild#channel, or "cli" for the CLI adapter).
    pub channel_id: String,
    /// Opaque user ID — Discord snowflake, CLI username, etc.
    pub user_id: String,
}

/// Normalized turn envelope — the common currency between channel adapters and the core pipeline.
/// Channel adapters produce these; the RPC adapter consumes them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TurnEnvelope {
    pub text: String,
    pub context: ChannelContext,
    /// Arbitrary key-value metadata adapters may attach (e.g. Discord message_id).
    pub metadata: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_envelope_roundtrip_json() {
        let env = TurnEnvelope {
            text: "hello world".to_string(),
            context: ChannelContext {
                channel_kind: ChannelKind::Cli,
                channel_id: "cli".to_string(),
                user_id: "josh".to_string(),
            },
            metadata: HashMap::new(),
        };
        let json = serde_json::to_string(&env).unwrap();
        let decoded: TurnEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn channel_kind_discord_roundtrip() {
        let kind = ChannelKind::Discord;
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: ChannelKind = serde_json::from_str(&json).unwrap();
        assert_eq!(kind, decoded);
    }

    #[test]
    fn channel_kind_cli_roundtrip() {
        let kind = ChannelKind::Cli;
        let json = serde_json::to_string(&kind).unwrap();
        let decoded: ChannelKind = serde_json::from_str(&json).unwrap();
        assert_eq!(kind, decoded);
    }

    #[test]
    fn turn_envelope_with_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("message_id".to_string(), "123456789".to_string());
        metadata.insert("timestamp".to_string(), "1684000000".to_string());

        let env = TurnEnvelope {
            text: "test message".to_string(),
            context: ChannelContext {
                channel_kind: ChannelKind::Discord,
                channel_id: "guild#general".to_string(),
                user_id: "user_snowflake".to_string(),
            },
            metadata,
        };

        let json = serde_json::to_string(&env).unwrap();
        let decoded: TurnEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.metadata.get("message_id"), Some(&"123456789".to_string()));
        assert_eq!(decoded.metadata.get("timestamp"), Some(&"1684000000".to_string()));
    }

    #[test]
    fn turn_envelope_rejects_missing_fields() {
        // Missing required field: metadata
        let json = r#"{"text": "hello", "context": {"channel_kind": "Cli", "channel_id": "cli", "user_id": "josh"}}"#;
        let result: Result<TurnEnvelope, _> = serde_json::from_str(json);
        assert!(result.is_err(), "Should reject TurnEnvelope with missing required fields");
    }
}
