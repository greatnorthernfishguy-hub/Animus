// src/envelope.rs
// Turn envelope and channel context types for the Anima gateway
// Part of the native agentic gateway for E-T Systems / NeuroGraph Ecosystem
//
// ---- Changelog ----
// 2026-05-10 Task2/envelope — TurnEnvelope + ChannelContext types
// What: Normalized turn envelope and channel context structs with serde
// Why: Common currency between channel adapters and the RPC adapter (spec §2)
// How: Plain structs + serde derive; tested with roundtrip JSON
// 2026-05-10 Task10/cli-adapter — ChannelContext updated for CLI adapter
// What: Removed ChannelKind enum; replaced channel_kind with channel_type: String;
//       added connection_start: f64; added TurnEnvelope::new() constructor
// Why: CLI adapter uses string channel_type and needs connection_start timestamp;
//      ChannelKind enum was only used internally and adds no value
// How: Struct field swap + new() constructor with empty metadata default
// -------------------

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Per-channel context that adapters fill in before handing a turn to the core pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChannelContext {
    pub channel_id: String,
    pub user_id: String,
    pub channel_type: String,
    pub connection_start: f64,
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

impl TurnEnvelope {
    pub fn new(text: &str, context: ChannelContext) -> Self {
        Self {
            text: text.to_string(),
            context,
            metadata: std::collections::HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_envelope_roundtrip_json() {
        let env = TurnEnvelope {
            text: "hello world".to_string(),
            context: ChannelContext {
                channel_type: "cli".to_string(),
                channel_id: "cli".to_string(),
                user_id: "josh".to_string(),
                connection_start: 0.0,
            },
            metadata: HashMap::new(),
        };
        let json = serde_json::to_string(&env).unwrap();
        let decoded: TurnEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(env, decoded);
    }

    #[test]
    fn turn_envelope_with_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("message_id".to_string(), "123456789".to_string());
        metadata.insert("timestamp".to_string(), "1684000000".to_string());

        let env = TurnEnvelope {
            text: "test message".to_string(),
            context: ChannelContext {
                channel_type: "discord".to_string(),
                channel_id: "guild#general".to_string(),
                user_id: "user_snowflake".to_string(),
                connection_start: 0.0,
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
        let json = r#"{"text": "hello", "context": {"channel_type": "cli", "channel_id": "cli", "user_id": "josh", "connection_start": 0.0}}"#;
        let result: Result<TurnEnvelope, _> = serde_json::from_str(json);
        assert!(result.is_err(), "Should reject TurnEnvelope with missing required fields");
    }

    #[test]
    fn turn_envelope_new_constructor() {
        let ctx = ChannelContext {
            channel_id: "cli".to_string(),
            user_id: "josh".to_string(),
            channel_type: "cli".to_string(),
            connection_start: 1234567890.0,
        };
        let env = TurnEnvelope::new("hello", ctx.clone());
        assert_eq!(env.text, "hello");
        assert_eq!(env.context, ctx);
        assert!(env.metadata.is_empty());
    }
}
