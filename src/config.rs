// ---- Changelog ----
// 2026-05-10 Task3/config — AnimusConfig reads all credentials from environment
// What: Env-var-backed config struct with typed fields and defaults
// Why: Law 5 — all config from .bashrc/environment, no hardcoded values
// How: std::env::var() with sensible defaults; only ANIMUS_AUTH_TOKEN is required
// -------------------

use std::env;

#[derive(Debug, Clone)]
pub struct AnimusConfig {
    /// Auth token for WebSocket connections. Set: ANIMUS_AUTH_TOKEN
    pub auth_token: String,
    /// TrollGuard HTTP base URL. Default: http://127.0.0.1:7438
    pub trollguard_url: String,
    /// TID HTTP base URL. Default: http://127.0.0.1:7437
    pub tid_url: String,
    /// Absolute path to neurograph_rpc.py. Default: /home/josh/NeuroGraph/neurograph_rpc.py
    pub neurograph_rpc_path: String,
    /// Absolute path to animus_bridge/bridge.py. Default: /home/josh/Animus/animus_bridge/bridge.py
    pub bridge_path: String,
    /// Discord bot token (optional — Discord adapter disabled if unset). Set: DISCORD_TOKEN
    pub discord_token: Option<String>,
    /// Directory for Animus module tract files. Default: ~/.et_modules/shared_learning
    pub tract_dir: String,
    /// WebSocket server port. Default: 8848
    pub ws_port: u16,
    /// CES dashboard URL. Default: http://127.0.0.1:8847
    pub ces_url: String,
}

impl AnimusConfig {
    pub fn from_env() -> Result<Self, String> {
        let auth_token = env::var("ANIMUS_AUTH_TOKEN")
            .map_err(|_| "ANIMUS_AUTH_TOKEN not set in environment".to_string())?;

        let home = env::var("HOME").unwrap_or_else(|_| "/home/josh".to_string());
        let default_tract_dir = format!("{}/.et_modules/shared_learning", home);

        Ok(Self {
            auth_token,
            trollguard_url: env::var("TROLLGUARD_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:7438".to_string()),
            tid_url: env::var("TID_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:7437".to_string()),
            neurograph_rpc_path: env::var("NEUROGRAPH_RPC_PATH")
                .unwrap_or_else(|_| "/home/josh/NeuroGraph/neurograph_rpc.py".to_string()),
            bridge_path: env::var("ANIMUS_BRIDGE_PATH")
                .unwrap_or_else(|_| "/home/josh/Animus/animus_bridge/bridge.py".to_string()),
            discord_token: env::var("DISCORD_TOKEN").ok(),
            tract_dir: env::var("ANIMUS_TRACT_DIR").unwrap_or(default_tract_dir),
            ws_port: {
                let port_str = env::var("ANIMUS_WS_PORT").unwrap_or_else(|_| "8848".to_string());
                port_str
                    .parse::<u16>()
                    .map_err(|_| format!("ANIMUS_WS_PORT='{}' is not a valid u16 port", port_str))?
            },
            ces_url: env::var("CES_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8847".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn config_reads_required_vars() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("ANIMUS_AUTH_TOKEN", "test_token_123");
        let config = AnimusConfig::from_env().unwrap();
        assert_eq!(config.auth_token, "test_token_123");
        assert_eq!(config.trollguard_url, "http://127.0.0.1:7438");
        assert_eq!(config.ws_port, 8848);
        std::env::remove_var("ANIMUS_AUTH_TOKEN");
    }

    #[test]
    fn config_missing_auth_token_is_error() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("ANIMUS_AUTH_TOKEN");
        let result = AnimusConfig::from_env();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("ANIMUS_AUTH_TOKEN"));
    }
}
