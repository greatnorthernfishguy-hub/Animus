// ---- Changelog ----
// [2026-05-31] Claude (Sonnet 4.6) — Anima GUI Task 4: add gui_port
// What: New field gui_port reads ANIMUS_GUI_PORT env var (default 8848)
// Why: HttpAdapter needs the port from config (Law 5 — config from env)
// How: Same parse-with-error pattern as ws_port
// [2026-05-31] Claude (Sonnet 4.6) — #272: migrate tract_dir to ~/.et_modules/tracts/animus
// What: Default tract_dir changed from shared_learning to tracts/animus; file renamed animus→neurograph
// Why: #272 — shared_learning was legacy peer-bridge dir; tracts/<module>/neurograph.tract is the
//      correct per-module deposit shape NG's _drain_peer_tracts scans (filesystem-as-registry pattern)
// How: New default; main.rs adds create_dir_all + uses neurograph.tract filename
// [2026-05-25] Claude (Sonnet 4.6) — Phase 3: add ng_url field
// What: New field ng_url reads NEUROGRAPH_URL env var (default http://127.0.0.1:8850)
// Why: ContextBuilder needs the NeuroGraph HTTP sidecar URL (Law 5 — config from env)
// How: Optional env var with hardcoded default matching NG's sidecar listen port
// [2026-05-25] Claude (Sonnet 4.6) — Phase 1: remove bridge_path + neurograph_rpc_path
// What: Removed bridge_path and neurograph_rpc_path from AnimaConfig
// Why: bridge.py subprocess eliminated in Phase 1 — these fields have no consumers
// How: Fields + from_env() assignments removed; env var ANIMUS_BRIDGE_PATH no longer read
// 2026-05-10 Task3/config — AnimaConfig reads all credentials from environment
// What: Env-var-backed config struct with typed fields and defaults
// Why: Law 5 — all config from .bashrc/environment, no hardcoded values
// How: std::env::var() with sensible defaults; only ANIMUS_AUTH_TOKEN is required
// -------------------

use std::env;

#[derive(Debug, Clone)]
pub struct AnimaConfig {
    /// Auth token for WebSocket connections. Set: ANIMUS_AUTH_TOKEN
    pub auth_token: String,
    /// TrollGuard HTTP base URL. Default: http://127.0.0.1:7438
    pub trollguard_url: String,
    /// TID HTTP base URL. Default: http://127.0.0.1:7437
    pub tid_url: String,
    /// Discord bot token (optional — Discord adapter disabled if unset). Set: DISCORD_TOKEN
    pub discord_token: Option<String>,
    /// Directory for Animus module tract files. Default: ~/.et_modules/tracts/animus
    pub tract_dir: String,
    /// WebSocket server port. Default: 8848
    pub ws_port: u16,
    /// GUI HTTP server port. Default: 8848. Set: ANIMUS_GUI_PORT
    pub gui_port: u16,
    /// CES dashboard URL. Default: http://127.0.0.1:8847
    pub ces_url: String,
    /// OpenRouter API key for budget polling. Set: OPENROUTER_API_KEY (optional)
    pub openrouter_api_key: Option<String>,
    /// Budget "low" threshold in USD. Default: 10.0. Set: ANIMUS_BUDGET_LOW_USD
    pub budget_low_usd: f64,
    /// Budget "critical" threshold in USD. Default: 2.0. Set: ANIMUS_BUDGET_CRITICAL_USD
    pub budget_critical_usd: f64,
    /// Budget poll interval in seconds. Default: 300. Set: ANIMUS_BUDGET_POLL_SECS
    pub budget_poll_secs: u64,
    /// SearXNG or similar search endpoint. Set: ANIMUS_SEARCH_URL (optional)
    pub search_url: Option<String>,
    /// Comma-separated allowed paths for read_file tool. Set: ANIMUS_ALLOWED_PATHS
    pub allowed_paths: String,
    /// NeuroGraph HTTP sidecar base URL. Default: http://127.0.0.1:8850. Set: NEUROGRAPH_URL
    pub ng_url: String,
}

impl AnimaConfig {
    pub fn from_env() -> Result<Self, String> {
        let auth_token = env::var("ANIMUS_AUTH_TOKEN")
            .map_err(|_| "ANIMUS_AUTH_TOKEN not set in environment".to_string())?;

        // LAW 5 — fail-fast if HOME is unset; no hardcoded user paths
        let home = env::var("HOME").map_err(|_| "HOME env var not set".to_string())?;
        let default_tract_dir = format!("{}/.et_modules/tracts/animus", home);

        Ok(Self {
            auth_token,
            trollguard_url: env::var("TROLLGUARD_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:7438".to_string()),
            tid_url: env::var("TID_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:7437".to_string()),
            discord_token: env::var("DISCORD_TOKEN").ok(),
            tract_dir: env::var("ANIMUS_TRACT_DIR").unwrap_or(default_tract_dir),
            ws_port: {
                let port_str = env::var("ANIMUS_WS_PORT").unwrap_or_else(|_| "8848".to_string());
                port_str
                    .parse::<u16>()
                    .map_err(|_| format!("ANIMUS_WS_PORT='{}' is not a valid u16 port", port_str))?
            },
            gui_port: {
                let port_str = env::var("ANIMUS_GUI_PORT").unwrap_or_else(|_| "8848".to_string());
                port_str
                    .parse::<u16>()
                    .map_err(|_| format!("ANIMUS_GUI_PORT='{}' is not a valid u16 port", port_str))?
            },
            ces_url: env::var("CES_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8847".to_string()),
            openrouter_api_key: env::var("OPENROUTER_API_KEY").ok(),
            budget_low_usd: env::var("ANIMUS_BUDGET_LOW_USD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10.0),
            budget_critical_usd: env::var("ANIMUS_BUDGET_CRITICAL_USD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2.0),
            budget_poll_secs: env::var("ANIMUS_BUDGET_POLL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            search_url: env::var("ANIMUS_SEARCH_URL").ok(),
            allowed_paths: env::var("ANIMUS_ALLOWED_PATHS").unwrap_or_else(|_| {
                let home = env::var("HOME").unwrap_or_default();
                format!("{}/.et_modules,{}/docs", home, home)
            }),
            ng_url: env::var("NEUROGRAPH_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8850".to_string()),
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
        let config = AnimaConfig::from_env().unwrap();
        assert_eq!(config.auth_token, "test_token_123");
        assert_eq!(config.trollguard_url, "http://127.0.0.1:7438");
        assert_eq!(config.ws_port, 8848);
        std::env::remove_var("ANIMUS_AUTH_TOKEN");
    }

    #[test]
    fn config_missing_auth_token_is_error() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("ANIMUS_AUTH_TOKEN");
        let result = AnimaConfig::from_env();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("ANIMUS_AUTH_TOKEN"));
    }

    #[test]
    fn config_new_fields_have_correct_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("ANIMUS_AUTH_TOKEN", "tok");
        std::env::remove_var("OPENROUTER_API_KEY");
        std::env::remove_var("ANIMUS_BUDGET_LOW_USD");
        std::env::remove_var("ANIMUS_BUDGET_CRITICAL_USD");
        std::env::remove_var("ANIMUS_BUDGET_POLL_SECS");
        std::env::remove_var("ANIMUS_SEARCH_URL");
        std::env::remove_var("ANIMUS_ALLOWED_PATHS");
        let cfg = AnimaConfig::from_env().unwrap();
        assert!(cfg.openrouter_api_key.is_none());
        assert!((cfg.budget_low_usd - 10.0).abs() < 0.01);
        assert!((cfg.budget_critical_usd - 2.0).abs() < 0.01);
        assert_eq!(cfg.budget_poll_secs, 300);
        assert!(cfg.search_url.is_none());
        // ANIMUS_ALLOWED_PATHS default contains $HOME/.et_modules
        assert!(cfg.allowed_paths.contains(".et_modules"));
        std::env::remove_var("ANIMUS_AUTH_TOKEN");
    }

    #[test]
    fn config_ng_url_default() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("ANIMUS_AUTH_TOKEN", "tok");
        std::env::remove_var("NEUROGRAPH_URL");
        let cfg = AnimaConfig::from_env().unwrap();
        assert_eq!(cfg.ng_url, "http://127.0.0.1:8850");
        std::env::remove_var("ANIMUS_AUTH_TOKEN");
    }

    #[test]
    fn config_ng_url_from_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("ANIMUS_AUTH_TOKEN", "tok");
        std::env::set_var("NEUROGRAPH_URL", "http://192.168.1.10:8850");
        let cfg = AnimaConfig::from_env().unwrap();
        assert_eq!(cfg.ng_url, "http://192.168.1.10:8850");
        std::env::remove_var("ANIMUS_AUTH_TOKEN");
        std::env::remove_var("NEUROGRAPH_URL");
    }

    #[test]
    fn config_gui_port_default() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("ANIMUS_AUTH_TOKEN", "tok");
        std::env::remove_var("ANIMUS_GUI_PORT");
        let cfg = AnimaConfig::from_env().unwrap();
        assert_eq!(cfg.gui_port, 8848);
        std::env::remove_var("ANIMUS_AUTH_TOKEN");
    }

    #[test]
    fn config_gui_port_from_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("ANIMUS_AUTH_TOKEN", "tok");
        std::env::set_var("ANIMUS_GUI_PORT", "9090");
        let cfg = AnimaConfig::from_env().unwrap();
        assert_eq!(cfg.gui_port, 9090);
        std::env::remove_var("ANIMUS_AUTH_TOKEN");
        std::env::remove_var("ANIMUS_GUI_PORT");
    }
}
