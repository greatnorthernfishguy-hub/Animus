// ---- Changelog ----
// 2026-05-10 Task6/trollguard — TrollGuard HTTP client
// What: Async HTTP client for TrollGuard perimeter scan; graceful fallback when unavailable
// Why: All turns must pass TrollGuard perimeter before reaching the turn pipeline (spec §2)
// How: reqwest::Client POST /scan/text; on any failure, allow + set tg_unavailable=true
// -------------------

use serde::{Deserialize, Serialize};
use tracing::warn;

pub struct TrollGuardBridge {
    base_url: String,
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct ScanResult {
    /// True if the verdict is SAFE or SUSPICIOUS (not MALICIOUS). Also true when TG is unavailable.
    pub is_clean: bool,
    /// The text returned by TrollGuard after scanning. When `tg_unavailable=true`, this is the
    /// raw input text (not scanned). Check `tg_unavailable` before assuming this is clean.
    pub sanitized_text: String,
    /// TrollGuard's verdict string: "SAFE", "SUSPICIOUS", "MALICIOUS", "TG_UNAVAILABLE", or "TG_PARSE_ERROR".
    /// Use this field to distinguish SUSPICIOUS from SAFE when routing decisions matter.
    pub verdict: String,
    /// True when TrollGuard could not be reached or returned an unparseable response.
    /// The turn is allowed through but downstream should treat `sanitized_text` as unscanned.
    pub tg_unavailable: bool,
}

#[derive(Serialize)]
struct ScanRequest<'a> {
    text: &'a str,
    source: &'a str,
}

#[derive(Deserialize)]
struct ScanResponse {
    verdict: String,
    sanitized_text: String,
}

impl TrollGuardBridge {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("failed to build reqwest client"),
        }
    }

    pub async fn scan(&self, text: &str, source: &str) -> ScanResult {
        let url = format!("{}/scan/text", self.base_url);
        let body = ScanRequest { text, source };

        match self.client.post(&url).json(&body).send().await {
            Err(e) => {
                warn!("TrollGuard unreachable: {} — allowing turn with flag", e);
                ScanResult {
                    is_clean: true,
                    sanitized_text: text.to_string(),
                    verdict: "TG_UNAVAILABLE".to_string(),
                    tg_unavailable: true,
                }
            }
            Ok(resp) => {
                match resp.json::<ScanResponse>().await {
                    Err(e) => {
                        warn!("TrollGuard bad response: {} — allowing turn", e);
                        ScanResult {
                            is_clean: true,
                            sanitized_text: text.to_string(),
                            verdict: "TG_PARSE_ERROR".to_string(),
                            tg_unavailable: true,
                        }
                    }
                    Ok(scan) => {
                        // SUSPICIOUS is allowed (flagged but not blocked); callers can inspect `verdict` to distinguish from SAFE.
                        let is_clean = scan.verdict == "SAFE" || scan.verdict == "SUSPICIOUS";
                        ScanResult {
                            is_clean,
                            sanitized_text: scan.sanitized_text,
                            verdict: scan.verdict,
                            tg_unavailable: false,
                        }
                    }
                }
            }
        }
    }
}
