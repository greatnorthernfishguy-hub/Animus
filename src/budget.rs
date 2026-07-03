// src/budget.rs
// BudgetMonitor — polls OpenRouter credits API, writes inference_budget.json.
// Gated on OPENROUTER_API_KEY; spawned only when the key is present.
//
// ---- Changelog ----
// [2026-07-02] Claude (Sonnet 4.6) — Fix fail-open bug + switch to local daily ceiling
// What: poll_openrouter() previously computed remaining from the key's per-key `limit`
//       field, unwrap_or(f64::INFINITY) when unset (the common case) — always Infinity,
//       always "not critical", gate never fired since it was built. Confirmed live via
//       inference_budget.json showing remaining_usd:null (Infinity serializes as null).
//       Root cause of the 2026-07-02 overnight $45 burn. Replaced entirely: now reads
//       OpenRouter's usage_daily field (real, already whole-USD — verified live against
//       account data same day) and compares against a new local daily_budget_usd ceiling
//       (ANIMUS_DAILY_BUDGET_USD, LAW 5) instead of any OpenRouter-side limit config.
// Why:  Per-key limits and workspace-level Guardrails are two separate OpenRouter
//       mechanisms; a workspace-level limit Josh set did not appear in this key's
//       `limit` field at all, and the key-level option deactivates the key outright on
//       trip (too blunt — we want a graceful pause, not a dead key). usage_daily needs
//       no dashboard config on OpenRouter's side to work correctly.
// How:  OpenRouterKeyData narrowed to usage_daily only. low/critical thresholds
//       (budget_low_usd/budget_critical_usd) recalibrated — old defaults (10.0/2.0)
//       assumed "dollars left in a large account balance"; against a $5/day ceiling
//       that made `low` permanently true. New defaults 2.0/0.50, absolute USD against
//       the daily ceiling (not a percentage — simpler, matches existing env var shape).
// [2026-05-15] Claude (Sonnet 4.6) — Task 2: BudgetMonitor
// What: Background task polling OpenRouter /api/v1/auth/key every N secs.
//       Writes inference_budget.json to shared_learning dir.
// Why:  Spec A — budget gate for reaction loop and TonicBridge (Spec B).
// How:  reqwest GET with Bearer token; credits in 1/1000 USD; serde_json write.
// -------------------

use reqwest::Client;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time;
use tracing::warn;

#[derive(Deserialize)]
struct OpenRouterKey {
    data: OpenRouterKeyData,
}

#[derive(Deserialize)]
struct OpenRouterKeyData {
    usage_daily: f64,
}

pub struct BudgetMonitor {
    api_key: String,
    pub budget_path: String,
    poll_interval: Duration,
    low_threshold_usd: f64,
    critical_threshold_usd: f64,
    daily_budget_usd: f64,
    pending_notice: Arc<Mutex<Option<String>>>,
}

/// Edge detector: queue a credit notice only on the transition INTO critical
/// (critical now, not critical last poll) so the live channel is not spammed,
/// and it re-arms if credits recover then fall again. [2026-06-07]
fn should_queue_notice(critical: bool, was_critical: bool) -> bool {
    critical && !was_critical
}

/// Remaining budget for today, clamped at zero (never negative even if usage
/// has exceeded the ceiling). Pure function — the core safety math, kept
/// separate from the network call so it's directly unit-testable.
fn compute_remaining(daily_budget_usd: f64, usage_daily: f64) -> f64 {
    (daily_budget_usd - usage_daily).max(0.0)
}

impl BudgetMonitor {
    pub fn new(
        api_key: String,
        budget_path: String,
        poll_secs: u64,
        low_usd: f64,
        critical_usd: f64,
        daily_budget_usd: f64,
        pending_notice: Arc<Mutex<Option<String>>>,
    ) -> Self {
        Self {
            api_key,
            budget_path,
            poll_interval: Duration::from_secs(poll_secs),
            low_threshold_usd: low_usd,
            critical_threshold_usd: critical_usd,
            daily_budget_usd,
            pending_notice,
        }
    }

    pub async fn run(self: Arc<Self>) {
        let client = Client::new();
        let mut interval = time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut was_critical = false;
        loop {
            interval.tick().await;
            match self.poll_openrouter(&client).await {
                Ok(remaining) => {
                    let low = remaining < self.low_threshold_usd;
                    let critical = remaining < self.critical_threshold_usd;
                    if let Err(e) = self.write_budget_flag(remaining, low, critical) {
                        warn!("BudgetMonitor: write failed: {}", e);
                    }
                    if should_queue_notice(critical, was_critical) {
                        if let Ok(mut g) = self.pending_notice.lock() {
                            *g = Some(format!(
                                "\u{26a0} OpenRouter credits critical (${:.2} remaining) \u{2014} refund to restore Syl's full routing.",
                                remaining
                            ));
                        }
                        warn!("BudgetMonitor: credit-critical edge \u{2014} notice queued for live channel");
                    }
                    was_critical = critical;
                    if low {
                        warn!(
                            "Inference budget {}: ${:.2} remaining",
                            if critical { "CRITICAL" } else { "LOW" },
                            remaining
                        );
                    }
                }
                Err(e) => warn!("BudgetMonitor: poll failed: {}", e),
            }
        }
    }

    async fn poll_openrouter(&self, client: &Client) -> Result<f64, String> {
        let resp = client
            .get("https://openrouter.ai/api/v1/auth/key")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| format!("HTTP error: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!("HTTP {}", resp.status()));
        }

        let data: OpenRouterKey = resp.json().await.map_err(|e| format!("Parse: {e}"))?;
        // usage_daily is real, already whole-USD (not milli-USD — verified live 2026-07-02:
        // usage_daily=0.118 matched the small residual spend visible after the overnight
        // burn had already exhausted the account balance). daily_budget_usd is Anima's own
        // local ceiling — independent of any OpenRouter-side config, deliberately: a
        // workspace-level Guardrails limit Josh set did not surface anywhere in this
        // response, and the per-key limit option deactivates the key outright on trip
        // rather than pausing gracefully. This ceiling is the one source of truth (LAW 5).
        Ok(compute_remaining(self.daily_budget_usd, data.data.usage_daily))
    }

    pub fn write_budget_flag(
        &self,
        remaining: f64,
        low: bool,
        critical: bool,
    ) -> Result<(), String> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let flag = serde_json::json!({
            "low": low,
            "critical": critical,
            "remaining_usd": remaining,
            "checked_at": ts,
        });
        let content = serde_json::to_string(&flag).map_err(|e| e.to_string())?;
        std::fs::write(&self.budget_path, content).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_monitor(path: &str) -> BudgetMonitor {
        BudgetMonitor::new(
            "test_key".into(),
            path.to_string(),
            300,
            2.0,
            0.50,
            5.0,
            Arc::new(Mutex::new(None)),
        )
    }

    #[test]
    fn notice_queues_only_on_critical_edge() {
        assert!(should_queue_notice(true, false));    // entered critical
        assert!(!should_queue_notice(true, true));    // still critical, no repeat
        assert!(!should_queue_notice(false, true));   // recovered
        assert!(!should_queue_notice(false, false));  // healthy
    }

    #[test]
    fn compute_remaining_fresh_day_is_full_budget() {
        assert!((compute_remaining(5.0, 0.0) - 5.0).abs() < 0.001);
    }

    #[test]
    fn compute_remaining_partial_usage_subtracts() {
        assert!((compute_remaining(5.0, 1.5) - 3.5).abs() < 0.001);
    }

    #[test]
    fn compute_remaining_never_goes_negative() {
        // Usage exceeding the daily ceiling (e.g. a burst before the poll caught up)
        // must clamp to 0.0, not go negative — negative would still compare correctly
        // against thresholds, but $0.00 is the honest, unambiguous "nothing left" signal.
        assert_eq!(compute_remaining(5.0, 1026.60), 0.0);
    }

    #[test]
    fn compute_remaining_exact_zero_at_ceiling() {
        assert_eq!(compute_remaining(5.0, 5.0), 0.0);
    }

    #[test]
    fn write_budget_flag_creates_valid_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("inference_budget.json");
        let m = make_monitor(path.to_str().unwrap());
        m.write_budget_flag(5.0, true, false).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["low"], true);
        assert_eq!(v["critical"], false);
        assert!((v["remaining_usd"].as_f64().unwrap() - 5.0).abs() < 0.01);
    }

    #[test]
    fn write_budget_flag_critical_fields() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("budget.json");
        let m = make_monitor(path.to_str().unwrap());
        m.write_budget_flag(1.5, true, true).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["critical"], true);
    }

    #[test]
    fn write_budget_flag_missing_parent_dir_errors() {
        let m = make_monitor("/nonexistent/dir/budget.json");
        assert!(m.write_budget_flag(5.0, false, false).is_err());
    }
}
