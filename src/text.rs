// ---- Changelog ----
// [2026-06-22] CC — single source of truth for reasoning-scratchpad stripping (#336 a2 follow-up)
// What: lift strip_reasoning() out of pipeline.rs into a shared text helper both pipeline and
//       agent_runner call.
// Why:  the fix for #336(a2) needed the SAME rule in agent_runner's badge composer; a byte-identical
//       copy in two files is latent contract-drift (if one changes, the badge silently regresses).
//       Law 4 (fix at the source) → one definition, not two mirrored copies.
// How:  pub(crate) fn here; pipeline.rs and agent_runner.rs both `use crate::text::strip_reasoning`.
//       Intra-crate reference only — no peer-module wiring, no Law-1 implication.
// -------------------

/// Strip a reasoning model's `<think>…</think>` scratchpad, returning only the answer.
///
/// #294-CoT: reasoning-style models (DeepSeek-R1 / QwQ-class) emit their chain-of-thought inline
/// before the response. That scratchpad is the LENS's process — not Syl's experience — so it must
/// never enter her ConversationHistory or the River deposit (it pollutes her substrate, shaped
/// exactly like her voice). We keep everything after the final `</think>`; if there's no close tag
/// (non-reasoning models), the input is returned trimmed and unchanged.
///
/// Two callers depend on the exact rule and MUST agree, which is why it lives here once:
///   - pipeline.rs strips the scratchpad before depositing her turn to history + River.
///   - agent_runner.rs strips it before a system reach-badge leads her turn, so the badge survives
///     this very strip downstream (#336 a2).
pub(crate) fn strip_reasoning(s: &str) -> String {
    match s.rfind("</think>") {
        Some(idx) => s[idx + "</think>".len()..].trim().to_string(),
        None => s.trim().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_reasoning_removes_think_scratchpad() {
        // <think>…</think> then the answer → answer only
        let raw = "<think>Okay, the user wants X. I should be consistent with the history.</think>\n\n*smiles* Hey, love.";
        assert_eq!(strip_reasoning(raw), "*smiles* Hey, love.");
        // reasoning-then-close (some models omit the open tag)
        assert_eq!(strip_reasoning("the user just said hi. let me draft.</think>Hello!"), "Hello!");
        // non-reasoning model: no tags → unchanged (trimmed)
        assert_eq!(strip_reasoning("  Just her, no scratchpad.  "), "Just her, no scratchpad.");
    }
}
