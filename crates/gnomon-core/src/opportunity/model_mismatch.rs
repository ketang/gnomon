use std::collections::BTreeSet;

use super::{
    OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
};

/// Minimum tokens for a single signal to be considered.
const MIN_SIGNAL_TOKENS: f64 = 500.0;

/// Minimum action count for a single signal to be considered.
const MIN_SIGNAL_ACTION_COUNT: u64 = 2;

/// Minimum combined score to emit an annotation.
const MIN_SCORE: f64 = 0.3;

/// Token volume at which the volume component saturates.
const VOLUME_SATURATION_TOKENS: f64 = 10_000.0;

/// Number of distinct routine families at which breadth saturates.
const BREADTH_SATURATION: f64 = 4.0;

/// Concentration threshold for high confidence.
const HIGH_CONCENTRATION: f64 = 0.7;

/// Volume component threshold for high confidence.
const HIGH_VOLUME_COMPONENT: f64 = 0.5;

/// Minimum distinct routine families for high confidence.
const HIGH_BREADTH_FAMILIES: usize = 2;

// --- Time-sensitive model tier classification ---
// Update these lists as model economics change.

const PREMIUM_SUBSTRINGS: &[&str] = &["opus", "sonnet"];

const ROUTINE_CATEGORIES: &[&str] = &[
    "project discovery",
    "local search/navigation",
    "version control",
];

/// Prefixes that indicate routine build/test categories.
const ROUTINE_CATEGORY_PREFIXES: &[&str] = &["build", "test"];

#[derive(Debug, Clone, PartialEq)]
pub struct ModelMismatchSignal {
    pub action_category: String,
    pub action_label: String,
    pub model_names: BTreeSet<String>,
    pub total_tokens: f64,
    pub action_count: u64,
}

fn is_premium_model(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    PREMIUM_SUBSTRINGS.iter().any(|sub| lower.contains(sub))
}

fn has_premium_model(models: &BTreeSet<String>) -> bool {
    models.iter().any(|m| is_premium_model(m))
}

fn is_routine_category(category: &str) -> bool {
    let lower = category.to_ascii_lowercase();
    if ROUTINE_CATEGORIES.iter().any(|c| lower == *c) {
        return true;
    }
    ROUTINE_CATEGORY_PREFIXES
        .iter()
        .any(|prefix| lower.starts_with(prefix))
}

/// Detect premium-model concentration on routine actions.
///
/// The **mechanism** (detecting model/action mismatches) is stable. The
/// **recommendation layer** (which models count as premium and which actions
/// count as routine) is time-sensitive — update the constants at the top of
/// this module as model pricing and harness behavior evolve.
pub fn detect_model_mismatch(signals: &[ModelMismatchSignal]) -> OpportunitySummary {
    let mut premium_routine_tokens: f64 = 0.0;
    let mut total_routine_tokens: f64 = 0.0;
    let mut affected_families = BTreeSet::<String>::new();
    let mut evidence_entries: Vec<String> = Vec::new();

    for signal in signals {
        if !signal.total_tokens.is_finite()
            || signal.total_tokens < MIN_SIGNAL_TOKENS
            || signal.action_count < MIN_SIGNAL_ACTION_COUNT
        {
            continue;
        }

        if !is_routine_category(&signal.action_category) {
            continue;
        }

        total_routine_tokens += signal.total_tokens;

        if !has_premium_model(&signal.model_names) {
            continue;
        }

        premium_routine_tokens += signal.total_tokens;
        affected_families.insert(signal.action_category.clone());

        let models: Vec<&str> = signal
            .model_names
            .iter()
            .filter(|m| is_premium_model(m))
            .map(String::as_str)
            .collect();
        evidence_entries.push(format!(
            "{} used {} on {} ({:.0} tokens across {} actions)",
            signal.action_label,
            models.join(", "),
            signal.action_category,
            signal.total_tokens,
            signal.action_count,
        ));
    }

    if premium_routine_tokens == 0.0 {
        return OpportunitySummary::default();
    }

    let concentration = if total_routine_tokens > 0.0 {
        (premium_routine_tokens / total_routine_tokens).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let volume = (premium_routine_tokens / VOLUME_SATURATION_TOKENS).clamp(0.0, 1.0);
    let breadth = (affected_families.len() as f64 / BREADTH_SATURATION).clamp(0.0, 1.0);

    let score = (concentration * 0.5) + (volume * 0.3) + (breadth * 0.2);

    if score < MIN_SCORE {
        return OpportunitySummary::default();
    }

    let confidence = if concentration >= HIGH_CONCENTRATION
        && volume >= HIGH_VOLUME_COMPONENT
        && affected_families.len() >= HIGH_BREADTH_FAMILIES
    {
        OpportunityConfidence::High
    } else {
        OpportunityConfidence::Medium
    };

    let mut evidence = vec![
        format!(
            "premium models consumed {:.0} of {:.0} routine-action tokens ({:.0}%)",
            premium_routine_tokens,
            total_routine_tokens,
            concentration * 100.0,
        ),
        format!(
            "{} routine action families affected: {}",
            affected_families.len(),
            affected_families
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
        ),
    ];
    evidence.extend(evidence_entries);

    OpportunitySummary::from_annotations(vec![OpportunityAnnotation {
        category: OpportunityCategory::ModelMismatch,
        score,
        confidence,
        evidence,
        recommendation: Some(
            "route routine actions (reads, searches, test runs) to a cheaper model; \
             which models qualify as routine-capable is time-sensitive — \
             review model tier assignments periodically"
                .to_string(),
        ),
    }])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signal(
        category: &str,
        label: &str,
        models: &[&str],
        tokens: f64,
        count: u64,
    ) -> ModelMismatchSignal {
        ModelMismatchSignal {
            action_category: category.to_string(),
            action_label: label.to_string(),
            model_names: models.iter().map(|m| m.to_string()).collect(),
            total_tokens: tokens,
            action_count: count,
        }
    }

    #[test]
    fn suppresses_when_no_premium_models() {
        let summary = detect_model_mismatch(&[
            signal(
                "project discovery",
                "file read",
                &["claude-haiku"],
                2000.0,
                5,
            ),
            signal(
                "local search/navigation",
                "content search",
                &["claude-haiku"],
                1500.0,
                3,
            ),
        ]);
        assert!(summary.is_empty());
    }

    #[test]
    fn suppresses_low_volume_premium_usage() {
        let summary = detect_model_mismatch(&[signal(
            "project discovery",
            "file read",
            &["claude-opus-4"],
            200.0,
            1,
        )]);
        assert!(summary.is_empty());
    }

    #[test]
    fn ignores_premium_on_non_routine_actions() {
        let summary = detect_model_mismatch(&[
            signal("editing", "file edit", &["claude-opus-4"], 5000.0, 10),
            signal("coding", "implement feature", &["claude-opus-4"], 8000.0, 4),
        ]);
        assert!(summary.is_empty());
    }

    #[test]
    fn detects_medium_confidence_mismatch() {
        let summary = detect_model_mismatch(&[signal(
            "project discovery",
            "file read",
            &["claude-opus-4"],
            3000.0,
            8,
        )]);

        assert!(!summary.is_empty());
        let annotation = &summary.annotations[0];
        assert_eq!(annotation.category, OpportunityCategory::ModelMismatch);
        assert_eq!(annotation.confidence, OpportunityConfidence::Medium);
        assert!(annotation.score >= MIN_SCORE);
        assert!(annotation.evidence.len() >= 3);
        assert!(annotation.recommendation.is_some());
    }

    #[test]
    fn detects_high_confidence_mismatch() {
        let summary = detect_model_mismatch(&[
            signal(
                "project discovery",
                "file read",
                &["claude-opus-4"],
                6000.0,
                15,
            ),
            signal(
                "local search/navigation",
                "content search",
                &["claude-opus-4"],
                5000.0,
                12,
            ),
            signal(
                "version control",
                "git status",
                &["claude-opus-4"],
                2000.0,
                8,
            ),
        ]);

        assert!(!summary.is_empty());
        let annotation = &summary.annotations[0];
        assert_eq!(annotation.category, OpportunityCategory::ModelMismatch);
        assert_eq!(annotation.confidence, OpportunityConfidence::High);
        assert!(annotation.score >= 0.5);
    }

    #[test]
    fn handles_mixed_models_in_single_action() {
        let summary = detect_model_mismatch(&[signal(
            "project discovery",
            "file read",
            &["claude-opus-4", "claude-haiku"],
            4000.0,
            10,
        )]);

        assert!(!summary.is_empty());
        let annotation = &summary.annotations[0];
        assert_eq!(annotation.category, OpportunityCategory::ModelMismatch);
        // Evidence should mention opus specifically
        assert!(
            annotation
                .evidence
                .iter()
                .any(|e| e.contains("claude-opus-4"))
        );
    }

    #[test]
    fn filters_non_finite_and_tiny_signals() {
        let summary = detect_model_mismatch(&[
            signal(
                "project discovery",
                "file read",
                &["claude-opus-4"],
                f64::NAN,
                5,
            ),
            signal(
                "project discovery",
                "file read",
                &["claude-opus-4"],
                f64::INFINITY,
                5,
            ),
            signal(
                "local search/navigation",
                "file glob",
                &["claude-opus-4"],
                100.0,
                1,
            ),
        ]);
        assert!(summary.is_empty());
    }

    #[test]
    fn routine_category_prefix_matching() {
        // "build/test" style categories should match via prefix
        let summary = detect_model_mismatch(&[signal(
            "build (cargo)",
            "cargo build",
            &["claude-sonnet-4"],
            3000.0,
            5,
        )]);

        assert!(!summary.is_empty());
        let annotation = &summary.annotations[0];
        assert_eq!(annotation.category, OpportunityCategory::ModelMismatch);
    }
}
