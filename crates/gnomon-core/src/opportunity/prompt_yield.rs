use std::collections::BTreeSet;

use super::{
    OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
};

const MIN_SURFACE_SCORE: f64 = 0.5;
const HIGH_CONFIDENCE_SCORE: f64 = 1.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PromptYieldEvidenceKind {
    HighInputLowEffect,
    PlanningLoop,
    LargePasteNoFollowThrough,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromptYieldSignal {
    pub kind: PromptYieldEvidenceKind,
    pub score: f64,
    pub detail: String,
}

impl PromptYieldSignal {
    pub fn new(kind: PromptYieldEvidenceKind, score: f64, detail: impl Into<String>) -> Self {
        Self {
            kind,
            score,
            detail: detail.into(),
        }
    }
}

pub fn detect_prompt_yield(
    signals: impl IntoIterator<Item = PromptYieldSignal>,
) -> OpportunitySummary {
    let mut total_score = 0.0;
    let mut evidence = Vec::new();
    let mut kinds = BTreeSet::new();

    for signal in signals {
        if !signal.score.is_finite() || signal.score <= 0.0 {
            continue;
        }
        if signal.detail.trim().is_empty() {
            continue;
        }

        total_score += signal.score;
        kinds.insert(signal.kind);
        evidence.push(format!(
            "{}: {}",
            signal_kind_label(signal.kind),
            signal.detail.trim()
        ));
    }

    if evidence.is_empty() || total_score < MIN_SURFACE_SCORE {
        return OpportunitySummary::default();
    }

    let confidence = if total_score >= HIGH_CONFIDENCE_SCORE && kinds.len() >= 2 {
        OpportunityConfidence::High
    } else {
        OpportunityConfidence::Medium
    };

    OpportunitySummary::from_annotations(vec![OpportunityAnnotation {
        category: OpportunityCategory::PromptYield,
        score: total_score,
        confidence,
        evidence,
        recommendation: Some(
            "tighten prompts to reduce input tokens that do not drive concrete actions".to_string(),
        ),
    }])
}

fn signal_kind_label(kind: PromptYieldEvidenceKind) -> &'static str {
    match kind {
        PromptYieldEvidenceKind::HighInputLowEffect => "high input low effect",
        PromptYieldEvidenceKind::PlanningLoop => "planning loop",
        PromptYieldEvidenceKind::LargePasteNoFollowThrough => "large paste no follow-through",
    }
}

#[cfg(test)]
mod tests {
    use super::{PromptYieldEvidenceKind, PromptYieldSignal, detect_prompt_yield};
    use crate::opportunity::{OpportunityCategory, OpportunityConfidence};

    #[test]
    fn suppresses_weak_prompt_yield_signals() {
        let summary = detect_prompt_yield(vec![
            PromptYieldSignal::new(
                PromptYieldEvidenceKind::HighInputLowEffect,
                0.2,
                "minor input overhead",
            ),
            PromptYieldSignal::new(
                PromptYieldEvidenceKind::PlanningLoop,
                0.1,
                "brief planning exchange",
            ),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.top_category, None);
        assert_eq!(summary.total_score, 0.0);
        assert_eq!(summary.top_confidence, None);
    }

    #[test]
    fn surfaces_a_single_strong_signal_as_medium_confidence() {
        let summary = detect_prompt_yield(vec![PromptYieldSignal::new(
            PromptYieldEvidenceKind::HighInputLowEffect,
            0.7,
            "large prompt with no downstream tool calls",
        )]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::PromptYield));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::Medium));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score >= 0.7);
        assert!(
            summary.annotations[0]
                .evidence
                .iter()
                .any(|entry| entry.contains("high input low effect"))
        );
    }

    #[test]
    fn combines_multiple_kinds_into_high_confidence() {
        let summary = detect_prompt_yield(vec![
            PromptYieldSignal::new(
                PromptYieldEvidenceKind::HighInputLowEffect,
                0.6,
                "high token input with weak observable output",
            ),
            PromptYieldSignal::new(
                PromptYieldEvidenceKind::LargePasteNoFollowThrough,
                0.55,
                "large pasted content with no concrete follow-up",
            ),
        ]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::PromptYield));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::High));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score > 1.0);
        assert!(
            summary.annotations[0]
                .recommendation
                .as_deref()
                .is_some_and(|text| text.contains("tighten prompts"))
        );
    }

    #[test]
    fn ignores_non_finite_and_empty_signals() {
        let summary = detect_prompt_yield(vec![
            PromptYieldSignal::new(PromptYieldEvidenceKind::PlanningLoop, f64::NAN, "nan score"),
            PromptYieldSignal::new(PromptYieldEvidenceKind::HighInputLowEffect, 0.8, "   "),
            PromptYieldSignal::new(
                PromptYieldEvidenceKind::LargePasteNoFollowThrough,
                0.4,
                "pasted block ignored",
            ),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.total_score, 0.0);
    }
}
