use std::collections::BTreeSet;

use super::{
    OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
};

const MIN_SURFACE_SCORE: f64 = 0.5;
const HIGH_CONFIDENCE_SCORE: f64 = 1.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SearchChurnEvidenceKind {
    RepeatedSearch,
    RepeatedRead,
    RepeatedGitInspection,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchChurnSignal {
    pub kind: SearchChurnEvidenceKind,
    pub score: f64,
    pub detail: String,
}

impl SearchChurnSignal {
    pub fn new(kind: SearchChurnEvidenceKind, score: f64, detail: impl Into<String>) -> Self {
        Self {
            kind,
            score,
            detail: detail.into(),
        }
    }
}

pub fn detect_search_churn(
    signals: impl IntoIterator<Item = SearchChurnSignal>,
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
        category: OpportunityCategory::SearchChurn,
        score: total_score,
        confidence,
        evidence,
        recommendation: Some(
            "reduce repeated search and navigation by using targeted queries or caching earlier results"
                .to_string(),
        ),
    }])
}

fn signal_kind_label(kind: SearchChurnEvidenceKind) -> &'static str {
    match kind {
        SearchChurnEvidenceKind::RepeatedSearch => "repeated search",
        SearchChurnEvidenceKind::RepeatedRead => "repeated read",
        SearchChurnEvidenceKind::RepeatedGitInspection => "repeated git inspection",
    }
}

#[cfg(test)]
mod tests {
    use super::{SearchChurnEvidenceKind, SearchChurnSignal, detect_search_churn};
    use crate::opportunity::{OpportunityCategory, OpportunityConfidence};

    #[test]
    fn suppresses_weak_search_churn_signals() {
        let summary = detect_search_churn(vec![
            SearchChurnSignal::new(
                SearchChurnEvidenceKind::RepeatedSearch,
                0.2,
                "minor repeated grep",
            ),
            SearchChurnSignal::new(
                SearchChurnEvidenceKind::RepeatedRead,
                0.1,
                "re-read a single file",
            ),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.top_category, None);
        assert_eq!(summary.total_score, 0.0);
        assert_eq!(summary.top_confidence, None);
    }

    #[test]
    fn surfaces_a_single_strong_signal_as_medium_confidence() {
        let summary = detect_search_churn(vec![SearchChurnSignal::new(
            SearchChurnEvidenceKind::RepeatedSearch,
            0.7,
            "grep for the same pattern 5 times across turns",
        )]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::SearchChurn));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::Medium));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score >= 0.7);
        assert!(
            summary.annotations[0]
                .evidence
                .iter()
                .any(|entry| entry.contains("repeated search"))
        );
    }

    #[test]
    fn combines_multiple_kinds_into_high_confidence() {
        let summary = detect_search_churn(vec![
            SearchChurnSignal::new(
                SearchChurnEvidenceKind::RepeatedSearch,
                0.6,
                "grep for similar patterns across multiple turns",
            ),
            SearchChurnSignal::new(
                SearchChurnEvidenceKind::RepeatedGitInspection,
                0.55,
                "repeated git log and git diff on the same range",
            ),
        ]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::SearchChurn));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::High));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score > 1.0);
        assert!(
            summary.annotations[0]
                .recommendation
                .as_deref()
                .is_some_and(|text| text.contains("reduce repeated search"))
        );
    }

    #[test]
    fn ignores_non_finite_and_empty_signals() {
        let summary = detect_search_churn(vec![
            SearchChurnSignal::new(SearchChurnEvidenceKind::RepeatedRead, f64::NAN, "nan score"),
            SearchChurnSignal::new(SearchChurnEvidenceKind::RepeatedSearch, 0.8, "   "),
            SearchChurnSignal::new(
                SearchChurnEvidenceKind::RepeatedGitInspection,
                0.4,
                "git status loop",
            ),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.total_score, 0.0);
    }
}
