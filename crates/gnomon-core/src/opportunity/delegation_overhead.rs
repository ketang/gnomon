use std::collections::BTreeSet;

use super::{
    OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
};

const MIN_SURFACE_SCORE: f64 = 0.5;
const HIGH_CONFIDENCE_SCORE: f64 = 1.0;
const MIN_DISTINCT_KINDS_FOR_HIGH: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DelegationEvidenceKind {
    SmallTaskSidechain,
    RelayTraffic,
    CoordinationOverhead,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DelegationSignal {
    pub kind: DelegationEvidenceKind,
    pub score: f64,
    pub detail: String,
}

impl DelegationSignal {
    pub fn new(kind: DelegationEvidenceKind, score: f64, detail: impl Into<String>) -> Self {
        Self {
            kind,
            score,
            detail: detail.into(),
        }
    }
}

pub fn detect_delegation_overhead(
    signals: impl IntoIterator<Item = DelegationSignal>,
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
            evidence_kind_label(signal.kind),
            signal.detail.trim()
        ));
    }

    if evidence.is_empty() || total_score < MIN_SURFACE_SCORE {
        return OpportunitySummary::default();
    }

    let confidence =
        if total_score >= HIGH_CONFIDENCE_SCORE && kinds.len() >= MIN_DISTINCT_KINDS_FOR_HIGH {
            OpportunityConfidence::High
        } else {
            OpportunityConfidence::Medium
        };

    OpportunitySummary::from_annotations(vec![OpportunityAnnotation {
        category: OpportunityCategory::Delegation,
        score: total_score,
        confidence,
        evidence,
        recommendation: Some(
            "reduce delegation overhead by inlining small tasks and minimizing relay messages"
                .to_string(),
        ),
    }])
}

fn evidence_kind_label(kind: DelegationEvidenceKind) -> &'static str {
    match kind {
        DelegationEvidenceKind::SmallTaskSidechain => "small-task sidechain",
        DelegationEvidenceKind::RelayTraffic => "relay traffic",
        DelegationEvidenceKind::CoordinationOverhead => "coordination overhead",
    }
}

#[cfg(test)]
mod tests {
    use super::{DelegationEvidenceKind, DelegationSignal, detect_delegation_overhead};
    use crate::opportunity::{OpportunityCategory, OpportunityConfidence};

    #[test]
    fn suppresses_weak_delegation_signals() {
        let summary = detect_delegation_overhead(vec![
            DelegationSignal::new(
                DelegationEvidenceKind::RelayTraffic,
                0.2,
                "minor relay between agents",
            ),
            DelegationSignal::new(
                DelegationEvidenceKind::SmallTaskSidechain,
                0.1,
                "tiny sidechain task",
            ),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.top_category, None);
        assert_eq!(summary.total_score, 0.0);
        assert_eq!(summary.top_confidence, None);
    }

    #[test]
    fn surfaces_single_strong_signal_as_medium_confidence() {
        let summary = detect_delegation_overhead(vec![DelegationSignal::new(
            DelegationEvidenceKind::CoordinationOverhead,
            0.7,
            "high ratio of coordination messages to concrete edits",
        )]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::Delegation));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::Medium));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score >= 0.7);
        assert!(
            summary.annotations[0]
                .evidence
                .iter()
                .any(|entry| entry.contains("coordination overhead"))
        );
    }

    #[test]
    fn combines_multiple_kinds_into_high_confidence() {
        let summary = detect_delegation_overhead(vec![
            DelegationSignal::new(
                DelegationEvidenceKind::SmallTaskSidechain,
                0.6,
                "sub-agent spawned for a one-line change",
            ),
            DelegationSignal::new(
                DelegationEvidenceKind::RelayTraffic,
                0.55,
                "messages relayed without transformation",
            ),
        ]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::Delegation));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::High));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score > 1.0);
        assert!(
            summary.annotations[0]
                .recommendation
                .as_deref()
                .is_some_and(|text| text.contains("inlining"))
        );
    }

    #[test]
    fn ignores_non_finite_and_empty_signals() {
        let summary = detect_delegation_overhead(vec![
            DelegationSignal::new(DelegationEvidenceKind::RelayTraffic, f64::NAN, "nan"),
            DelegationSignal::new(DelegationEvidenceKind::SmallTaskSidechain, 0.8, "   "),
            DelegationSignal::new(
                DelegationEvidenceKind::CoordinationOverhead,
                0.4,
                "moderate coordination cost",
            ),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.total_score, 0.0);
    }

    #[test]
    fn single_kind_at_high_score_remains_medium_confidence() {
        let summary = detect_delegation_overhead(vec![
            DelegationSignal::new(
                DelegationEvidenceKind::RelayTraffic,
                0.6,
                "relay pass-through A",
            ),
            DelegationSignal::new(
                DelegationEvidenceKind::RelayTraffic,
                0.5,
                "relay pass-through B",
            ),
        ]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::Delegation));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::Medium));
        assert!(summary.total_score >= 1.0);
    }
}
