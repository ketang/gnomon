use std::collections::BTreeSet;

use super::{
    OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
};

const MIN_SURFACE_SCORE: f64 = 0.5;
const HIGH_CONFIDENCE_SCORE: f64 = 1.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SessionSetupEvidenceKind {
    MemoryBootstrap,
    RepoOrientation,
    StartupDelay,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionSetupSignal {
    pub kind: SessionSetupEvidenceKind,
    pub score: f64,
    pub detail: String,
}

impl SessionSetupSignal {
    pub fn new(kind: SessionSetupEvidenceKind, score: f64, detail: impl Into<String>) -> Self {
        Self {
            kind,
            score,
            detail: detail.into(),
        }
    }
}

pub fn detect_session_setup(
    signals: impl IntoIterator<Item = SessionSetupSignal>,
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

    let confidence = if total_score >= HIGH_CONFIDENCE_SCORE
        && kinds.contains(&SessionSetupEvidenceKind::MemoryBootstrap)
        && kinds.contains(&SessionSetupEvidenceKind::RepoOrientation)
    {
        OpportunityConfidence::High
    } else {
        OpportunityConfidence::Medium
    };

    OpportunitySummary::from_annotations(vec![OpportunityAnnotation {
        category: OpportunityCategory::SessionSetup,
        score: total_score,
        confidence,
        evidence,
        recommendation: Some(
            "move bootstrap and early orientation work out of the task path".to_string(),
        ),
    }])
}

fn signal_kind_label(kind: SessionSetupEvidenceKind) -> &'static str {
    match kind {
        SessionSetupEvidenceKind::MemoryBootstrap => "memory bootstrap",
        SessionSetupEvidenceKind::RepoOrientation => "repo orientation",
        SessionSetupEvidenceKind::StartupDelay => "startup delay",
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionSetupEvidenceKind, SessionSetupSignal, detect_session_setup};
    use crate::opportunity::{OpportunityCategory, OpportunityConfidence};

    #[test]
    fn suppresses_weak_session_setup_signals() {
        let summary = detect_session_setup(vec![
            SessionSetupSignal::new(
                SessionSetupEvidenceKind::MemoryBootstrap,
                0.2,
                "small bootstrap bump",
            ),
            SessionSetupSignal::new(
                SessionSetupEvidenceKind::StartupDelay,
                0.1,
                "brief startup lag",
            ),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.top_category, None);
        assert_eq!(summary.total_score, 0.0);
        assert_eq!(summary.top_confidence, None);
    }

    #[test]
    fn surfaces_a_single_strong_signal_as_medium_confidence() {
        let summary = detect_session_setup(vec![SessionSetupSignal::new(
            SessionSetupEvidenceKind::MemoryBootstrap,
            0.7,
            "bootstrap consumed a meaningful amount of context",
        )]);

        assert_eq!(
            summary.top_category,
            Some(OpportunityCategory::SessionSetup)
        );
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::Medium));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score >= 0.7);
        assert!(
            summary.annotations[0]
                .evidence
                .iter()
                .any(|entry| entry.contains("memory bootstrap"))
        );
    }

    #[test]
    fn combines_bootstrap_and_orientation_into_high_confidence() {
        let summary = detect_session_setup(vec![
            SessionSetupSignal::new(
                SessionSetupEvidenceKind::MemoryBootstrap,
                0.65,
                "memory bootstrap prompt and project context",
            ),
            SessionSetupSignal::new(
                SessionSetupEvidenceKind::RepoOrientation,
                0.55,
                "initial repo inspection before task work",
            ),
        ]);

        assert_eq!(
            summary.top_category,
            Some(OpportunityCategory::SessionSetup)
        );
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::High));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score > 1.0);
        assert!(
            summary.annotations[0]
                .recommendation
                .as_deref()
                .is_some_and(|text| text.contains("bootstrap"))
        );
    }

    #[test]
    fn ignores_non_finite_and_empty_signals() {
        let summary = detect_session_setup(vec![
            SessionSetupSignal::new(SessionSetupEvidenceKind::MemoryBootstrap, f64::NAN, "nan"),
            SessionSetupSignal::new(SessionSetupEvidenceKind::RepoOrientation, 0.8, "   "),
            SessionSetupSignal::new(SessionSetupEvidenceKind::StartupDelay, 0.4, "startup drag"),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.total_score, 0.0);
    }
}
