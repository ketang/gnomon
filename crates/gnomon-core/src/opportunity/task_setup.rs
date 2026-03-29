use std::collections::BTreeSet;

use super::{
    OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
};

const MIN_SURFACE_SCORE: f64 = 0.5;
const HIGH_CONFIDENCE_SCORE: f64 = 1.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskSetupEvidenceKind {
    RepeatedOrientation,
    LongPreEditExploration,
    PlanningChurn,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskSetupSignal {
    pub kind: TaskSetupEvidenceKind,
    pub score: f64,
    pub detail: String,
}

impl TaskSetupSignal {
    pub fn new(kind: TaskSetupEvidenceKind, score: f64, detail: impl Into<String>) -> Self {
        Self {
            kind,
            score,
            detail: detail.into(),
        }
    }
}

pub fn detect_task_setup(signals: impl IntoIterator<Item = TaskSetupSignal>) -> OpportunitySummary {
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
        category: OpportunityCategory::TaskSetup,
        score: total_score,
        confidence,
        evidence,
        recommendation: Some(
            "reduce repeated exploration by front-loading task context or using targeted lookups"
                .to_string(),
        ),
    }])
}

fn signal_kind_label(kind: TaskSetupEvidenceKind) -> &'static str {
    match kind {
        TaskSetupEvidenceKind::RepeatedOrientation => "repeated orientation",
        TaskSetupEvidenceKind::LongPreEditExploration => "long pre-edit exploration",
        TaskSetupEvidenceKind::PlanningChurn => "planning churn",
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskSetupEvidenceKind, TaskSetupSignal, detect_task_setup};
    use crate::opportunity::{OpportunityCategory, OpportunityConfidence};

    #[test]
    fn suppresses_weak_task_setup_signals() {
        let summary = detect_task_setup(vec![
            TaskSetupSignal::new(
                TaskSetupEvidenceKind::RepeatedOrientation,
                0.2,
                "minor re-read of a single file",
            ),
            TaskSetupSignal::new(
                TaskSetupEvidenceKind::PlanningChurn,
                0.1,
                "brief planning pause",
            ),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.top_category, None);
        assert_eq!(summary.total_score, 0.0);
        assert_eq!(summary.top_confidence, None);
    }

    #[test]
    fn surfaces_single_strong_signal_as_medium_confidence() {
        let summary = detect_task_setup(vec![TaskSetupSignal::new(
            TaskSetupEvidenceKind::LongPreEditExploration,
            0.7,
            "extended exploration across multiple directories before first edit",
        )]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::TaskSetup));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::Medium));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score >= 0.7);
        assert!(
            summary.annotations[0]
                .evidence
                .iter()
                .any(|entry| entry.contains("long pre-edit exploration"))
        );
    }

    #[test]
    fn combines_multiple_kinds_into_high_confidence() {
        let summary = detect_task_setup(vec![
            TaskSetupSignal::new(
                TaskSetupEvidenceKind::RepeatedOrientation,
                0.6,
                "re-read the same config files three times before editing",
            ),
            TaskSetupSignal::new(
                TaskSetupEvidenceKind::LongPreEditExploration,
                0.55,
                "broad directory scan with no edits for many turns",
            ),
        ]);

        assert_eq!(summary.top_category, Some(OpportunityCategory::TaskSetup));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::High));
        assert_eq!(summary.annotations.len(), 1);
        assert!(summary.total_score > 1.0);
        assert!(
            summary.annotations[0]
                .recommendation
                .as_deref()
                .is_some_and(|text| text.contains("front-loading"))
        );
    }

    #[test]
    fn ignores_non_finite_and_empty_signals() {
        let summary = detect_task_setup(vec![
            TaskSetupSignal::new(TaskSetupEvidenceKind::RepeatedOrientation, f64::NAN, "nan"),
            TaskSetupSignal::new(TaskSetupEvidenceKind::LongPreEditExploration, 0.8, "   "),
            TaskSetupSignal::new(
                TaskSetupEvidenceKind::PlanningChurn,
                0.4,
                "one planning cycle",
            ),
        ]);

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.total_score, 0.0);
    }
}
