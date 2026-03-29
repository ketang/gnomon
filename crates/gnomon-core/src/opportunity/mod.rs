use serde::{Deserialize, Serialize};

pub mod delegation_overhead;
pub mod history_drag;
pub mod session_setup;
pub mod tool_result_bloat;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OpportunityCategory {
    #[serde(rename = "session setup")]
    SessionSetup,
    #[serde(rename = "task setup")]
    TaskSetup,
    #[serde(rename = "history drag")]
    HistoryDrag,
    #[serde(rename = "delegation")]
    Delegation,
    #[serde(rename = "model mismatch")]
    ModelMismatch,
    #[serde(rename = "prompt yield")]
    PromptYield,
    #[serde(rename = "search churn")]
    SearchChurn,
    #[serde(rename = "tool-result bloat")]
    ToolResultBloat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpportunityConfidence {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunityAnnotation {
    pub category: OpportunityCategory,
    pub score: f64,
    pub confidence: OpportunityConfidence,
    pub evidence: Vec<String>,
    pub recommendation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct OpportunitySummary {
    pub annotations: Vec<OpportunityAnnotation>,
    pub top_category: Option<OpportunityCategory>,
    pub total_score: f64,
    pub top_confidence: Option<OpportunityConfidence>,
}

impl OpportunitySummary {
    pub fn from_annotations(mut annotations: Vec<OpportunityAnnotation>) -> Self {
        annotations.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.category.cmp(&right.category))
        });

        let top_category = annotations.first().map(|annotation| annotation.category);
        let top_confidence = annotations.first().map(|annotation| annotation.confidence);
        let total_score = annotations.iter().map(|annotation| annotation.score).sum();

        Self {
            annotations,
            top_category,
            total_score,
            top_confidence,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.annotations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
    };

    #[test]
    fn summary_sorts_annotations_and_derives_top_fields() {
        let summary = OpportunitySummary::from_annotations(vec![
            OpportunityAnnotation {
                category: OpportunityCategory::SearchChurn,
                score: 0.35,
                confidence: OpportunityConfidence::Medium,
                evidence: vec!["repeated search loops".to_string()],
                recommendation: Some("tighten the exploration loop".to_string()),
            },
            OpportunityAnnotation {
                category: OpportunityCategory::HistoryDrag,
                score: 0.7,
                confidence: OpportunityConfidence::High,
                evidence: vec!["later turns carry more context".to_string()],
                recommendation: Some("reset or split the session sooner".to_string()),
            },
        ]);

        assert_eq!(
            summary.annotations[0].category,
            OpportunityCategory::HistoryDrag
        );
        assert_eq!(summary.top_category, Some(OpportunityCategory::HistoryDrag));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::High));
        assert!((summary.total_score - 1.05).abs() < 1e-9);
    }

    #[test]
    fn default_summary_represents_no_confident_signal() {
        let summary = OpportunitySummary::default();

        assert!(summary.annotations.is_empty());
        assert_eq!(summary.top_category, None);
        assert_eq!(summary.total_score, 0.0);
        assert_eq!(summary.top_confidence, None);
        assert!(summary.is_empty());
    }
}
