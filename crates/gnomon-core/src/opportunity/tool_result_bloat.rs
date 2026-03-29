use super::{OpportunityAnnotation, OpportunityCategory, OpportunityConfidence};

const MIN_RESULT_COUNT: usize = 3;
const MIN_TOTAL_OUTPUT_BYTES: usize = 16 * 1024;
const HIGH_TOTAL_OUTPUT_BYTES: usize = 128 * 1024;
const MIN_AFFECTED_TOKENS: f64 = 1_500.0;
const HIGH_AFFECTED_TOKENS: f64 = 6_000.0;
const MIN_SCORE: f64 = 0.35;

#[derive(Debug, Clone, PartialEq)]
pub struct ToolResultBloatInput {
    pub tool_name: String,
    pub invocation: Option<String>,
    pub total_output_bytes: usize,
    pub result_count: usize,
    pub estimated_affected_tokens: f64,
}

pub fn detect_tool_result_bloat(input: &ToolResultBloatInput) -> Option<OpportunityAnnotation> {
    if input.result_count < MIN_RESULT_COUNT
        || input.total_output_bytes < MIN_TOTAL_OUTPUT_BYTES
        || input.estimated_affected_tokens < MIN_AFFECTED_TOKENS
    {
        return None;
    }

    let byte_pressure =
        (input.total_output_bytes as f64 / HIGH_TOTAL_OUTPUT_BYTES as f64).clamp(0.0, 1.0);
    let token_pressure = (input.estimated_affected_tokens / HIGH_AFFECTED_TOKENS).clamp(0.0, 1.0);
    let repetition_pressure =
        (input.result_count as f64 / (MIN_RESULT_COUNT as f64 * 2.0)).clamp(0.0, 1.0);
    let score = (byte_pressure * 0.45) + (token_pressure * 0.4) + (repetition_pressure * 0.15);

    if score < MIN_SCORE {
        return None;
    }

    let confidence = if input.total_output_bytes >= HIGH_TOTAL_OUTPUT_BYTES
        && input.estimated_affected_tokens >= HIGH_AFFECTED_TOKENS
    {
        OpportunityConfidence::High
    } else {
        OpportunityConfidence::Medium
    };

    let mut evidence = vec![
        format!("tool {}", input.tool_name),
        format!("result count {}", input.result_count),
        format!("total output bytes {}", input.total_output_bytes),
        format!(
            "estimated affected tokens {:.0}",
            input.estimated_affected_tokens
        ),
    ];
    if let Some(invocation) = input.invocation.as_deref() {
        evidence.push(format!("representative invocation {invocation}"));
    }

    Some(OpportunityAnnotation {
        category: OpportunityCategory::ToolResultBloat,
        score,
        confidence,
        evidence,
        recommendation: Some(
            "reduce bulky tool output with tighter selectors, limits, or narrower invocations"
                .to_string(),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::{ToolResultBloatInput, detect_tool_result_bloat};
    use crate::opportunity::{OpportunityCategory, OpportunityConfidence};

    fn sample_input() -> ToolResultBloatInput {
        ToolResultBloatInput {
            tool_name: "shell".to_string(),
            invocation: Some("rg -n TODO src".to_string()),
            total_output_bytes: 48 * 1024,
            result_count: 4,
            estimated_affected_tokens: 3_000.0,
        }
    }

    #[test]
    fn suppresses_small_or_rare_tool_results() {
        let annotation = detect_tool_result_bloat(&ToolResultBloatInput {
            tool_name: "shell".to_string(),
            invocation: Some("ls".to_string()),
            total_output_bytes: 2 * 1024,
            result_count: 1,
            estimated_affected_tokens: 120.0,
        });

        assert_eq!(annotation, None);
    }

    #[test]
    fn emits_medium_confidence_for_moderate_bloat() {
        let annotation = detect_tool_result_bloat(&sample_input())
            .expect("moderate repeated tool output should be flagged");

        assert_eq!(annotation.category, OpportunityCategory::ToolResultBloat);
        assert_eq!(annotation.confidence, OpportunityConfidence::Medium);
        assert!(annotation.score >= 0.35);
        assert!(
            annotation
                .evidence
                .iter()
                .any(|entry| entry.contains("representative invocation rg -n TODO src"))
        );
    }

    #[test]
    fn emits_high_confidence_for_large_repeated_bloat() {
        let annotation = detect_tool_result_bloat(&ToolResultBloatInput {
            total_output_bytes: 256 * 1024,
            result_count: 6,
            estimated_affected_tokens: 9_000.0,
            ..sample_input()
        })
        .expect("large repeated tool output should be flagged");

        assert_eq!(annotation.confidence, OpportunityConfidence::High);
        assert!(annotation.score <= 1.0);
        assert!(
            annotation
                .evidence
                .iter()
                .any(|entry| entry.contains("estimated affected tokens 9000"))
        );
    }
}
