use serde::{Deserialize, Serialize};

use super::{
    OpportunityAnnotation, OpportunityCategory, OpportunityConfidence, OpportunitySummary,
};

pub const MIN_TURN_COUNT: usize = 4;
pub const MIN_TOTAL_INPUT: f64 = 120.0;
pub const MEDIUM_GROWTH_RATIO: f64 = 1.35;
pub const HIGH_GROWTH_RATIO: f64 = 1.8;
pub const UNCACHED_DOMINANCE_RATIO: f64 = 0.65;
pub const CACHED_DOMINANCE_RATIO: f64 = 0.35;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HistoryDragTurn {
    pub uncached_input: f64,
    pub cached_input: f64,
}

impl HistoryDragTurn {
    pub const fn new(uncached_input: f64, cached_input: f64) -> Self {
        Self {
            uncached_input,
            cached_input,
        }
    }

    pub fn total_input(self) -> f64 {
        clean_value(self.uncached_input) + clean_value(self.cached_input)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoryDragMix {
    MostlyUncached,
    Mixed,
    MostlyCached,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryDragDetection {
    pub annotation: OpportunityAnnotation,
    pub mix: HistoryDragMix,
    pub early_total_input: f64,
    pub late_total_input: f64,
    pub early_uncached_share: f64,
    pub late_uncached_share: f64,
}

impl HistoryDragDetection {
    pub fn summary(&self) -> OpportunitySummary {
        OpportunitySummary::from_annotations(vec![self.annotation.clone()])
    }
}

pub fn detect(turns: &[HistoryDragTurn]) -> Option<HistoryDragDetection> {
    if turns.len() < MIN_TURN_COUNT {
        return None;
    }

    let clean_turns = turns
        .iter()
        .map(|turn| HistoryDragTurn::new(turn.uncached_input, turn.cached_input))
        .collect::<Vec<_>>();
    let overall_input = total_input(&clean_turns);
    if overall_input < MIN_TOTAL_INPUT {
        return None;
    }

    let split_at = clean_turns.len() / 2;
    let (early_turns, late_turns) = clean_turns.split_at(split_at);
    let early_total_input = total_input(early_turns);
    let late_total_input = total_input(late_turns);
    if early_total_input <= 0.0 || late_total_input <= early_total_input {
        return None;
    }

    let growth_ratio = late_total_input / early_total_input;
    if growth_ratio < MEDIUM_GROWTH_RATIO {
        return None;
    }

    let early_uncached_share = uncached_share(early_turns);
    let late_uncached_share = uncached_share(late_turns);
    let mix = classify_mix(late_uncached_share);
    let confidence = classify_confidence(overall_input, growth_ratio);
    let score = score_detection(
        overall_input,
        growth_ratio,
        late_uncached_share,
        mix,
    );
    let annotation = OpportunityAnnotation {
        category: OpportunityCategory::HistoryDrag,
        score,
        confidence,
        evidence: evidence_lines(
            early_total_input,
            late_total_input,
            early_uncached_share,
            late_uncached_share,
            mix,
        ),
        recommendation: Some(
            "reduce late-turn carry-over by splitting the task or resetting context earlier"
                .to_string(),
        ),
    };

    Some(HistoryDragDetection {
        annotation,
        mix,
        early_total_input,
        late_total_input,
        early_uncached_share,
        late_uncached_share,
    })
}

pub fn detect_summary(turns: &[HistoryDragTurn]) -> OpportunitySummary {
    detect(turns)
        .map(|detection| detection.summary())
        .unwrap_or_default()
}

fn classify_mix(late_uncached_share: f64) -> HistoryDragMix {
    if late_uncached_share >= UNCACHED_DOMINANCE_RATIO {
        HistoryDragMix::MostlyUncached
    } else if late_uncached_share <= CACHED_DOMINANCE_RATIO {
        HistoryDragMix::MostlyCached
    } else {
        HistoryDragMix::Mixed
    }
}

fn classify_confidence(total_input: f64, growth_ratio: f64) -> OpportunityConfidence {
    if total_input >= 240.0 && growth_ratio >= HIGH_GROWTH_RATIO {
        OpportunityConfidence::High
    } else {
        OpportunityConfidence::Medium
    }
}

fn score_detection(
    total_input: f64,
    growth_ratio: f64,
    late_uncached_share: f64,
    mix: HistoryDragMix,
) -> f64 {
    let growth_component = ((growth_ratio - 1.0) / 1.5).clamp(0.0, 1.0);
    let mix_component = match mix {
        HistoryDragMix::MostlyUncached => late_uncached_share,
        HistoryDragMix::MostlyCached => 1.0 - late_uncached_share,
        HistoryDragMix::Mixed => 0.5,
    };
    let volume_component = (total_input / 480.0).clamp(0.0, 1.0);

    (0.5 * growth_component) + (0.3 * mix_component) + (0.2 * volume_component)
}

fn evidence_lines(
    early_total_input: f64,
    late_total_input: f64,
    early_uncached_share: f64,
    late_uncached_share: f64,
    mix: HistoryDragMix,
) -> Vec<String> {
    let mut lines = vec![
        format!(
            "late turns used {:.1} input vs {:.1} in early turns",
            late_total_input, early_total_input
        ),
        format!(
            "late turns were {:.0}% uncached vs {:.0}% uncached early",
            late_uncached_share * 100.0,
            early_uncached_share * 100.0
        ),
    ];

    lines.push(match mix {
        HistoryDragMix::MostlyUncached => {
            "history drag is mostly uncached input".to_string()
        }
        HistoryDragMix::MostlyCached => "history drag is mostly cached input".to_string(),
        HistoryDragMix::Mixed => "history drag is mixed across cached and uncached input".to_string(),
    });

    lines
}

fn total_input(turns: &[HistoryDragTurn]) -> f64 {
    turns.iter().map(|turn| turn.total_input()).sum()
}

fn uncached_share(turns: &[HistoryDragTurn]) -> f64 {
    let uncached_input: f64 = turns
        .iter()
        .map(|turn| clean_value(turn.uncached_input))
        .sum();
    let total_input = total_input(turns);

    if total_input <= 0.0 {
        0.0
    } else {
        uncached_input / total_input
    }
}

fn clean_value(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HistoryDragMix, HistoryDragTurn, detect, detect_summary, MEDIUM_GROWTH_RATIO,
    };
    use crate::opportunity::{OpportunityCategory, OpportunityConfidence};

    fn turn(uncached_input: f64, cached_input: f64) -> HistoryDragTurn {
        HistoryDragTurn::new(uncached_input, cached_input)
    }

    #[test]
    fn suppresses_short_series() {
        assert!(detect(&[turn(30.0, 5.0), turn(32.0, 6.0), turn(31.0, 7.0)]).is_none());
    }

    #[test]
    fn suppresses_flat_series_even_with_enough_volume() {
        let turns = [
            turn(30.0, 10.0),
            turn(28.0, 12.0),
            turn(29.0, 11.0),
            turn(31.0, 9.0),
        ];

        assert!(detect(&turns).is_none());
    }

    #[test]
    fn detects_uncached_history_drag() {
        let turns = [
            turn(18.0, 4.0),
            turn(20.0, 5.0),
            turn(85.0, 9.0),
            turn(92.0, 10.0),
        ];

        let detection = detect(&turns).expect("expected an uncached history-drag signal");
        assert_eq!(
            detection.annotation.category,
            OpportunityCategory::HistoryDrag
        );
        assert_eq!(detection.annotation.confidence, OpportunityConfidence::High);
        assert_eq!(detection.mix, HistoryDragMix::MostlyUncached);
        assert!(detection.late_uncached_share > 0.8);
        assert!(detection.annotation.score > 0.5);
        assert!(detection
            .annotation
            .evidence
            .iter()
            .any(|line| line.contains("mostly uncached input")));

        let summary = detect_summary(&turns);
        assert_eq!(summary.top_category, Some(OpportunityCategory::HistoryDrag));
        assert_eq!(summary.top_confidence, Some(OpportunityConfidence::High));
        assert_eq!(summary.annotations.len(), 1);
    }

    #[test]
    fn detects_cached_history_drag() {
        let turns = [
            turn(20.0, 18.0),
            turn(22.0, 20.0),
            turn(9.0, 88.0),
            turn(11.0, 95.0),
        ];

        let detection = detect(&turns).expect("expected a cached history-drag signal");
        assert_eq!(detection.mix, HistoryDragMix::MostlyCached);
        assert!(detection.late_uncached_share < 0.2);
        assert!(detection.annotation.score > 0.4);
        assert!(detection
            .annotation
            .evidence
            .iter()
            .any(|line| line.contains("mostly cached input")));
    }

    #[test]
    fn growth_threshold_is_respected() {
        let turns = [
            turn(40.0, 10.0),
            turn(40.0, 10.0),
            turn(54.0, 14.0),
            turn(54.0, 14.0),
        ];

        let detection = detect(&turns).expect("signal should clear the minimum growth threshold");
        let growth_ratio = detection.late_total_input / detection.early_total_input;
        assert!(growth_ratio >= MEDIUM_GROWTH_RATIO);
    }
}
