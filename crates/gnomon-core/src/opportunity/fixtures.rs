use std::collections::BTreeSet;

use super::delegation_overhead::{
    DelegationEvidenceKind, DelegationSignal, detect_delegation_overhead,
};
use super::history_drag::{self, HistoryDragMix, HistoryDragTurn};
use super::model_mismatch::{ModelMismatchSignal, detect_model_mismatch};
use super::prompt_yield::{PromptYieldEvidenceKind, PromptYieldSignal, detect_prompt_yield};
use super::search_churn::{SearchChurnEvidenceKind, SearchChurnSignal, detect_search_churn};
use super::session_setup::{SessionSetupEvidenceKind, SessionSetupSignal, detect_session_setup};
use super::task_setup::{TaskSetupEvidenceKind, TaskSetupSignal, detect_task_setup};
use super::tool_result_bloat::{ToolResultBloatInput, detect_tool_result_bloat};
use super::{OpportunityCategory, OpportunityConfidence};

// ---------------------------------------------------------------------------
// Session Setup
// ---------------------------------------------------------------------------

#[test]
fn session_setup_positive_high_confidence() {
    let summary = detect_session_setup(vec![
        SessionSetupSignal::new(
            SessionSetupEvidenceKind::MemoryBootstrap,
            0.65,
            "loaded 12 memory files",
        ),
        SessionSetupSignal::new(
            SessionSetupEvidenceKind::RepoOrientation,
            0.55,
            "read 8 top-level files before first edit",
        ),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.category, OpportunityCategory::SessionSetup);
    assert_eq!(ann.confidence, OpportunityConfidence::High);
    assert!((ann.score - 1.2).abs() < 0.01);
}

#[test]
fn session_setup_negative_below_threshold() {
    let summary = detect_session_setup(vec![SessionSetupSignal::new(
        SessionSetupEvidenceKind::StartupDelay,
        0.4,
        "minor delay",
    )]);

    assert!(summary.is_empty());
}

#[test]
fn session_setup_confidence_boundary_missing_kind() {
    // Score >= 1.0 but only one required kind → Medium
    let summary = detect_session_setup(vec![
        SessionSetupSignal::new(
            SessionSetupEvidenceKind::MemoryBootstrap,
            0.6,
            "loaded memory",
        ),
        SessionSetupSignal::new(SessionSetupEvidenceKind::StartupDelay, 0.5, "slow startup"),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.confidence, OpportunityConfidence::Medium);
    assert!(ann.score >= 1.0);
}

// ---------------------------------------------------------------------------
// Task Setup
// ---------------------------------------------------------------------------

#[test]
fn task_setup_positive_high_confidence() {
    let summary = detect_task_setup(vec![
        TaskSetupSignal::new(
            TaskSetupEvidenceKind::RepeatedOrientation,
            0.6,
            "re-read the same 5 files",
        ),
        TaskSetupSignal::new(
            TaskSetupEvidenceKind::LongPreEditExploration,
            0.55,
            "explored for 40 turns before editing",
        ),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.category, OpportunityCategory::TaskSetup);
    assert_eq!(ann.confidence, OpportunityConfidence::High);
    assert!((ann.score - 1.15).abs() < 0.01);
}

#[test]
fn task_setup_negative_below_threshold() {
    let summary = detect_task_setup(vec![
        TaskSetupSignal::new(
            TaskSetupEvidenceKind::RepeatedOrientation,
            0.25,
            "minor re-read",
        ),
        TaskSetupSignal::new(
            TaskSetupEvidenceKind::LongPreEditExploration,
            0.2,
            "brief exploration",
        ),
    ]);

    assert!(summary.is_empty());
}

#[test]
fn task_setup_confidence_boundary_single_kind() {
    // Score >= 1.0 but only 1 distinct kind → Medium
    let summary = detect_task_setup(vec![
        TaskSetupSignal::new(
            TaskSetupEvidenceKind::PlanningChurn,
            0.6,
            "rewrote plan twice",
        ),
        TaskSetupSignal::new(
            TaskSetupEvidenceKind::PlanningChurn,
            0.5,
            "rewrote plan again",
        ),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.confidence, OpportunityConfidence::Medium);
    assert!(ann.score >= 1.0);
}

// ---------------------------------------------------------------------------
// History Drag
// ---------------------------------------------------------------------------

#[test]
fn history_drag_positive_high_confidence() {
    // 6 turns with ~2x growth, mostly uncached, total > 240
    let turns = vec![
        HistoryDragTurn::new(15.0, 5.0),
        HistoryDragTurn::new(20.0, 5.0),
        HistoryDragTurn::new(25.0, 5.0),
        HistoryDragTurn::new(50.0, 10.0),
        HistoryDragTurn::new(60.0, 10.0),
        HistoryDragTurn::new(70.0, 10.0),
    ];

    let detection = history_drag::detect(&turns).expect("growing series should trigger");
    assert_eq!(
        detection.annotation.category,
        OpportunityCategory::HistoryDrag
    );
    assert_eq!(detection.annotation.confidence, OpportunityConfidence::High);
    assert_eq!(detection.mix, HistoryDragMix::MostlyUncached);
    assert!(detection.annotation.score > 0.0 && detection.annotation.score <= 1.0);
}

#[test]
fn history_drag_negative_flat_series() {
    // 4 turns, enough volume, but flat (ratio < 1.35)
    let turns = vec![
        HistoryDragTurn::new(30.0, 10.0),
        HistoryDragTurn::new(30.0, 10.0),
        HistoryDragTurn::new(30.0, 10.0),
        HistoryDragTurn::new(30.0, 10.0),
    ];

    assert!(history_drag::detect(&turns).is_none());
}

#[test]
fn history_drag_confidence_boundary_medium() {
    // Growth ratio > 1.35 but < 1.8, or total input < 240 → Medium
    let turns = vec![
        HistoryDragTurn::new(20.0, 10.0),
        HistoryDragTurn::new(20.0, 10.0),
        HistoryDragTurn::new(30.0, 15.0),
        HistoryDragTurn::new(30.0, 15.0),
    ];

    let detection = history_drag::detect(&turns).expect("growing series should trigger");
    assert_eq!(
        detection.annotation.confidence,
        OpportunityConfidence::Medium
    );
}

// ---------------------------------------------------------------------------
// Delegation Overhead
// ---------------------------------------------------------------------------

#[test]
fn delegation_positive_high_confidence() {
    let summary = detect_delegation_overhead(vec![
        DelegationSignal::new(
            DelegationEvidenceKind::SmallTaskSidechain,
            0.6,
            "spawned agent for 3-line fix",
        ),
        DelegationSignal::new(
            DelegationEvidenceKind::RelayTraffic,
            0.55,
            "4 relay messages for single file edit",
        ),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.category, OpportunityCategory::Delegation);
    assert_eq!(ann.confidence, OpportunityConfidence::High);
    assert!((ann.score - 1.15).abs() < 0.01);
}

#[test]
fn delegation_negative_below_threshold() {
    let summary = detect_delegation_overhead(vec![
        DelegationSignal::new(
            DelegationEvidenceKind::SmallTaskSidechain,
            0.25,
            "small task",
        ),
        DelegationSignal::new(DelegationEvidenceKind::RelayTraffic, 0.2, "minor relay"),
    ]);

    assert!(summary.is_empty());
}

#[test]
fn delegation_confidence_boundary_single_kind() {
    // Score >= 1.0 but same kind twice → Medium
    let summary = detect_delegation_overhead(vec![
        DelegationSignal::new(
            DelegationEvidenceKind::CoordinationOverhead,
            0.6,
            "excessive coordination A",
        ),
        DelegationSignal::new(
            DelegationEvidenceKind::CoordinationOverhead,
            0.5,
            "excessive coordination B",
        ),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.confidence, OpportunityConfidence::Medium);
    assert!(ann.score >= 1.0);
}

// ---------------------------------------------------------------------------
// Model Mismatch
// ---------------------------------------------------------------------------

fn opus_models() -> BTreeSet<String> {
    BTreeSet::from(["claude-opus-4".to_string()])
}

fn haiku_models() -> BTreeSet<String> {
    BTreeSet::from(["claude-haiku-3".to_string()])
}

#[test]
fn model_mismatch_positive_high_confidence() {
    let signals = vec![
        ModelMismatchSignal {
            action_category: "project discovery".to_string(),
            action_label: "glob project files".to_string(),
            model_names: opus_models(),
            total_tokens: 6_000.0,
            action_count: 10,
        },
        ModelMismatchSignal {
            action_category: "local search/navigation".to_string(),
            action_label: "grep for pattern".to_string(),
            model_names: opus_models(),
            total_tokens: 5_000.0,
            action_count: 8,
        },
        ModelMismatchSignal {
            action_category: "version control".to_string(),
            action_label: "git log".to_string(),
            model_names: opus_models(),
            total_tokens: 2_000.0,
            action_count: 4,
        },
    ];

    let summary = detect_model_mismatch(&signals);
    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.category, OpportunityCategory::ModelMismatch);
    assert_eq!(ann.confidence, OpportunityConfidence::High);
    assert!(ann.score > 0.3);
}

#[test]
fn model_mismatch_negative_haiku_only() {
    let signals = vec![
        ModelMismatchSignal {
            action_category: "project discovery".to_string(),
            action_label: "glob project files".to_string(),
            model_names: haiku_models(),
            total_tokens: 6_000.0,
            action_count: 10,
        },
        ModelMismatchSignal {
            action_category: "local search/navigation".to_string(),
            action_label: "grep for pattern".to_string(),
            model_names: haiku_models(),
            total_tokens: 5_000.0,
            action_count: 8,
        },
    ];

    let summary = detect_model_mismatch(&signals);
    assert!(summary.is_empty());
}

#[test]
fn model_mismatch_confidence_boundary_single_category() {
    // Opus on only 1 routine category → Medium (breadth < 2 families)
    let signals = vec![ModelMismatchSignal {
        action_category: "project discovery".to_string(),
        action_label: "glob project files".to_string(),
        model_names: opus_models(),
        total_tokens: 3_000.0,
        action_count: 8,
    }];

    let summary = detect_model_mismatch(&signals);
    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.confidence, OpportunityConfidence::Medium);
}

// ---------------------------------------------------------------------------
// Prompt Yield
// ---------------------------------------------------------------------------

#[test]
fn prompt_yield_positive_high_confidence() {
    let summary = detect_prompt_yield(vec![
        PromptYieldSignal::new(
            PromptYieldEvidenceKind::HighInputLowEffect,
            0.6,
            "8k tokens in, 200 tokens useful output",
        ),
        PromptYieldSignal::new(
            PromptYieldEvidenceKind::PlanningLoop,
            0.55,
            "rewrote plan 3 times without progress",
        ),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.category, OpportunityCategory::PromptYield);
    assert_eq!(ann.confidence, OpportunityConfidence::High);
    assert!((ann.score - 1.15).abs() < 0.01);
}

#[test]
fn prompt_yield_negative_below_threshold() {
    let summary = detect_prompt_yield(vec![
        PromptYieldSignal::new(
            PromptYieldEvidenceKind::HighInputLowEffect,
            0.25,
            "slightly low yield",
        ),
        PromptYieldSignal::new(PromptYieldEvidenceKind::PlanningLoop, 0.2, "minor loop"),
    ]);

    assert!(summary.is_empty());
}

#[test]
fn prompt_yield_confidence_boundary_single_kind() {
    // Score >= 1.0 but only 1 kind → Medium
    let summary = detect_prompt_yield(vec![
        PromptYieldSignal::new(
            PromptYieldEvidenceKind::LargePasteNoFollowThrough,
            0.6,
            "pasted large block A",
        ),
        PromptYieldSignal::new(
            PromptYieldEvidenceKind::LargePasteNoFollowThrough,
            0.5,
            "pasted large block B",
        ),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.confidence, OpportunityConfidence::Medium);
    assert!(ann.score >= 1.0);
}

// ---------------------------------------------------------------------------
// Search Churn
// ---------------------------------------------------------------------------

#[test]
fn search_churn_positive_high_confidence() {
    let summary = detect_search_churn(vec![
        SearchChurnSignal::new(
            SearchChurnEvidenceKind::RepeatedSearch,
            0.6,
            "searched for 'Config' 5 times",
        ),
        SearchChurnSignal::new(
            SearchChurnEvidenceKind::RepeatedGitInspection,
            0.55,
            "ran git log 4 times on same path",
        ),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.category, OpportunityCategory::SearchChurn);
    assert_eq!(ann.confidence, OpportunityConfidence::High);
    assert!((ann.score - 1.15).abs() < 0.01);
}

#[test]
fn search_churn_negative_below_threshold() {
    let summary = detect_search_churn(vec![
        SearchChurnSignal::new(
            SearchChurnEvidenceKind::RepeatedSearch,
            0.25,
            "minor repeated search",
        ),
        SearchChurnSignal::new(
            SearchChurnEvidenceKind::RepeatedRead,
            0.2,
            "minor repeated read",
        ),
    ]);

    assert!(summary.is_empty());
}

#[test]
fn search_churn_confidence_boundary_single_kind() {
    // Score >= 1.0 but only 1 kind → Medium
    let summary = detect_search_churn(vec![
        SearchChurnSignal::new(
            SearchChurnEvidenceKind::RepeatedRead,
            0.6,
            "re-read file A 4 times",
        ),
        SearchChurnSignal::new(
            SearchChurnEvidenceKind::RepeatedRead,
            0.5,
            "re-read file B 3 times",
        ),
    ]);

    assert_eq!(summary.annotations.len(), 1);
    let ann = &summary.annotations[0];
    assert_eq!(ann.confidence, OpportunityConfidence::Medium);
    assert!(ann.score >= 1.0);
}

// ---------------------------------------------------------------------------
// Tool Result Bloat
// ---------------------------------------------------------------------------

#[test]
fn tool_result_bloat_positive_high_confidence() {
    let annotation = detect_tool_result_bloat(&ToolResultBloatInput {
        tool_name: "shell".to_string(),
        invocation: Some("rg -n TODO .".to_string()),
        total_output_bytes: 256 * 1024,
        result_count: 6,
        estimated_affected_tokens: 9_000.0,
    })
    .expect("large repeated bloat should be detected");

    assert_eq!(annotation.category, OpportunityCategory::ToolResultBloat);
    assert_eq!(annotation.confidence, OpportunityConfidence::High);
    assert!(annotation.score > 0.35 && annotation.score <= 1.0);
}

#[test]
fn tool_result_bloat_negative_below_minimums() {
    let result = detect_tool_result_bloat(&ToolResultBloatInput {
        tool_name: "shell".to_string(),
        invocation: Some("ls".to_string()),
        total_output_bytes: 2 * 1024,
        result_count: 1,
        estimated_affected_tokens: 120.0,
    });

    assert!(result.is_none());
}

#[test]
fn tool_result_bloat_confidence_boundary_medium() {
    // Above minimums but below high thresholds → Medium
    let annotation = detect_tool_result_bloat(&ToolResultBloatInput {
        tool_name: "grep".to_string(),
        invocation: None,
        total_output_bytes: 48 * 1024,
        result_count: 4,
        estimated_affected_tokens: 3_000.0,
    })
    .expect("moderate bloat should be detected");

    assert_eq!(annotation.confidence, OpportunityConfidence::Medium);
    assert!(annotation.score >= 0.35);
}
