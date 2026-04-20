#![allow(dead_code)]

//! Shared helpers for provider-aware import tests that consume the
//! checked-in Claude and Codex fixture corpora at
//! `crates/gnomon-core/tests/fixtures/{claude,codex}/`.

use std::path::{Path, PathBuf};

use crate::sources::{ConfiguredSource, ConfiguredSources, SourceFileKind, SourceProvider};

pub(crate) fn claude_fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("claude")
}

pub(crate) fn codex_fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("codex")
}

pub(crate) fn claude_fixture_sources() -> ConfiguredSources {
    let root = claude_fixture_root();
    ConfiguredSources::new(vec![
        ConfiguredSource::directory(
            SourceProvider::Claude,
            SourceFileKind::Transcript,
            root.join("projects"),
        ),
        ConfiguredSource::file(
            SourceProvider::Claude,
            SourceFileKind::History,
            root.join("history.jsonl"),
        ),
    ])
}

pub(crate) fn codex_fixture_sources() -> ConfiguredSources {
    let root = codex_fixture_root();
    ConfiguredSources::new(vec![
        ConfiguredSource::directory(
            SourceProvider::Codex,
            SourceFileKind::Rollout,
            root.join("sessions"),
        ),
        ConfiguredSource::file(
            SourceProvider::Codex,
            SourceFileKind::History,
            root.join("history.jsonl"),
        ),
        ConfiguredSource::file(
            SourceProvider::Codex,
            SourceFileKind::SessionIndex,
            root.join("session_index.jsonl"),
        ),
    ])
}

pub(crate) fn mixed_fixture_sources() -> ConfiguredSources {
    let claude_root = claude_fixture_root();
    let codex_root = codex_fixture_root();
    ConfiguredSources::new(vec![
        ConfiguredSource::directory(
            SourceProvider::Claude,
            SourceFileKind::Transcript,
            claude_root.join("projects"),
        ),
        ConfiguredSource::file(
            SourceProvider::Claude,
            SourceFileKind::History,
            claude_root.join("history.jsonl"),
        ),
        ConfiguredSource::directory(
            SourceProvider::Codex,
            SourceFileKind::Rollout,
            codex_root.join("sessions"),
        ),
        ConfiguredSource::file(
            SourceProvider::Codex,
            SourceFileKind::History,
            codex_root.join("history.jsonl"),
        ),
        ConfiguredSource::file(
            SourceProvider::Codex,
            SourceFileKind::SessionIndex,
            codex_root.join("session_index.jsonl"),
        ),
    ])
}
