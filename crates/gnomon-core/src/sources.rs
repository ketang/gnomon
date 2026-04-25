use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::dirs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceProvider {
    Claude,
    Codex,
}

impl SourceProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "claude" => Some(Self::Claude),
            "codex" => Some(Self::Codex),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFileKind {
    Transcript,
    History,
    Rollout,
    SessionIndex,
}

impl SourceFileKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transcript => "transcript",
            Self::History => "history",
            Self::Rollout => "rollout",
            Self::SessionIndex => "session_index",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "transcript" => Some(Self::Transcript),
            "history" => Some(Self::History),
            "rollout" => Some(Self::Rollout),
            "session_index" => Some(Self::SessionIndex),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SourceDescriptor {
    pub provider: SourceProvider,
    pub kind: SourceFileKind,
}

impl SourceDescriptor {
    pub const fn new(provider: SourceProvider, kind: SourceFileKind) -> Self {
        Self { provider, kind }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfiguredSourceLocation {
    Directory { root: PathBuf },
    File { path: PathBuf },
}

impl ConfiguredSourceLocation {
    pub fn scan_root(&self) -> &Path {
        match self {
            Self::Directory { root } => root,
            Self::File { path } => path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredSource {
    pub descriptor: SourceDescriptor,
    pub location: ConfiguredSourceLocation,
}

impl ConfiguredSource {
    pub fn directory(provider: SourceProvider, kind: SourceFileKind, root: PathBuf) -> Self {
        Self {
            descriptor: SourceDescriptor::new(provider, kind),
            location: ConfiguredSourceLocation::Directory { root },
        }
    }

    pub fn file(provider: SourceProvider, kind: SourceFileKind, path: PathBuf) -> Self {
        Self {
            descriptor: SourceDescriptor::new(provider, kind),
            location: ConfiguredSourceLocation::File { path },
        }
    }

    pub fn provider(&self) -> SourceProvider {
        self.descriptor.provider
    }

    pub fn kind(&self) -> SourceFileKind {
        self.descriptor.kind
    }

    pub fn resolve_path(&self, relative_path: &str) -> Result<PathBuf> {
        match &self.location {
            ConfiguredSourceLocation::Directory { root } => Ok(root.join(relative_path)),
            ConfiguredSourceLocation::File { path } => Ok(path.clone()),
        }
    }

    pub fn canonical_relative_path(&self) -> Result<String> {
        match (&self.location, self.kind()) {
            (ConfiguredSourceLocation::File { path }, SourceFileKind::History) => {
                let file_name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .context("configured history file had no UTF-8 file name")?;
                Ok(file_name.to_string())
            }
            (ConfiguredSourceLocation::File { path }, SourceFileKind::SessionIndex) => {
                let file_name = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .context("configured session-index file had no UTF-8 file name")?;
                Ok(file_name.to_string())
            }
            (ConfiguredSourceLocation::File { .. }, other) => bail!(
                "single-file source location is not supported for {}",
                other.as_str()
            ),
            (ConfiguredSourceLocation::Directory { .. }, other) => bail!(
                "directory source location needs a discovered relative path for {}",
                other.as_str()
            ),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfiguredSources {
    entries: Vec<ConfiguredSource>,
}

impl ConfiguredSources {
    pub fn new(entries: Vec<ConfiguredSource>) -> Self {
        Self { entries }
    }

    pub fn legacy_claude(source_root: &Path) -> Self {
        let mut entries = vec![ConfiguredSource::directory(
            SourceProvider::Claude,
            SourceFileKind::Transcript,
            source_root.to_path_buf(),
        )];
        if let Some(history_path) = dirs::default_claude_history_file_for_projects_root(source_root)
        {
            entries.push(ConfiguredSource::file(
                SourceProvider::Claude,
                SourceFileKind::History,
                history_path,
            ));
        }
        Self::new(entries)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ConfiguredSource> {
        self.entries.iter()
    }

    pub fn resolve_path(
        &self,
        descriptor: SourceDescriptor,
        relative_path: &str,
    ) -> Result<PathBuf> {
        let source = self
            .entries
            .iter()
            .find(|entry| entry.descriptor == descriptor)
            .with_context(|| {
                format!(
                    "no configured source location for provider={} kind={}",
                    descriptor.provider.as_str(),
                    descriptor.kind.as_str()
                )
            })?;
        source.resolve_path(relative_path)
    }

    pub fn claude_transcript_root(&self) -> Option<&Path> {
        self.entries.iter().find_map(|entry| {
            (entry.provider() == SourceProvider::Claude
                && entry.kind() == SourceFileKind::Transcript)
                .then(|| match &entry.location {
                    ConfiguredSourceLocation::Directory { root } => root.as_path(),
                    ConfiguredSourceLocation::File { .. } => unreachable!(),
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyhow::Result;

    use super::{
        ConfiguredSource, ConfiguredSources, SourceDescriptor, SourceFileKind, SourceProvider,
    };

    #[test]
    fn directory_source_resolves_relative_paths() -> Result<()> {
        let sources = ConfiguredSources::new(vec![ConfiguredSource::directory(
            SourceProvider::Claude,
            SourceFileKind::Transcript,
            PathBuf::from("/tmp/claude/projects"),
        )]);

        let path = sources.resolve_path(
            SourceDescriptor::new(SourceProvider::Claude, SourceFileKind::Transcript),
            "project/session.jsonl",
        )?;

        assert_eq!(
            path,
            PathBuf::from("/tmp/claude/projects/project/session.jsonl")
        );
        Ok(())
    }

    #[test]
    fn file_source_resolves_to_the_configured_file() -> Result<()> {
        let sources = ConfiguredSources::new(vec![ConfiguredSource::file(
            SourceProvider::Codex,
            SourceFileKind::SessionIndex,
            PathBuf::from("/tmp/.codex/session_index.jsonl"),
        )]);

        let path = sources.resolve_path(
            SourceDescriptor::new(SourceProvider::Codex, SourceFileKind::SessionIndex),
            "session_index.jsonl",
        )?;

        assert_eq!(path, PathBuf::from("/tmp/.codex/session_index.jsonl"));
        Ok(())
    }
}
