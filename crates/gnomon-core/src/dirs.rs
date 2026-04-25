use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use directories::{BaseDirs, ProjectDirs};

pub fn project_dirs() -> Result<ProjectDirs> {
    ProjectDirs::from("com", "ketang", "gnomon")
        .context("unable to resolve platform-specific directories for gnomon")
}

pub fn default_state_dir() -> Result<PathBuf> {
    project_dirs()?
        .state_dir()
        .map(|path| path.to_path_buf())
        .context("unable to resolve a writable state directory for gnomon")
}

pub fn default_config_dir() -> Result<PathBuf> {
    Ok(project_dirs()?.config_dir().to_path_buf())
}

pub fn default_claude_source_root() -> Result<PathBuf> {
    let base_dirs = BaseDirs::new().context("unable to resolve the current home directory")?;
    Ok(base_dirs.home_dir().join(".claude").join("projects"))
}

pub fn default_claude_history_file_for_projects_root(source_root: &Path) -> Option<PathBuf> {
    let projects_dir_name = source_root.file_name()?.to_str()?;
    let claude_dir = source_root.parent()?;
    let claude_dir_name = claude_dir.file_name()?.to_str()?;
    if projects_dir_name != "projects" || claude_dir_name != ".claude" {
        return None;
    }
    Some(claude_dir.join("history.jsonl"))
}

pub fn default_source_root() -> Result<PathBuf> {
    default_claude_source_root()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::{default_source_root, default_state_dir};

    #[test]
    fn default_source_root_ends_with_claude_projects() -> Result<()> {
        let root = default_source_root()?;
        let root_str = root.to_string_lossy();
        assert!(
            root_str.ends_with(".claude/projects") || root_str.ends_with(".claude\\projects"),
            "expected path ending with .claude/projects, got: {root_str}"
        );
        Ok(())
    }

    #[test]
    fn default_state_dir_is_nonempty() -> Result<()> {
        let dir = default_state_dir()?;
        assert!(
            !dir.as_os_str().is_empty(),
            "expected a non-empty state dir path"
        );
        Ok(())
    }
}
