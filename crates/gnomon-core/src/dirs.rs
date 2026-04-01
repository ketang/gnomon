use std::path::PathBuf;

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

pub fn default_source_root() -> Result<PathBuf> {
    let base_dirs = BaseDirs::new().context("unable to resolve the current home directory")?;
    Ok(base_dirs.home_dir().join(".claude").join("projects"))
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
