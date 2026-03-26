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

pub fn default_source_root() -> Result<PathBuf> {
    let base_dirs = BaseDirs::new().context("unable to resolve the current home directory")?;
    Ok(base_dirs.home_dir().join(".claude").join("projects"))
}
