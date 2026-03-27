use std::fs;
use std::path::PathBuf;

use anyhow::Result;

use crate::{db, dirs};

#[derive(Debug, Clone, Default)]
pub struct ConfigOverrides {
    pub db_path: Option<PathBuf>,
    pub source_root: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub app_name: &'static str,
    pub state_dir: PathBuf,
    pub db_path: PathBuf,
    pub source_root: PathBuf,
}

impl RuntimeConfig {
    pub fn load(overrides: ConfigOverrides) -> Result<Self> {
        let state_dir = dirs::default_state_dir()?;
        let db_path = overrides
            .db_path
            .unwrap_or_else(|| state_dir.join(db::DEFAULT_DB_FILENAME));
        let source_root = match overrides.source_root {
            Some(path) => path,
            None => dirs::default_source_root()?,
        };

        Ok(Self {
            app_name: "gnomon",
            state_dir,
            db_path,
            source_root,
        })
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        fs::create_dir_all(&self.state_dir)?;
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyhow::Result;
    use tempfile::tempdir;

    use super::{ConfigOverrides, RuntimeConfig};
    use crate::db::DEFAULT_DB_FILENAME;

    #[test]
    fn load_defaults_uses_platform_dirs() -> Result<()> {
        let config = RuntimeConfig::load(ConfigOverrides::default())?;
        assert_eq!(config.app_name, "gnomon");
        assert!(
            config.state_dir.to_string_lossy().contains("gnomon"),
            "state_dir should contain 'gnomon', got: {}",
            config.state_dir.display()
        );
        assert!(
            config.db_path.ends_with(DEFAULT_DB_FILENAME),
            "db_path should end with '{}', got: {}",
            DEFAULT_DB_FILENAME,
            config.db_path.display()
        );
        Ok(())
    }

    #[test]
    fn load_with_db_path_override_uses_provided_path() -> Result<()> {
        let custom_path = PathBuf::from("/custom/override/usage.sqlite3");
        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: Some(custom_path.clone()),
            source_root: None,
        })?;
        assert_eq!(config.db_path, custom_path);
        Ok(())
    }

    #[test]
    fn load_with_source_root_override_uses_provided_path() -> Result<()> {
        let custom_root = PathBuf::from("/custom/override/source");
        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: None,
            source_root: Some(custom_root.clone()),
        })?;
        assert_eq!(config.source_root, custom_root);
        Ok(())
    }

    #[test]
    fn ensure_dirs_creates_state_directory() -> Result<()> {
        let temp = tempdir()?;
        let state_dir = temp.path().join("nested").join("state");
        let config = RuntimeConfig {
            app_name: "gnomon",
            state_dir: state_dir.clone(),
            db_path: state_dir.join(DEFAULT_DB_FILENAME),
            source_root: temp.path().join("source"),
        };
        assert!(!state_dir.exists(), "state_dir should not exist before ensure_dirs");
        config.ensure_dirs()?;
        assert!(state_dir.exists(), "state_dir should exist after ensure_dirs");
        Ok(())
    }
}
