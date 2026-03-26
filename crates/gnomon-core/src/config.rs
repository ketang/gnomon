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
