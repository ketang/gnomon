use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use glob::Pattern;
use serde::{Deserialize, Serialize};

use crate::{db, dirs};

pub const DEFAULT_CONFIG_FILENAME: &str = "config.toml";

const DEFAULT_CONFIG_TEMPLATE: &str = r#"# gnomon user configuration
#
# Edit this file to control where gnomon reads session history from and which
# projects should be excluded from import.

[source]
root = "~/.claude/projects"

[project_identity]
stale_claude_worktree_recovery = true
fallback_path_projects = true

[[project_filters]]
action = "exclude"
match_on = "resolved_root"
path_prefix = "/tmp/"
"#;

#[derive(Debug, Clone, Default)]
pub struct ConfigOverrides {
    pub db_path: Option<PathBuf>,
    pub source_root: Option<PathBuf>,
    pub state_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub app_name: &'static str,
    pub state_dir: PathBuf,
    pub config_path: PathBuf,
    pub db_path: PathBuf,
    pub source_root: PathBuf,
    pub project_identity: ProjectIdentityPolicy,
    pub project_filters: Vec<ProjectFilterRule>,
    pub rtk: RtkConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Default)]
struct FileConfig {
    #[serde(default)]
    source: SourceConfig,
    #[serde(default)]
    project_identity: ProjectIdentityPolicy,
    #[serde(default)]
    project_filters: Vec<ProjectFilterRule>,
    #[serde(default)]
    rtk: RtkConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
struct SourceConfig {
    root: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProjectIdentityPolicy {
    #[serde(default = "default_true")]
    pub stale_claude_worktree_recovery: bool,
    #[serde(default = "default_true")]
    pub fallback_path_projects: bool,
}

impl Default for ProjectIdentityPolicy {
    fn default() -> Self {
        Self {
            stale_claude_worktree_recovery: true,
            fallback_path_projects: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectFilterAction {
    Include,
    Exclude,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectFilterMatchOn {
    RawCwd,
    ResolvedRoot,
    IdentityReason,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProjectFilterRule {
    pub action: ProjectFilterAction,
    pub match_on: ProjectFilterMatchOn,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub glob: Option<String>,
    #[serde(default)]
    pub equals: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProjectFilterContext<'a> {
    pub raw_cwd: Option<&'a Path>,
    pub resolved_root: &'a Path,
    pub identity_reason: Option<&'a str>,
}

impl ProjectFilterRule {
    pub fn matches(&self, context: &ProjectFilterContext<'_>) -> Result<bool> {
        let value = match self.match_on {
            ProjectFilterMatchOn::RawCwd => match context.raw_cwd {
                Some(path) => path.to_string_lossy().into_owned(),
                None => return Ok(false),
            },
            ProjectFilterMatchOn::ResolvedRoot => {
                context.resolved_root.to_string_lossy().into_owned()
            }
            ProjectFilterMatchOn::IdentityReason => match context.identity_reason {
                Some(reason) => reason.to_string(),
                None => return Ok(false),
            },
        };

        let mut matcher_count = 0usize;
        let mut matched = false;

        if let Some(prefix) = &self.path_prefix {
            matcher_count += 1;
            matched |= value.starts_with(&normalize_match_string(prefix, self.match_on)?);
        }
        if let Some(glob) = &self.glob {
            matcher_count += 1;
            let pattern = Pattern::new(&normalize_match_string(glob, self.match_on)?)
                .with_context(|| format!("invalid project filter glob pattern `{glob}`"))?;
            matched |= pattern.matches(&value);
        }
        if let Some(equals) = &self.equals {
            matcher_count += 1;
            matched |= value == normalize_match_string(equals, self.match_on)?;
        }

        if matcher_count != 1 {
            bail!(
                "project filter rule for {:?} must define exactly one matcher",
                self.match_on
            );
        }

        Ok(matched)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct RtkConfig {
    #[serde(default = "RtkConfig::default_enabled")]
    pub enabled: bool,
    #[serde(default = "RtkConfig::default_db_path")]
    pub db_path: String,
    #[serde(default = "RtkConfig::default_pre_slack_ms")]
    pub pre_slack_ms: u64,
    #[serde(default = "RtkConfig::default_post_slack_ms")]
    pub post_slack_ms: u64,
}

impl RtkConfig {
    fn default_enabled() -> bool {
        true
    }
    fn default_db_path() -> String {
        "~/.local/share/rtk/history.db".to_string()
    }
    fn default_pre_slack_ms() -> u64 {
        2000
    }
    fn default_post_slack_ms() -> u64 {
        30000
    }

    pub fn resolved_db_path(&self) -> Result<PathBuf> {
        expand_user_path(&self.db_path)
    }
}

impl Default for RtkConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            db_path: Self::default_db_path(),
            pre_slack_ms: Self::default_pre_slack_ms(),
            post_slack_ms: Self::default_post_slack_ms(),
        }
    }
}

impl RuntimeConfig {
    pub fn load(overrides: ConfigOverrides) -> Result<Self> {
        let state_dir = match overrides.state_dir {
            Some(path) => path,
            None => dirs::default_state_dir()?,
        };
        let config_path = match overrides.config_path {
            Some(path) => path,
            None => dirs::default_config_dir()?.join(DEFAULT_CONFIG_FILENAME),
        };
        bootstrap_config_file(&config_path)?;
        let file_config = load_file_config(&config_path)?;
        let db_path = overrides
            .db_path
            .unwrap_or_else(|| state_dir.join(db::DEFAULT_DB_FILENAME));
        let source_root = match overrides.source_root {
            Some(path) => path,
            None => match file_config.source.root {
                Some(path) => expand_user_path(&path)?,
                None => dirs::default_source_root()?,
            },
        };

        Ok(Self {
            app_name: "gnomon",
            state_dir,
            config_path,
            db_path,
            source_root,
            project_identity: file_config.project_identity,
            project_filters: file_config.project_filters,
            rtk: file_config.rtk,
        })
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        fs::create_dir_all(&self.state_dir)?;
        if let Some(parent) = self.config_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if !self.config_path.exists() {
            fs::write(&self.config_path, DEFAULT_CONFIG_TEMPLATE).with_context(|| {
                format!(
                    "unable to write default config {}",
                    self.config_path.display()
                )
            })?;
        }

        Ok(())
    }
}

fn bootstrap_config_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("unable to create config dir {}", parent.display()))?;
    }
    if !path.exists() {
        fs::write(path, DEFAULT_CONFIG_TEMPLATE)
            .with_context(|| format!("unable to write default config {}", path.display()))?;
    }
    Ok(())
}

fn load_file_config(path: &Path) -> Result<FileConfig> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("unable to read config file {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("unable to parse config file {}", path.display()))
}

fn normalize_match_string(value: &str, match_on: ProjectFilterMatchOn) -> Result<String> {
    match match_on {
        ProjectFilterMatchOn::RawCwd | ProjectFilterMatchOn::ResolvedRoot => {
            Ok(expand_user_path(value)?.to_string_lossy().into_owned())
        }
        ProjectFilterMatchOn::IdentityReason => Ok(value.to_string()),
    }
}

fn expand_user_path(value: &str) -> Result<PathBuf> {
    if let Some(rest) = value.strip_prefix("~/") {
        let home = directories::BaseDirs::new()
            .context("unable to resolve the current home directory")?
            .home_dir()
            .to_path_buf();
        return Ok(home.join(rest));
    }

    if value == "~" {
        let home = directories::BaseDirs::new()
            .context("unable to resolve the current home directory")?
            .home_dir()
            .to_path_buf();
        return Ok(home);
    }

    Ok(PathBuf::from(value))
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use anyhow::Result;
    use tempfile::tempdir;

    use super::{
        ConfigOverrides, DEFAULT_CONFIG_FILENAME, FileConfig, ProjectFilterAction,
        ProjectFilterContext, ProjectFilterMatchOn, ProjectFilterRule, RtkConfig, RuntimeConfig,
    };
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
            state_dir: None,
            config_path: None,
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
            state_dir: None,
            config_path: None,
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
            config_path: state_dir.join(DEFAULT_CONFIG_FILENAME),
            db_path: state_dir.join(DEFAULT_DB_FILENAME),
            source_root: temp.path().join("source"),
            project_identity: Default::default(),
            project_filters: Vec::new(),
            rtk: Default::default(),
        };
        assert!(
            !state_dir.exists(),
            "state_dir should not exist before ensure_dirs"
        );
        config.ensure_dirs()?;
        assert!(
            state_dir.exists(),
            "state_dir should exist after ensure_dirs"
        );
        Ok(())
    }

    #[test]
    fn load_bootstraps_default_config_file() -> Result<()> {
        let temp = tempdir()?;
        let state_dir = temp.path().join("state");
        let config_path = temp.path().join("config").join(DEFAULT_CONFIG_FILENAME);

        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: None,
            source_root: None,
            state_dir: Some(state_dir),
            config_path: Some(config_path.clone()),
        })?;

        assert!(config_path.exists());
        assert_eq!(config.config_path, config_path);
        assert!(config.project_filters.iter().any(|rule| {
            rule.match_on == ProjectFilterMatchOn::ResolvedRoot
                && rule.action == ProjectFilterAction::Exclude
                && rule.path_prefix.as_deref() == Some("/tmp/")
        }));
        Ok(())
    }

    #[test]
    fn load_uses_configured_source_root() -> Result<()> {
        let temp = tempdir()?;
        let config_path = temp.path().join("config.toml");
        fs::write(&config_path, "[source]\nroot = \"/tmp/gnomon-source\"\n")?;

        let config = RuntimeConfig::load(ConfigOverrides {
            db_path: None,
            source_root: None,
            state_dir: Some(temp.path().join("state")),
            config_path: Some(config_path),
        })?;

        assert_eq!(config.source_root, PathBuf::from("/tmp/gnomon-source"));
        Ok(())
    }

    #[test]
    fn project_filter_rule_matches_path_prefix() -> Result<()> {
        let rule = ProjectFilterRule {
            action: ProjectFilterAction::Exclude,
            match_on: ProjectFilterMatchOn::ResolvedRoot,
            path_prefix: Some("/tmp/".to_string()),
            glob: None,
            equals: None,
        };
        let ctx = ProjectFilterContext {
            raw_cwd: Some(Path::new("/tmp/example")),
            resolved_root: Path::new("/tmp/example"),
            identity_reason: Some("git root could not be resolved from cwd"),
        };

        assert!(rule.matches(&ctx)?);
        Ok(())
    }

    #[test]
    fn rtk_config_defaults_to_enabled_with_standard_db_path() {
        let cfg = RtkConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.pre_slack_ms, 2000);
        assert_eq!(cfg.post_slack_ms, 30000);
    }

    #[test]
    fn file_config_with_rtk_section_parses_correctly() {
        let toml = r#"
[rtk]
enabled = false
pre_slack_ms = 5000
"#;
        let file_cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(!file_cfg.rtk.enabled);
        assert_eq!(file_cfg.rtk.pre_slack_ms, 5000);
        assert_eq!(file_cfg.rtk.post_slack_ms, 30000); // default
    }
}
