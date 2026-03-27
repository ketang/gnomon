use std::fs;
use std::path::{Path, PathBuf};

pub const AUTHORITATIVE_VCS: &str = "git";
pub const PATH_REASON_GIT_ROOT_NOT_FOUND: &str = "git root could not be resolved from cwd";
pub const PATH_REASON_GIT_ROOT_UNAVAILABLE: &str =
    "git repository did not expose a canonical worktree root";
pub const PATH_REASON_MISSING_CWD: &str = "source file did not contain a cwd";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectIdentityKind {
    Git,
    Path,
}

impl ProjectIdentityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Git => "git",
            Self::Path => "path",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedProject {
    pub identity_kind: ProjectIdentityKind,
    pub canonical_key: String,
    pub display_name: String,
    pub root_path: PathBuf,
    pub git_root_path: Option<PathBuf>,
    pub git_origin: Option<String>,
    pub identity_reason: Option<String>,
}

pub fn resolve_project_from_cwd(cwd: &Path) -> ResolvedProject {
    let normalized_cwd = normalize_path(cwd);

    match gix::discover(&normalized_cwd) {
        Ok(repo) => match canonical_git_root(&repo) {
            Some(git_root) => {
                let git_root = normalize_path(&git_root);
                ResolvedProject {
                    identity_kind: ProjectIdentityKind::Git,
                    canonical_key: format!("git:{}", git_root.display()),
                    display_name: display_name(&git_root),
                    root_path: git_root.clone(),
                    git_root_path: Some(git_root),
                    git_origin: git_origin(&repo),
                    identity_reason: None,
                }
            }
            None => path_project(&normalized_cwd, PATH_REASON_GIT_ROOT_UNAVAILABLE),
        },
        Err(_) => path_project(&normalized_cwd, PATH_REASON_GIT_ROOT_NOT_FOUND),
    }
}

pub fn path_project(path: &Path, reason: impl Into<String>) -> ResolvedProject {
    let normalized_path = normalize_path(path);
    let reason = reason.into();

    ResolvedProject {
        identity_kind: ProjectIdentityKind::Path,
        canonical_key: format!("path:{}", normalized_path.display()),
        display_name: display_name(&normalized_path),
        root_path: normalized_path,
        git_root_path: None,
        git_origin: None,
        identity_reason: Some(reason),
    }
}

fn canonical_git_root(repo: &gix::Repository) -> Option<PathBuf> {
    if let Ok(main_repo) = repo.main_repo() {
        if let Some(workdir) = main_repo.workdir() {
            return Some(workdir.to_path_buf());
        }

        let common_dir = main_repo.common_dir();
        if common_dir.file_name().and_then(|name| name.to_str()) == Some(".git") {
            return common_dir.parent().map(PathBuf::from);
        }
    }

    repo.workdir().map(PathBuf::from)
}

fn git_origin(repo: &gix::Repository) -> Option<String> {
    repo.config_snapshot()
        .string("remote.origin.url")
        .map(|value| String::from_utf8_lossy(value.as_ref()).into_owned())
}

fn normalize_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| path.display().to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use anyhow::Result;
    use tempfile::tempdir;

    use super::{
        PATH_REASON_GIT_ROOT_NOT_FOUND, ProjectIdentityKind, path_project,
        resolve_project_from_cwd,
    };
    use crate::test_helpers::make_git_repo;

    #[test]
    fn resolve_git_project_returns_git_kind() -> Result<()> {
        let temp = tempdir()?;
        let repo_root = temp.path().join("my-repo");
        make_git_repo(&repo_root)?;

        let project = resolve_project_from_cwd(&repo_root);

        assert_eq!(project.identity_kind, ProjectIdentityKind::Git);
        assert!(
            project.canonical_key.starts_with("git:"),
            "canonical_key should start with 'git:', got: {}",
            project.canonical_key
        );
        assert!(
            project.identity_reason.is_none(),
            "Git projects should have no identity_reason"
        );
        Ok(())
    }

    #[test]
    fn resolve_non_git_path_returns_path_kind() -> Result<()> {
        let temp = tempdir()?;
        let plain_dir = temp.path().join("plain-dir");
        fs::create_dir_all(&plain_dir)?;

        let project = resolve_project_from_cwd(&plain_dir);

        assert_eq!(project.identity_kind, ProjectIdentityKind::Path);
        assert!(
            project.canonical_key.starts_with("path:"),
            "canonical_key should start with 'path:', got: {}",
            project.canonical_key
        );
        assert_eq!(
            project.identity_reason.as_deref(),
            Some(PATH_REASON_GIT_ROOT_NOT_FOUND),
        );
        Ok(())
    }

    #[test]
    fn path_project_sets_correct_fields() -> Result<()> {
        let temp = tempdir()?;
        let dir = temp.path().join("my-project");
        fs::create_dir_all(&dir)?;
        let canonical = fs::canonicalize(&dir)?;

        let project = path_project(&dir, "test reason");

        assert_eq!(project.identity_kind, ProjectIdentityKind::Path);
        assert_eq!(
            project.canonical_key,
            format!("path:{}", canonical.display())
        );
        assert_eq!(project.identity_reason.as_deref(), Some("test reason"));
        assert!(project.git_root_path.is_none());
        assert!(project.git_origin.is_none());
        Ok(())
    }

    #[test]
    fn display_name_derived_from_directory_name() -> Result<()> {
        let temp = tempdir()?;
        let dir = temp.path().join("cool-project-name");
        fs::create_dir_all(&dir)?;

        let project = path_project(&dir, "reason");

        assert_eq!(project.display_name, "cool-project-name");
        Ok(())
    }
}
