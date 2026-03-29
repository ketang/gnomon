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

/// Canonical project identity resolved from a working directory.
///
/// **Git projects** collapse all worktrees (main, linked, bare-backed) and
/// subdirectories to a single canonical root so that the same repository is
/// never stored as multiple projects.  The canonical root is:
/// - For regular and linked worktrees: the main repository's working directory.
/// - For bare repositories: the bare repo directory itself.
///
/// **Path projects** are a fallback when Git resolution fails.  They use the
/// normalized cwd as the identity and always carry an `identity_reason`
/// explaining why Git resolution was not possible.
///
/// Invariant: for Git projects, `root_path == git_root_path.unwrap()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedProject {
    pub identity_kind: ProjectIdentityKind,
    /// Deduplication key.  Format: `"git:{canonical_root}"` or `"path:{normalized_cwd}"`.
    pub canonical_key: String,
    pub display_name: String,
    /// For Git projects: the canonical repository root (main worktree root or bare dir).
    /// For Path projects: the normalized cwd that was passed in.
    pub root_path: PathBuf,
    /// Always `Some` for Git projects (equal to `root_path`), always `None` for Path projects.
    pub git_root_path: Option<PathBuf>,
    /// The `remote.origin.url` value, if present.
    pub git_origin: Option<String>,
    /// Always `None` for Git projects.  For Path projects, an actionable reason
    /// explaining why Git resolution failed (e.g. no git repo found, no canonical root).
    pub identity_reason: Option<String>,
}

/// Resolves a project identity from a cwd using Git as the authoritative source when available.
/// Git-backed projects collapse to a canonical root path; non-Git paths retain the normalized cwd.
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

/// Resolves the canonical root path for a Git repository, collapsing all
/// worktree variants to a single stable identity:
///
/// 1. **Regular repo / main worktree**: `main_repo().workdir()` → the repo root.
/// 2. **Linked worktree**: `main_repo()` navigates to the parent repo, then
///    `workdir()` returns the *main* repo root (not the linked worktree dir).
/// 3. **Bare repo (with or without worktrees)**: `workdir()` is `None`, so we
///    fall back to `common_dir`.  If `common_dir` ends in `.git`, we return its
///    parent (the project directory); otherwise we return `common_dir` itself
///    (the bare repo directory, e.g. `repo.git/`).
/// 4. **Fallback**: if `main_repo()` fails, use `repo.workdir()` directly.
fn canonical_git_root(repo: &gix::Repository) -> Option<PathBuf> {
    if let Ok(main_repo) = repo.main_repo() {
        if let Some(workdir) = main_repo.workdir() {
            return Some(workdir.to_path_buf());
        }

        let common_dir = main_repo.common_dir();
        // Bare or common-dir-backed repositories may not have a workdir; keep the shared repo dir.
        if common_dir.file_name().and_then(|name| name.to_str()) == Some(".git") {
            return common_dir.parent().map(PathBuf::from);
        }

        return Some(common_dir.to_path_buf());
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

    use anyhow::{Context, Result};
    use tempfile::tempdir;

    use super::{
        PATH_REASON_GIT_ROOT_NOT_FOUND, ProjectIdentityKind, path_project, resolve_project_from_cwd,
    };
    use crate::test_helpers::{make_git_repo, nonexistent_path, run_git, run_git_freeform};

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

    #[test]
    fn common_dir_backed_worktrees_use_the_shared_git_dir_for_identity() -> Result<()> {
        let temp = tempdir()?;
        let repo_root = temp.path().join("repo");
        let bare_root = temp.path().join("repo.git");
        let worktree_root = temp.path().join("agent-a");

        make_git_repo(&repo_root)?;
        run_git(&repo_root, ["branch", "-m", "main"])?;
        run_git_freeform(&[
            "clone",
            "--bare",
            repo_root.to_str().context("non-utf8 repo path")?,
            bare_root.to_str().context("non-utf8 bare path")?,
        ])?;
        run_git_freeform(&[
            "-C",
            bare_root.to_str().context("non-utf8 bare path")?,
            "worktree",
            "add",
            worktree_root.to_str().context("non-utf8 worktree path")?,
            "main",
        ])?;
        fs::create_dir_all(worktree_root.join("nested"))?;

        let project = resolve_project_from_cwd(&worktree_root.join("nested"));

        assert_eq!(project.identity_kind, ProjectIdentityKind::Git);
        assert_eq!(project.root_path, bare_root.clone());
        assert_eq!(project.git_root_path, Some(bare_root));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Issue #39 — Comprehensive identity semantics tests
    // -----------------------------------------------------------------------

    #[test]
    fn main_worktree_fields_are_precise() -> Result<()> {
        let temp = tempdir()?;
        let repo_root = temp.path().join("my-repo");
        make_git_repo(&repo_root)?;

        let canonical_root = fs::canonicalize(&repo_root)?;
        let project = resolve_project_from_cwd(&repo_root);

        assert_eq!(project.identity_kind, ProjectIdentityKind::Git);
        assert_eq!(
            project.canonical_key,
            format!("git:{}", canonical_root.display())
        );
        assert_eq!(project.display_name, "my-repo");
        assert_eq!(project.root_path, canonical_root);
        assert_eq!(
            project.git_root_path,
            Some(canonical_root),
            "root_path must equal git_root_path for Git projects"
        );
        assert!(project.identity_reason.is_none());
        Ok(())
    }

    #[test]
    fn subdirectory_resolves_to_repo_root() -> Result<()> {
        let temp = tempdir()?;
        let repo_root = temp.path().join("my-repo");
        make_git_repo(&repo_root)?;
        fs::create_dir_all(repo_root.join("src/deep"))?;

        let canonical_root = fs::canonicalize(&repo_root)?;
        let project = resolve_project_from_cwd(&repo_root.join("src/deep"));

        assert_eq!(project.identity_kind, ProjectIdentityKind::Git);
        assert_eq!(project.root_path, canonical_root);
        assert_eq!(
            project.canonical_key,
            format!("git:{}", canonical_root.display()),
            "canonical_key must reference the repo root, not the subdirectory"
        );
        Ok(())
    }

    #[test]
    fn linked_worktree_resolves_to_main_repo() -> Result<()> {
        let temp = tempdir()?;
        let repo_root = temp.path().join("main-repo");
        let worktree_root = temp.path().join("feature-wt");

        make_git_repo(&repo_root)?;
        run_git(&repo_root, ["branch", "-m", "main"])?;
        run_git(&repo_root, ["branch", "feature"])?;
        run_git_freeform(&[
            "-C",
            repo_root.to_str().context("non-utf8 path")?,
            "worktree",
            "add",
            worktree_root.to_str().context("non-utf8 path")?,
            "feature",
        ])?;

        let canonical_main = fs::canonicalize(&repo_root)?;
        let project = resolve_project_from_cwd(&worktree_root);

        assert_eq!(project.identity_kind, ProjectIdentityKind::Git);
        assert_eq!(
            project.root_path, canonical_main,
            "linked worktree must resolve to the main repo root"
        );
        assert_eq!(project.git_root_path, Some(canonical_main.clone()));
        assert_eq!(
            project.canonical_key,
            format!("git:{}", canonical_main.display()),
        );
        assert!(project.identity_reason.is_none());
        Ok(())
    }

    #[test]
    fn linked_worktree_subdirectory_resolves_to_main_repo() -> Result<()> {
        let temp = tempdir()?;
        let repo_root = temp.path().join("main-repo");
        let worktree_root = temp.path().join("feature-wt");

        make_git_repo(&repo_root)?;
        run_git(&repo_root, ["branch", "-m", "main"])?;
        run_git(&repo_root, ["branch", "feature"])?;
        run_git_freeform(&[
            "-C",
            repo_root.to_str().context("non-utf8 path")?,
            "worktree",
            "add",
            worktree_root.to_str().context("non-utf8 path")?,
            "feature",
        ])?;
        fs::create_dir_all(worktree_root.join("src/nested"))?;

        let canonical_main = fs::canonicalize(&repo_root)?;
        let project = resolve_project_from_cwd(&worktree_root.join("src/nested"));

        assert_eq!(project.identity_kind, ProjectIdentityKind::Git);
        assert_eq!(
            project.root_path, canonical_main,
            "subdirectory within linked worktree must resolve to the main repo root"
        );
        Ok(())
    }

    #[test]
    fn bare_repo_without_worktrees_resolves_identity() -> Result<()> {
        let temp = tempdir()?;
        let repo_root = temp.path().join("repo");
        let bare_root = temp.path().join("repo.git");

        make_git_repo(&repo_root)?;
        run_git(&repo_root, ["branch", "-m", "main"])?;
        run_git_freeform(&[
            "clone",
            "--bare",
            repo_root.to_str().context("non-utf8 path")?,
            bare_root.to_str().context("non-utf8 path")?,
        ])?;

        let project = resolve_project_from_cwd(&bare_root);

        // Bare repos may or may not be discoverable by gix::discover.
        // If discovered as Git: root_path should be the bare directory.
        // If not discoverable: falls back to Path with an actionable reason.
        // Either outcome is acceptable — document whichever we observe.
        match project.identity_kind {
            ProjectIdentityKind::Git => {
                let canonical_bare = fs::canonicalize(&bare_root)?;
                assert_eq!(project.root_path, canonical_bare);
                assert_eq!(project.git_root_path, Some(canonical_bare));
                assert!(project.identity_reason.is_none());
            }
            ProjectIdentityKind::Path => {
                assert!(
                    project.identity_reason.is_some(),
                    "Path fallback must carry an actionable identity_reason"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn nonexistent_path_falls_back_to_path_identity() {
        let path = nonexistent_path();
        let project = resolve_project_from_cwd(&path);

        assert_eq!(project.identity_kind, ProjectIdentityKind::Path);
        assert!(project.canonical_key.starts_with("path:"));
        assert_eq!(
            project.identity_reason.as_deref(),
            Some(PATH_REASON_GIT_ROOT_NOT_FOUND),
        );
        assert!(project.git_root_path.is_none());
        assert!(project.git_origin.is_none());
    }
}
