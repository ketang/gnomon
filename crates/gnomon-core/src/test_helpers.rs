use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde_json::json;
use tempfile::{TempDir, tempdir};

use crate::db::{DEFAULT_DB_FILENAME, Database};

/// Creates a temp directory containing a fresh Database with all migrations applied.
/// The caller must keep the returned `TempDir` alive for as long as the `Database` is used.
pub(crate) fn make_temp_db() -> Result<(TempDir, Database)> {
    let dir = tempdir()?;
    let db = Database::open(dir.path().join(DEFAULT_DB_FILENAME))?;
    Ok((dir, db))
}

/// Initialises a git repository at `repo_root` with one seed commit so that
/// `gix::discover` can resolve it as a valid Git project.
/// Creates `repo_root` if it does not already exist.
pub(crate) fn make_git_repo(repo_root: &Path) -> Result<()> {
    fs::create_dir_all(repo_root)?;
    run_git(repo_root, ["init"])?;
    run_git(repo_root, ["config", "user.email", "gnomon@example.com"])?;
    run_git(repo_root, ["config", "user.name", "Gnomon Tests"])?;
    fs::write(repo_root.join("README.md"), "seed\n")?;
    run_git(repo_root, ["add", "."])?;
    run_git(repo_root, ["commit", "-m", "seed"])?;
    Ok(())
}

/// Writes a one-line JSONL file at `path` whose first record contains `{"cwd": "<cwd>"}`.
/// Creates parent directories as needed.
pub(crate) fn make_jsonl_file(path: &Path, cwd: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{}\n", json!({ "cwd": cwd })))
        .with_context(|| format!("unable to write {}", path.display()))?;
    Ok(())
}

/// Runs a git subcommand in `repo_root` and returns an error if it fails.
pub(crate) fn run_git<const N: usize>(repo_root: &Path, args: [&str; N]) -> Result<()> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()
        .with_context(|| format!("unable to run git {:?}", args))?;
    if !output.status.success() {
        bail!(
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

/// Returns a `PathBuf` guaranteed not to exist in the filesystem.
/// Useful for testing "path does not exist" branches.
pub(crate) fn nonexistent_path() -> PathBuf {
    PathBuf::from("/tmp/__gnomon_test_nonexistent_path_that_should_never_exist__")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use anyhow::Result;

    use super::{make_git_repo, make_jsonl_file, make_temp_db, nonexistent_path};

    #[test]
    fn make_temp_db_creates_database_with_migrations() -> Result<()> {
        let (_dir, db) = make_temp_db()?;
        // schema_version > 0 proves migrations ran
        assert!(db.schema_version()? > 0);
        Ok(())
    }

    #[test]
    fn make_git_repo_creates_valid_repository() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let repo_root = dir.path().join("repo");
        make_git_repo(&repo_root)?;
        assert!(repo_root.join(".git").exists());
        assert!(repo_root.join("README.md").exists());
        Ok(())
    }

    #[test]
    fn make_jsonl_file_writes_cwd_record() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let jsonl = dir.path().join("sub").join("sessions.jsonl");
        let cwd = Path::new("/some/project");
        make_jsonl_file(&jsonl, cwd)?;
        let contents = std::fs::read_to_string(&jsonl)?;
        assert!(contents.contains("/some/project"));
        Ok(())
    }

    #[test]
    fn nonexistent_path_does_not_exist() {
        assert!(!nonexistent_path().exists());
    }
}
