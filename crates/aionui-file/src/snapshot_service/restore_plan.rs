//! Pure restore-plan generation from existing `opencode_tool_snapshots` ledger rows.
//!
//! Reuses the same Git parent-tree semantics as [`super::helpers::revert_files_from_commit`]
//! without mutating the working tree. Self-contained: the git logic lives here
//! (not in `helpers`) to keep the parent module's surface area unchanged and
//! to avoid an import cycle with `restore_plan`.

use git2::{Commit, Repository, Tree};
use serde::{Deserialize, Serialize};

/// How reverting this path would treat it (filesystem-level, not OpenCode session state).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestorePathOperation {
    Create,
    Modify,
    Delete,
    Unknown,
}

/// Per-path restore metadata for a single ledger row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestorePathEntry {
    pub path: String,
    pub operation: RestorePathOperation,
    /// `true` when pre-tool content can be recovered from the parent of `commit_sha`.
    pub prior_content_restorable: bool,
    /// `true` when prior content exists but is not UTF-8 text (preview blocked).
    pub preview_blocked: bool,
    /// Parent commit of the tool-call snapshot (baseline for narrow revert).
    pub source_commit_sha: Option<String>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

/// Restore plan for one `opencode_tool_snapshots` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallRestorePlan {
    pub tool_call_id: String,
    pub commit_sha: String,
    pub paths: Vec<RestorePathEntry>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

/// Tools and mutation surfaces **not** covered by the per-tool-call ledger in this slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestorePlanUnsupportedCoverage {
    /// `run_shell` post-exec deltas are not attributed to tool_call_id without live shell tracking.
    pub run_shell_not_snapshotted: bool,
    /// Mutations outside `local_fs_mcp` (other MCP bridges, OpenCode built-ins) are not ledgered.
    pub non_local_fs_mcp_not_covered: bool,
    /// OpenCode conversation/session revert is a separate surface; this plan is filesystem-only.
    pub opencode_session_revert_not_used: bool,
}

impl Default for RestorePlanUnsupportedCoverage {
    fn default() -> Self {
        Self::documented_defaults()
    }
}

impl RestorePlanUnsupportedCoverage {
    pub fn documented_defaults() -> Self {
        Self {
            run_shell_not_snapshotted: true,
            non_local_fs_mcp_not_covered: true,
            opencode_session_revert_not_used: true,
        }
    }
}

fn parse_commit<'a>(
    repo: &'a Repository,
    commit_sha: &str,
) -> Result<Commit<'a>, (Vec<String>, Vec<String>)> {
    let oid = match git2::Oid::from_str(commit_sha) {
        Ok(o) => o,
        Err(e) => {
            return Err((
                vec![format!("invalid commit SHA '{commit_sha}': {e}")],
                Vec::new(),
            ));
        }
    };
    match repo.find_commit(oid) {
        Ok(c) => Ok(c),
        Err(e) => Err((
            vec![format!("commit not found '{commit_sha}': {e}")],
            Vec::new(),
        )),
    }
}

fn parent_tree<'a>(commit: &'a Commit<'a>) -> Result<Option<Tree<'a>>, String> {
    match commit.parent(0) {
        Ok(p) => p.tree().map(Some).map_err(|e| format!("get parent tree: {e}")),
        Err(_) => Ok(None),
    }
}

fn classify_path(
    repo: &Repository,
    path: &str,
    commit_tree: &Tree<'_>,
    parent_tree: Option<&Tree<'_>>,
) -> (RestorePathOperation, bool, bool, Vec<String>, Vec<String>) {
    let mut warnings = Vec::new();
    let mut errors = Vec::new();
    let in_commit = commit_tree.get_path(std::path::Path::new(path)).is_ok();
    let in_parent = parent_tree
        .and_then(|t| t.get_path(std::path::Path::new(path)).ok())
        .is_some();

    let operation = match (in_commit, in_parent) {
        (true, true) => RestorePathOperation::Modify,
        (true, false) => RestorePathOperation::Create,
        (false, true) => RestorePathOperation::Delete,
        (false, false) => {
            warnings.push(format!("path '{path}' is absent from both commit and parent trees"));
            RestorePathOperation::Unknown
        }
    };

    let (prior_content_restorable, preview_blocked) = match (operation, parent_tree) {
        (RestorePathOperation::Create, _) => (false, false),
        (RestorePathOperation::Modify, Some(t)) => match blob_for_path(repo, t, path) {
            Ok(Some(bytes)) => match std::str::from_utf8(&bytes) {
                Ok(_) => (true, false),
                Err(_) => (true, true),
            },
            Ok(None) => {
                warnings.push(format!("no parent blob for '{path}'"));
                (false, false)
            }
            Err(e) => {
                errors.push(format!("read parent blob for '{path}': {e}"));
                (false, false)
            }
        },
        (RestorePathOperation::Delete, Some(_)) => (true, false),
        (RestorePathOperation::Delete, None) => (false, false),
        (RestorePathOperation::Unknown, _) => (false, false),
        (RestorePathOperation::Modify, None) => {
            warnings.push(format!("no parent tree available for '{path}'; cannot preview"));
            (false, false)
        }
    };

    (operation, prior_content_restorable, preview_blocked, warnings, errors)
}

fn blob_for_path(repo: &Repository, tree: &Tree<'_>, path: &str) -> Result<Option<Vec<u8>>, String> {
    let entry = match tree.get_path(std::path::Path::new(path)) {
        Ok(e) => e,
        Err(_) => return Ok(None),
    };
    let blob = repo
        .find_blob(entry.id())
        .map_err(|e| format!("find_blob for '{path}': {e}"))?;
    Ok(Some(blob.content().to_vec()))
}

/// Build a restore plan from ledger fields + an open git repository.
///
/// Pure w.r.t. the filesystem: does not checkout, write, or call OpenCode.
pub fn build_tool_call_restore_plan(
    tool_call_id: &str,
    commit_sha: &str,
    files_changed: &[String],
    repo: &Repository,
) -> ToolCallRestorePlan {
    let mut plan_warnings = Vec::new();
    let mut plan_errors = Vec::new();

    let commit = match parse_commit(repo, commit_sha) {
        Ok(c) => c,
        Err((errs, warns)) => {
            plan_errors.extend(errs);
            plan_warnings.extend(warns);
            return ToolCallRestorePlan {
                tool_call_id: tool_call_id.to_string(),
                commit_sha: commit_sha.to_string(),
                paths: Vec::new(),
                warnings: plan_warnings,
                errors: plan_errors,
            };
        }
    };

    let commit_tree = match commit.tree() {
        Ok(t) => t,
        Err(e) => {
            plan_errors.push(format!("read commit tree: {e}"));
            return ToolCallRestorePlan {
                tool_call_id: tool_call_id.to_string(),
                commit_sha: commit_sha.to_string(),
                paths: Vec::new(),
                warnings: plan_warnings,
                errors: plan_errors,
            };
        }
    };

    let parent_tree = match parent_tree(&commit) {
        Ok(t) => t,
        Err(e) => {
            plan_errors.push(format!("read parent tree: {e}"));
            return ToolCallRestorePlan {
                tool_call_id: tool_call_id.to_string(),
                commit_sha: commit_sha.to_string(),
                paths: Vec::new(),
                warnings: plan_warnings,
                errors: plan_errors,
            };
        }
    };

    let source_commit_sha = commit.parent(0).ok().map(|p| p.id().to_string());

    let mut paths = Vec::with_capacity(files_changed.len());
    for path in files_changed {
        let (op, restorable, preview_blocked, warns, errs) =
            classify_path(repo, path, &commit_tree, parent_tree.as_ref());
        paths.push(RestorePathEntry {
            path: path.clone(),
            operation: op,
            prior_content_restorable: restorable,
            preview_blocked,
            source_commit_sha: source_commit_sha.clone(),
            warnings: warns,
            errors: errs,
        });
    }

    ToolCallRestorePlan {
        tool_call_id: tool_call_id.to_string(),
        commit_sha: commit_sha.to_string(),
        paths,
        warnings: plan_warnings,
        errors: plan_errors,
    }
}

/// Decode `files_changed_json` from a ledger row; surfaces parse errors on the plan.
pub fn build_tool_call_restore_plan_from_ledger_json(
    tool_call_id: &str,
    commit_sha: &str,
    files_changed_json: &str,
    repo: &Repository,
) -> ToolCallRestorePlan {
    let files: Vec<String> = match serde_json::from_str(files_changed_json) {
        Ok(v) => v,
        Err(e) => {
            return ToolCallRestorePlan {
                tool_call_id: tool_call_id.to_string(),
                commit_sha: commit_sha.to_string(),
                paths: vec![],
                warnings: vec![],
                errors: vec![format!("invalid files_changed_json: {e}")],
            };
        }
    };
    build_tool_call_restore_plan(tool_call_id, commit_sha, &files, repo)
}

/// Returns `true` when every path entry has no blocking errors.
pub fn restore_plan_is_actionable(plan: &ToolCallRestorePlan) -> bool {
    plan.errors.is_empty() && plan.paths.iter().all(|p| p.errors.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use git2::{Repository, Signature};
    use std::path::Path;

    fn init_repo_with_file(path: &Path, file: &str, content: &str) {
        std::fs::write(path.join(file), content).unwrap();
        let repo = Repository::init(path).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new(file)).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = Signature::now("seed", "seed@test").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
            .unwrap();
    }

    fn tool_commit(
        repo: &Repository,
        tool_call_id: &str,
        files: &[&str],
    ) -> String {
        super::super::helpers::commit_tool_changes(
            repo,
            tool_call_id,
            &files.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn restore_plan_modify_marks_prior_content_restorable() {
        let tmp = tempfile::tempdir().unwrap();
        init_repo_with_file(tmp.path(), "a.txt", "v0");
        std::fs::write(tmp.path().join("a.txt"), "v1").unwrap();
        let repo = Repository::open(tmp.path()).unwrap();
        let sha = tool_commit(&repo, "call-1", &["a.txt"]);

        let plan = build_tool_call_restore_plan("call-1", &sha, &["a.txt".to_string()], &repo);
        assert!(plan.errors.is_empty(), "{:?}", plan.errors);
        assert_eq!(plan.paths.len(), 1);
        let entry = &plan.paths[0];
        assert_eq!(entry.path, "a.txt");
        assert_eq!(entry.operation, RestorePathOperation::Modify);
        assert!(entry.prior_content_restorable);
        assert!(!entry.preview_blocked);
        assert!(entry.source_commit_sha.is_some());
    }

    #[test]
    fn restore_plan_create_marks_delete_on_revert() {
        let tmp = tempfile::tempdir().unwrap();
        init_repo_with_file(tmp.path(), "base.txt", "x");
        std::fs::write(tmp.path().join("new.txt"), "n").unwrap();
        let repo = Repository::open(tmp.path()).unwrap();
        let sha = tool_commit(&repo, "call-2", &["new.txt"]);

        let plan = build_tool_call_restore_plan("call-2", &sha, &["new.txt".to_string()], &repo);
        let entry = &plan.paths[0];
        assert_eq!(entry.operation, RestorePathOperation::Create);
        assert!(!entry.prior_content_restorable);
    }

    #[test]
    fn restore_plan_delete_operation() {
        let tmp = tempfile::tempdir().unwrap();
        init_repo_with_file(tmp.path(), "gone.txt", "bye");
        std::fs::remove_file(tmp.path().join("gone.txt")).unwrap();
        let repo = Repository::open(tmp.path()).unwrap();
        let sha = tool_commit(&repo, "call-3", &["gone.txt"]);

        let plan = build_tool_call_restore_plan("call-3", &sha, &["gone.txt".to_string()], &repo);
        let entry = &plan.paths[0];
        assert_eq!(entry.operation, RestorePathOperation::Delete);
        assert!(entry.prior_content_restorable);
    }

    #[test]
    fn restore_plan_invalid_commit_sha_surfaces_error() {
        let tmp = tempfile::tempdir().unwrap();
        init_repo_with_file(tmp.path(), "a.txt", "v0");
        let repo = Repository::open(tmp.path()).unwrap();
        let plan = build_tool_call_restore_plan("call-x", "not-a-sha", &["a.txt".to_string()], &repo);
        assert!(!plan.errors.is_empty() || plan.paths.iter().any(|p| !p.errors.is_empty()));
    }

    #[test]
    fn restore_plan_binary_blocks_preview() {
        // Seed a binary file in the baseline so the tool-call commit MODIFIES
        // a blob that is non-UTF-8. That exercises the preview_blocked=true
        // path (the parent tree has the binary blob, so the operation is
        // Modify, and the UTF-8 check must fail).
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("bin.dat"), [0u8, 1, 2, 255]).unwrap();
        {
            let repo = Repository::init(tmp.path()).unwrap();
            let mut index = repo.index().unwrap();
            index.add_path(Path::new("bin.dat")).unwrap();
            index.write().unwrap();
            let tree_oid = index.write_tree().unwrap();
            let tree = repo.find_tree(tree_oid).unwrap();
            let sig = Signature::now("seed", "seed@test").unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
                .unwrap();
        }
        std::fs::write(tmp.path().join("bin.dat"), [9u8, 8, 7, 6]).unwrap();
        let repo = Repository::open(tmp.path()).unwrap();
        let sha = tool_commit(&repo, "call-bin", &["bin.dat"]);

        let plan = build_tool_call_restore_plan("call-bin", &sha, &["bin.dat".to_string()], &repo);
        let entry = &plan.paths[0];
        assert_eq!(entry.operation, RestorePathOperation::Modify);
        assert!(entry.prior_content_restorable);
        assert!(entry.preview_blocked, "binary parent blob should block preview");
    }

    #[test]
    fn unsupported_coverage_defaults_documented() {
        let cov = RestorePlanUnsupportedCoverage::documented_defaults();
        assert!(cov.run_shell_not_snapshotted);
        assert!(cov.non_local_fs_mcp_not_covered);
        assert!(cov.opencode_session_revert_not_used);
    }
}
