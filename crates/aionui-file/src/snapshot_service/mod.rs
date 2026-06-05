//! Git-based workspace snapshot service.
//!
//! Supports two modes:
//! - **git-repo**: directory already has `.git` — uses it directly.
//! - **snapshot**: no `.git` — creates a temporary git repo that tracks the
//!   workspace via a separate worktree.

mod helpers;

use aionui_common::{AppError, FileChangeOperation};
use dashmap::DashMap;
use git2::Repository;

use crate::types::{CompareResult, SnapshotInfo, SnapshotMode};

use helpers::{
    SNAPSHOT_DIR_PREFIX, WorkspaceState, build_info, commit_tool_changes, discard_single_file, init_snapshot_repo,
    list_branches, open_repo, parse_statuses, read_baseline, reset_single_file, resolve_workspace,
    revert_files_from_commit, stage_all_with_deletions, stage_single_file, temp_repo_path, unstage_all_files,
    unstage_single_file,
};

// ---------------------------------------------------------------------------
// SnapshotService
// ---------------------------------------------------------------------------

/// Git-based workspace snapshot service.
pub struct SnapshotService {
    workspaces: DashMap<String, WorkspaceState>,
}

impl Default for SnapshotService {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotService {
    pub fn new() -> Self {
        Self {
            workspaces: DashMap::new(),
        }
    }

    /// Remove leftover `aionui-snapshot-*` directories from the system temp
    /// dir. Call once at application startup.
    pub fn cleanup_stale_snapshots() {
        let temp_dir = std::env::temp_dir();
        let entries = match std::fs::read_dir(&temp_dir) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to read temp dir for snapshot cleanup"
                );
                return;
            }
        };
        for entry in entries.flatten() {
            let name = match entry.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            if name.starts_with(SNAPSHOT_DIR_PREFIX) {
                let path = entry.path();
                if let Err(e) = std::fs::remove_dir_all(&path) {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to clean up stale snapshot directory"
                    );
                } else {
                    tracing::info!(
                        path = %path.display(),
                        "Cleaned up stale snapshot directory"
                    );
                }
            }
        }
    }

    /// Stage `changed_files` and create a commit on HEAD attributing the
    /// change to `tool_call_id`. Returns the new commit SHA as a lowercase
    /// hex string.
    ///
    /// Requires that exactly one workspace is tracked (per-conversation
    /// scoping). Callers that need to scope by workspace key should resolve
    /// the workspace explicitly and use the trait API.
    pub async fn commit_tool_snapshot(
        &self,
        tool_call_id: &str,
        changed_files: &[String],
    ) -> Result<String, AppError> {
        let state = get_single_workspace(&self.workspaces)?;
        let tool_call_id = tool_call_id.to_owned();
        let files: Vec<String> = changed_files.to_vec();

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            let sha = commit_tool_changes(&repo, &tool_call_id, &files)?;
            tracing::info!(
                tool_call_id = %tool_call_id,
                commit_sha = %sha,
                files_changed = files.len(),
                "Committed tool-call snapshot"
            );
            Ok::<String, AppError>(sha)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    /// Restore the working tree for `files_to_revert` to the state recorded
    /// in the **parent** of `commit_sha`. Files created by that tool call
    /// (not present in the parent tree) are deleted from disk. The rest of
    /// the working tree is left untouched.
    pub async fn revert_to_tool_snapshot(
        &self,
        commit_sha: &str,
        files_to_revert: &[String],
    ) -> Result<(), AppError> {
        let state = get_single_workspace(&self.workspaces)?;
        let commit_sha = commit_sha.to_owned();
        let files: Vec<String> = files_to_revert.to_vec();

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            revert_files_from_commit(&repo, &state.workspace_path, &commit_sha, &files)?;
            tracing::info!(
                commit_sha = %commit_sha,
                files_reverted = files.len(),
                "Reverted tool-call snapshot (narrow)"
            );
            Ok(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    /// Native workspace VCS status (Task 18.1).
    ///
    /// Reads the workspace's local git repo directly (does not require a
    /// prior [`ISnapshotService::init`] call) and returns a unified-diff
    /// payload when the workspace is a tracked git repo. When no `.git`
    /// directory is present, returns `mode = "not-git"` with empty
    /// `patches` so the UI can prompt the user to call
    /// `workspace_vcs_init`.
    ///
    /// This is an inherent method (not on `ISnapshotService`) because the
    /// existing trait is keyed on the already-registered workspace state
    /// and assumes the workspace has been `init`'d; the Task 18.1
    /// status route needs to answer the "is this even a git repo?"
    /// question without forcing a state mutation on the dashboard load.
    pub async fn workspace_vcs_status(
        &self,
        workspace: &str,
    ) -> Result<aionui_api_types::WorkspaceVcsResponse, AppError> {
        use aionui_api_types::{FileDiffEntryResponse, WorkspaceVcsResponse, WorkspaceVcsSummary};
        use git2::Repository;

        let workspace = workspace.to_owned();

        tokio::task::spawn_blocking(move || {
            let canonical = resolve_workspace(&workspace)?;
            let git_dir = canonical.join(".git");

            if !git_dir.exists() {
                return Ok(WorkspaceVcsResponse {
                    mode: "not-git".into(),
                    is_tracked: false,
                    summary: WorkspaceVcsSummary::default(),
                    patches: Vec::new(),
                });
            }

            let repo = Repository::open(&canonical).map_err(|e| {
                AppError::Internal(format!(
                    "Failed to open git repo at {}: {}",
                    canonical.display(),
                    e
                ))
            })?;

            let entries = helpers::workspace_diff(&repo)?;

            let mut files_changed: usize = 0;
            let mut additions: usize = 0;
            let mut deletions: usize = 0;
            let patches: Vec<FileDiffEntryResponse> = entries
                .into_iter()
                .map(|e| {
                    files_changed += 1;
                    additions += e.additions as usize;
                    deletions += e.deletions as usize;
                    FileDiffEntryResponse {
                        relative_path: e.relative_path,
                        patch: e.patch,
                        additions: e.additions,
                        deletions: e.deletions,
                        // Decouple the response from `aionui-common`'s
                        // `lowercase` serde rename by serialising the
                        // `Debug` string of the internal
                        // `FileChangeOperation` ("Create" | "Modify" |
                        // "Delete") — matches the convention adopted in
                        // Task 18 for `FileDiffEntryResponse`.
                        operation: format!("{:?}", e.operation),
                    }
                })
                .collect();

            Ok(WorkspaceVcsResponse {
                mode: "git".into(),
                is_tracked: true,
                summary: WorkspaceVcsSummary {
                    files_changed,
                    additions,
                    deletions,
                },
                patches,
            })
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    /// Native workspace VCS init (Task 18.1).
    ///
    /// Idempotent. For a workspace that is already a git repo this just
    /// confirms it is registered in the service's per-workspace state.
    /// For a non-git workspace this creates a temporary git repo under
    /// the system temp dir that tracks the workspace via a separate
    /// worktree (snapshot mode), so subsequent `workspace_vcs_status`
    /// calls can return a meaningful diff. Delegates to the existing
    /// [`ISnapshotService::init`] implementation, which already handles
    /// both modes and the "already initialized" path.
    pub async fn workspace_vcs_init(&self, workspace: &str) -> Result<(), AppError> {
        crate::traits::ISnapshotService::init(self, workspace).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helper: get workspace state or return error
// ---------------------------------------------------------------------------

fn get_state(workspaces: &DashMap<String, WorkspaceState>, workspace: &str) -> Result<WorkspaceState, AppError> {
    workspaces
        .get(workspace)
        .map(|r| r.clone())
        .ok_or_else(|| AppError::BadRequest(format!("Workspace not initialized: {}", workspace)))
}

/// Return the single tracked workspace, or error if zero or more than one
/// are tracked. Used by the per-tool-call inherent methods which assume a
/// one-workspace context (per-conversation scoping).
fn get_single_workspace(workspaces: &DashMap<String, WorkspaceState>) -> Result<WorkspaceState, AppError> {
    let mut iter = workspaces.iter();
    let first = iter
        .next()
        .ok_or_else(|| AppError::BadRequest("No workspace initialized".into()))?;
    if iter.next().is_some() {
        return Err(AppError::BadRequest(
            "Per-tool-call snapshot methods require exactly one tracked workspace".into(),
        ));
    }
    Ok(first.value().clone())
}

// ---------------------------------------------------------------------------
// ISnapshotService implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl crate::traits::ISnapshotService for SnapshotService {
    async fn init(&self, workspace: &str) -> Result<SnapshotInfo, AppError> {
        let ws = workspace.to_owned();

        // Check if already initialized
        if let Some(state) = self.workspaces.get(&ws) {
            let st = state.clone();
            return tokio::task::spawn_blocking(move || {
                let repo = open_repo(&st)?;
                Ok(build_info(st.mode, &repo))
            })
            .await
            .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?;
        }

        let ws_clone = ws.clone();
        let result = tokio::task::spawn_blocking(move || {
            let canonical = resolve_workspace(&ws_clone)?;
            let canonical_str = canonical.to_string_lossy().to_string();

            let git_dir = canonical.join(".git");
            let (mode, repo_path) = if git_dir.exists() {
                (SnapshotMode::GitRepo, canonical.clone())
            } else {
                let temp = temp_repo_path(&canonical_str);
                init_snapshot_repo(&canonical, &temp)?;
                (SnapshotMode::Snapshot, temp)
            };

            let state = WorkspaceState {
                mode,
                repo_path: repo_path.clone(),
                workspace_path: canonical,
            };

            let repo = Repository::open(&repo_path)
                .map_err(|e| AppError::Internal(format!("Failed to open repo after init: {}", e)))?;
            let info = build_info(mode, &repo);

            Ok::<(WorkspaceState, SnapshotInfo), AppError>((state, info))
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))??;

        let (state, info) = result;
        self.workspaces.insert(ws, state);
        Ok(info)
    }

    async fn get_info(&self, workspace: &str) -> Result<SnapshotInfo, AppError> {
        let state = get_state(&self.workspaces, workspace)?;

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            Ok(build_info(state.mode, &repo))
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn compare(&self, workspace: &str) -> Result<CompareResult, AppError> {
        let state = get_state(&self.workspaces, workspace)?;

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            parse_statuses(&repo, &state.workspace_path)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn get_baseline_content(&self, workspace: &str, file_path: &str) -> Result<Option<String>, AppError> {
        let state = get_state(&self.workspaces, workspace)?;
        let rel = file_path.to_owned();

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            read_baseline(&repo, &rel)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn stage_file(&self, workspace: &str, file_path: &str) -> Result<(), AppError> {
        let state = get_state(&self.workspaces, workspace)?;
        let fp = file_path.to_owned();

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            stage_single_file(&repo, &fp)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn stage_all(&self, workspace: &str) -> Result<(), AppError> {
        let state = get_state(&self.workspaces, workspace)?;

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            stage_all_with_deletions(&repo)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn unstage_file(&self, workspace: &str, file_path: &str) -> Result<(), AppError> {
        let state = get_state(&self.workspaces, workspace)?;
        let fp = file_path.to_owned();

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            unstage_single_file(&repo, &fp)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn unstage_all(&self, workspace: &str) -> Result<(), AppError> {
        let state = get_state(&self.workspaces, workspace)?;

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            unstage_all_files(&repo)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn discard_file(
        &self,
        workspace: &str,
        file_path: &str,
        operation: FileChangeOperation,
    ) -> Result<(), AppError> {
        let state = get_state(&self.workspaces, workspace)?;
        let fp = file_path.to_owned();

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            discard_single_file(&repo, &state.workspace_path, &fp, operation)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn reset_file(
        &self,
        workspace: &str,
        file_path: &str,
        operation: FileChangeOperation,
    ) -> Result<(), AppError> {
        let state = get_state(&self.workspaces, workspace)?;
        let fp = file_path.to_owned();

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            reset_single_file(&repo, &state.workspace_path, &fp, operation)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn get_branches(&self, workspace: &str) -> Result<Vec<String>, AppError> {
        let state = get_state(&self.workspaces, workspace)?;

        tokio::task::spawn_blocking(move || {
            let repo = open_repo(&state)?;
            list_branches(&repo)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
    }

    async fn dispose(&self, workspace: &str) -> Result<(), AppError> {
        let state = match self.workspaces.remove(workspace) {
            Some((_, s)) => s,
            // Already disposed or never initialized -- idempotent
            None => return Ok(()),
        };

        if state.mode == SnapshotMode::Snapshot {
            let repo_path = state.repo_path.clone();
            tokio::task::spawn_blocking(move || {
                if repo_path.exists() {
                    std::fs::remove_dir_all(&repo_path).map_err(|e| {
                        AppError::Internal(format!("Failed to remove snapshot dir {}: {}", repo_path.display(), e))
                    })?;
                }
                Ok(())
            })
            .await
            .map_err(|e| AppError::Internal(format!("Blocking task failed: {}", e)))?
        } else {
            // git-repo mode: nothing to clean up
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests (Task 14.2)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::ISnapshotService;
    use git2::{Repository, Signature};
    use std::path::Path;

    /// Init a git repo at `path` with an initial commit that tracks `file`
    /// with `content`.
    fn init_repo_with_file(path: &Path, file: &str, content: &str) {
        std::fs::write(path.join(file), content).unwrap();
        let repo = Repository::init(path).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new(file)).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = Signature::now("seed", "seed@test").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[]).unwrap();
    }

    #[tokio::test]
    async fn narrow_revert_restores_modified_file_without_affecting_unrelated() {
        let tmp = tempfile::tempdir().unwrap();
        init_repo_with_file(tmp.path(), "target.txt", "v0");
        std::fs::write(tmp.path().join("unrelated.txt"), "u0").unwrap();
        // Commit unrelated.txt so both files are tracked at HEAD.
        {
            let repo = Repository::open(tmp.path()).unwrap();
            let mut index = repo.index().unwrap();
            index.add_path(Path::new("unrelated.txt")).unwrap();
            index.write().unwrap();
            let tree_oid = index.write_tree().unwrap();
            let tree = repo.find_tree(tree_oid).unwrap();
            let sig = Signature::now("seed", "seed@test").unwrap();
            let head_oid = repo.head().unwrap().target().unwrap();
            let parent = repo.find_commit(head_oid).unwrap();
            repo.commit(
                Some("HEAD"),
                &sig,
                &sig,
                "add unrelated",
                &tree,
                &[&parent],
            )
            .unwrap();
        }

        let svc = SnapshotService::new();
        let ws = tmp.path().to_str().unwrap();
        svc.init(ws).await.unwrap();

        // Simulate a tool call that modifies BOTH files.
        std::fs::write(tmp.path().join("target.txt"), "v1-after-tool").unwrap();
        std::fs::write(tmp.path().join("unrelated.txt"), "u1-after-tool").unwrap();

        // Commit a snapshot of ONLY target.txt -- this is what the per-tool-call
        // hook will do.
        let sha = svc
            .commit_tool_snapshot("call-1", &["target.txt".to_string()])
            .await
            .unwrap();
        assert!(!sha.is_empty());
        assert_eq!(sha.len(), 40);

        // Modify unrelated.txt AFTER the tool-call commit. This simulates a
        // later, unrelated tool call.
        std::fs::write(tmp.path().join("unrelated.txt"), "u2-later-change").unwrap();

        // Revert ONLY target.txt from the captured commit.
        svc.revert_to_tool_snapshot(&sha, &["target.txt".to_string()])
            .await
            .unwrap();

        // target.txt should be restored to the pre-tool-call state (v0).
        let target_after = std::fs::read_to_string(tmp.path().join("target.txt")).unwrap();
        assert_eq!(target_after, "v0");

        // unrelated.txt must be untouched by the narrow revert -- it should
        // still reflect the post-commit modification (u2-later-change).
        let unrelated_after = std::fs::read_to_string(tmp.path().join("unrelated.txt")).unwrap();
        assert_eq!(unrelated_after, "u2-later-change");
    }
}
