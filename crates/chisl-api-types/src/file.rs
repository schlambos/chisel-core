use chisl_common::FileChangeOperation;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// A. Core file operations — Request DTOs
// ---------------------------------------------------------------------------

/// Request body for `POST /api/fs/dir` — get files by directory.
#[derive(Debug, Deserialize)]
pub struct GetFilesByDirRequest {
    pub dir: String,
    pub root: String,
}

/// Request body for `POST /api/fs/list` — list workspace files.
#[derive(Debug, Deserialize)]
pub struct ListWorkspaceFilesRequest {
    pub root: String,
}

/// Request body for `POST /api/fs/metadata` — get file metadata.
#[derive(Debug, Deserialize)]
pub struct GetFileMetadataRequest {
    pub path: String,
    #[serde(default)]
    pub workspace: Option<String>,
}

/// Request body for `POST /api/fs/read` — read file.
#[derive(Debug, Deserialize)]
pub struct ReadFileRequest {
    pub path: String,
    #[serde(default)]
    pub workspace: Option<String>,
}

/// Request body for `POST /api/fs/read-buffer` — read file as binary.
#[derive(Debug, Deserialize)]
pub struct ReadFileBufferRequest {
    pub path: String,
    #[serde(default)]
    pub workspace: Option<String>,
}

/// Request body for `POST /api/fs/write` — write file.
#[derive(Debug, Deserialize)]
pub struct WriteFileRequest {
    pub path: String,
    pub data: String,
    /// Workspace root, used to compute `relativePath` in the
    /// `fileStream.contentUpdate` event.  Falls back to the file's
    /// parent directory when absent.
    #[serde(default)]
    pub workspace: Option<String>,
}

/// Request body for `POST /api/fs/copy` — copy files to workspace.
#[derive(Debug, Deserialize)]
pub struct CopyFilesRequest {
    pub file_paths: Vec<String>,
    pub workspace: String,
    #[serde(default)]
    pub source_root: Option<String>,
}

/// Request body for `POST /api/fs/remove` — remove file or directory.
#[derive(Debug, Deserialize)]
pub struct RemoveEntryRequest {
    pub path: String,
    /// Workspace root, used to compute `relativePath` in the
    /// `fileStream.contentUpdate` event.  Falls back to the file's
    /// parent directory when absent.
    #[serde(default)]
    pub workspace: Option<String>,
}

/// Request body for `POST /api/fs/rename` — rename file or directory.
#[derive(Debug, Deserialize)]
pub struct RenameRequest {
    pub path: String,
    pub new_name: String,
}

/// Request body for `POST /api/fs/temp` — create temp file.
#[derive(Debug, Deserialize)]
pub struct CreateTempFileRequest {
    pub file_name: String,
}

/// Request body for `POST /api/fs/image-base64` — get image as base64.
#[derive(Debug, Deserialize)]
pub struct GetImageBase64Request {
    pub path: String,
    #[serde(default)]
    pub workspace: Option<String>,
}

/// Request body for `POST /api/fs/fetch-remote-image` — fetch remote image.
#[derive(Debug, Deserialize)]
pub struct FetchRemoteImageRequest {
    pub url: String,
}

/// A single entry in a ZIP creation request.
#[derive(Debug, Clone, Deserialize)]
pub struct ZipFileEntry {
    pub name: String,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub file_path: Option<String>,
}

/// Request body for `POST /api/fs/zip` — create ZIP archive.
#[derive(Debug, Deserialize)]
pub struct ZipRequest {
    pub path: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub files: Vec<ZipFileEntry>,
}

/// Request body for `POST /api/fs/zip/cancel` — cancel ZIP creation.
#[derive(Debug, Deserialize)]
pub struct CancelZipRequest {
    pub request_id: String,
}

/// Query parameters for `GET /api/fs/browse` — shallow directory browser.
///
/// Unlike `/api/fs/dir` (which returns a recursive tree scoped to a workspace
/// root), `browse` is a WebUI-only host-file picker: it lists a single
/// directory level, surfaces navigation hints (`can_go_up`, `parent_path`),
/// and on Windows supports a `__ROOT__` sentinel for the drive-list screen.
#[derive(Debug, Deserialize)]
pub struct BrowseDirectoryQuery {
    /// Directory to list. Empty string means "use default" (Windows: drive
    /// list; Unix: current working directory). `"__ROOT__"` on Windows is
    /// treated the same as an empty path.
    #[serde(default)]
    pub path: Option<String>,
    /// When true, include regular files in the response. Defaults to false
    /// (directories only).
    #[serde(default)]
    pub show_files: Option<String>,
}

/// A single entry in a `/api/fs/browse` response.
///
/// Uses camelCase on the wire to match the original Express contract the
/// frontend `DirectorySelectionModal` still consumes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BrowseEntry {
    pub name: String,
    pub path: String,
    pub is_directory: bool,
    pub is_file: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Last-modified time as milliseconds since the unix epoch. Absent when
    /// the entry has no readable metadata (e.g. a Windows drive stub).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified: Option<i64>,
}

/// Response body for `GET /api/fs/browse`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct BrowseDirectoryResponse {
    /// The resolved directory currently being listed. Empty string when the
    /// response is a Windows drive-list screen.
    pub current_path: String,
    /// Path to navigate to when the user clicks "up". `None` when already at
    /// the root. Value `"__ROOT__"` is a sentinel used on Windows to mean
    /// "return to the drive-list screen".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_path: Option<String>,
    pub items: Vec<BrowseEntry>,
    pub can_go_up: bool,
    pub truncated: bool,
    /// True when the response represents the Windows drive-list screen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_root: Option<bool>,
}

// ---------------------------------------------------------------------------
// A. Core file operations — Response DTOs
// ---------------------------------------------------------------------------

/// A node in the directory tree returned by `getFilesByDir`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirOrFileResponse {
    pub name: String,
    pub full_path: String,
    pub relative_path: String,
    pub is_dir: bool,
    pub is_file: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<DirOrFileResponse>>,
}

/// A flat file entry returned by `listWorkspaceFiles`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkspaceFlatFileResponse {
    pub name: String,
    pub full_path: String,
    pub relative_path: String,
}

/// File metadata returned by `getFileMetadata`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadataResponse {
    pub name: String,
    pub path: String,
    pub size: u64,
    #[serde(rename = "type")]
    pub mime_type: String,
    pub last_modified: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_directory: Option<bool>,
}

/// Result of a batch copy operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyFilesResponse {
    pub copied_files: Vec<String>,
    pub failed_files: Vec<String>,
}

/// Result of a rename operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameResponse {
    pub new_path: String,
}

// ---------------------------------------------------------------------------
// D. File watch — Request DTOs
// ---------------------------------------------------------------------------

/// Request body for `POST /api/fs/watch/start` and `/stop`.
#[derive(Debug, Deserialize)]
pub struct FileWatchRequest {
    pub file_path: String,
}

/// Request body for `POST /api/fs/office-watch/start` and `/stop`.
#[derive(Debug, Deserialize)]
pub struct WorkspaceOfficeWatchRequest {
    pub workspace: String,
}

// ---------------------------------------------------------------------------
// E. Workspace snapshot — Request DTOs
// ---------------------------------------------------------------------------

/// Request body for snapshot init / getInfo / compare / stageAll / unstageAll / dispose.
#[derive(Debug, Deserialize)]
pub struct SnapshotWorkspaceRequest {
    pub workspace: String,
}

/// Request body for snapshot getBaselineContent.
#[derive(Debug, Deserialize)]
pub struct SnapshotBaselineRequest {
    pub workspace: String,
    pub file_path: String,
}

/// Request body for snapshot stageFile / unstageFile.
#[derive(Debug, Deserialize)]
pub struct SnapshotStageRequest {
    pub workspace: String,
    pub file_path: String,
}

/// Request body for snapshot discardFile / resetFile.
#[derive(Debug, Deserialize)]
pub struct SnapshotDiscardRequest {
    pub workspace: String,
    pub file_path: String,
    pub operation: FileChangeOperation,
}

// ---------------------------------------------------------------------------
// E. Workspace snapshot — Response DTOs
// ---------------------------------------------------------------------------

/// Snapshot mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SnapshotMode {
    GitRepo,
    Snapshot,
}

/// Information about a workspace snapshot.
///
/// API Spec: `branch: string | null` — always present in JSON output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfoResponse {
    pub mode: SnapshotMode,
    pub branch: Option<String>,
}

/// A single file change entry in a compare result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileChangeInfoResponse {
    pub file_path: String,
    pub relative_path: String,
    pub operation: FileChangeOperation,
}

/// Result of comparing workspace changes (staged vs unstaged).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotCompareResponse {
    pub staged: Vec<FileChangeInfoResponse>,
    pub unstaged: Vec<FileChangeInfoResponse>,
}

/// One file's unified diff against the snapshot baseline (Task 18).
///
/// `patch` is a standard `git diff`-style unified-diff string
/// (`--- a/<path>\n+++ b/<path>\n@@ …`), exactly what a code review tool
/// expects. `additions` and `deletions` are line counts (excluding the
/// diff's own `@@`/`---`/`+++` headers), suitable for the
/// "N files · +X / -Y lines" summary chip from Task 17. `operation`
/// mirrors `FileChangeOperation` so the caller can render a "created" /
/// "modified" / "deleted" label without parsing the patch text. Stored
/// as the `Debug` string (`"Create" | "Modify" | "Delete"`) so the
/// response is decoupled from the `chisl-common` enum's
/// `lowercase` serde rename rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileDiffEntryResponse {
    pub relative_path: String,
    pub patch: String,
    pub additions: u32,
    pub deletions: u32,
    pub operation: String,
}

/// Response body for `POST /api/fs/snapshot/diff` and the wrapper
/// returned by `GET /api/conversations/{id}/workspace/diff` (Task 18).
/// A clean tree surfaces as `{ "files": [] }`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceDiffResponse {
    pub files: Vec<FileDiffEntryResponse>,
}

/// Response body for `GET /api/conversations/{id}/workspace/vcs` (Task 18.1).
///
/// `mode` is `"git"` when the workspace is a tracked git repo and
/// `"not-git"` when no `.git` directory is present. `is_tracked` mirrors
/// `mode == "git"` for typed callers that don't want to compare strings.
/// `summary` rolls up `files_changed`, `additions`, and `deletions` across
/// the whole `patches` list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceVcsResponse {
    pub mode: String,
    pub is_tracked: bool,
    pub summary: WorkspaceVcsSummary,
    pub patches: Vec<FileDiffEntryResponse>,
}

/// Roll-up of `WorkspaceVcsResponse::patches`. Counts are line totals
/// (not files-of-additions); the UI shows them in the "N files · +X / -Y
/// lines" header chip.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkspaceVcsSummary {
    pub files_changed: usize,
    pub additions: usize,
    pub deletions: usize,
}

// ---------------------------------------------------------------------------
// K. Read-only tool-call restore plan (forge-5-02-03)
// ---------------------------------------------------------------------------
//
// Response body for
// `GET /api/conversations/{id}/opencode/tool-call-restore-plan?tool_call_id=…`.
//
// Read-only preview over the existing `opencode_tool_snapshots` ledger
// row; the route does not mutate the working tree. Mirrors the
// `chisl_file::snapshot_service::restore_plan` types but lives here so
// the API contract is decoupled from the file-crate's internal
// representation (a future change to the git internals will not ripple
// into the wire contract).

/// Operation that a per-path restore would perform on the working tree.
/// Serialised as a `snake_case` string for stability across the
/// `chisl-common` `lowercase` rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestorePathOperation {
    Create,
    Modify,
    Delete,
    Unknown,
}

/// Per-path entry inside a tool-call restore plan. Surface for the UI
/// preview so the user can see exactly what a future revert would do
/// per file before committing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestorePathEntryResponse {
    pub path: String,
    pub operation: RestorePathOperation,
    /// `true` when pre-tool content can be recovered from the parent of
    /// `commit_sha` (e.g. a `Modify` whose parent blob is readable).
    pub prior_content_restorable: bool,
    /// `true` when prior content exists but is not UTF-8 text — the
    /// preview pane should render a binary marker rather than the
    /// decoded bytes.
    pub preview_blocked: bool,
    /// Parent commit of the tool-call snapshot (baseline for narrow
    /// revert). `None` when the tool-call commit has no parent.
    pub source_commit_sha: Option<String>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

/// Response body for
/// `GET /api/conversations/{id}/opencode/tool-call-restore-plan`.
///
/// `found` is `false` when no ledger row exists for the requested
/// `tool_call_id` in this conversation (the UI shows a friendly
/// "no snapshot" state instead of a 404). When `found` is `true`,
/// `plan` carries the per-path preview and `actionable` is `true` only
/// when every entry has zero blocking errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRestorePlanResponse {
    pub tool_call_id: String,
    pub found: bool,
    pub plan: Option<ToolCallRestorePlanDetail>,
    pub actionable: bool,
    pub unsupported_coverage: RestorePlanUnsupportedCoverage,
}

/// Inner plan body. Kept separate from the response wrapper so the UI
/// can type the optional `plan` field without having to re-declare
/// every key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRestorePlanDetail {
    pub commit_sha: String,
    pub paths: Vec<RestorePathEntryResponse>,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

/// Tools and mutation surfaces **not** covered by the per-tool-call
/// ledger in this slice. Surfaced so the UI can render an explicit
/// "this plan does not cover …" panel rather than over-promising what
/// a restore will undo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestorePlanUnsupportedCoverage {
    /// `run_shell` post-exec deltas are not attributed to tool_call_id
    /// without live shell tracking.
    pub run_shell_not_snapshotted: bool,
    /// Mutations outside `local_fs_mcp` (other MCP bridges, OpenCode
    /// built-ins) are not ledgered.
    pub non_local_fs_mcp_not_covered: bool,
    /// OpenCode conversation/session revert is a separate surface; this
    /// plan is filesystem-only.
    pub opencode_session_revert_not_used: bool,
}

impl Default for RestorePlanUnsupportedCoverage {
    fn default() -> Self {
        Self {
            run_shell_not_snapshotted: true,
            non_local_fs_mcp_not_covered: true,
            opencode_session_revert_not_used: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -- Request deserialization tests --

    #[test]
    fn get_files_by_dir_request_deserialization() {
        let raw = r#"{"dir":"/home/user/project","root":"/home/user"}"#;
        let req: GetFilesByDirRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.dir, "/home/user/project");
        assert_eq!(req.root, "/home/user");
    }

    #[test]
    fn copy_files_request_snake_case() {
        let raw = json!({
            "file_paths": ["/a.txt", "/b.txt"],
            "workspace": "/ws",
            "source_root": "/src"
        });
        let req: CopyFilesRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.file_paths, vec!["/a.txt", "/b.txt"]);
        assert_eq!(req.workspace, "/ws");
        assert_eq!(req.source_root.as_deref(), Some("/src"));
    }

    #[test]
    fn copy_files_request_optional_source_root() {
        let raw = json!({
            "file_paths": ["/a.txt"],
            "workspace": "/ws"
        });
        let req: CopyFilesRequest = serde_json::from_value(raw).unwrap();
        assert!(req.source_root.is_none());
    }

    #[test]
    fn rename_request_snake_case() {
        let raw = r#"{"path":"/ws/old.txt","new_name":"new.txt"}"#;
        let req: RenameRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.path, "/ws/old.txt");
        assert_eq!(req.new_name, "new.txt");
    }

    #[test]
    fn zip_request_snake_case() {
        let raw = json!({
            "path": "/out.zip",
            "request_id": "req-1",
            "files": [
                { "name": "a.txt", "content": "hello" },
                { "name": "b.bin", "file_path": "/src/b.bin" }
            ]
        });
        let req: ZipRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.path, "/out.zip");
        assert_eq!(req.request_id.as_deref(), Some("req-1"));
        assert_eq!(req.files.len(), 2);
        assert_eq!(req.files[0].content.as_deref(), Some("hello"));
        assert!(req.files[0].file_path.is_none());
        assert!(req.files[1].content.is_none());
        assert_eq!(req.files[1].file_path.as_deref(), Some("/src/b.bin"));
    }

    #[test]
    fn zip_request_optional_request_id() {
        let raw = json!({
            "path": "/out.zip",
            "files": [{ "name": "a.txt", "content": "x" }]
        });
        let req: ZipRequest = serde_json::from_value(raw).unwrap();
        assert!(req.request_id.is_none());
    }

    #[test]
    fn file_watch_request_snake_case() {
        let raw = r#"{"file_path":"/path/to/file.txt"}"#;
        let req: FileWatchRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.file_path, "/path/to/file.txt");
    }

    #[test]
    fn snapshot_discard_request_deserialization() {
        let raw = json!({
            "workspace": "/ws",
            "file_path": "src/main.rs",
            "operation": "modify"
        });
        let req: SnapshotDiscardRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(req.workspace, "/ws");
        assert_eq!(req.file_path, "src/main.rs");
        assert_eq!(req.operation, FileChangeOperation::Modify);
    }

    // -- Response serialization tests --

    #[test]
    fn dir_or_file_response_serialization() {
        let resp = DirOrFileResponse {
            name: "src".into(),
            full_path: "/project/src".into(),
            relative_path: "src".into(),
            is_dir: true,
            is_file: false,
            children: Some(vec![DirOrFileResponse {
                name: "main.rs".into(),
                full_path: "/project/src/main.rs".into(),
                relative_path: "src/main.rs".into(),
                is_dir: false,
                is_file: true,
                children: None,
            }]),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["name"], "src");
        assert_eq!(json["full_path"], "/project/src");
        assert_eq!(json["relative_path"], "src");
        assert_eq!(json["is_dir"], true);
        assert_eq!(json["is_file"], false);
        assert_eq!(json["children"][0]["name"], "main.rs");
    }

    #[test]
    fn dir_or_file_response_no_children_omitted() {
        let resp = DirOrFileResponse {
            name: "file.txt".into(),
            full_path: "/file.txt".into(),
            relative_path: "file.txt".into(),
            is_dir: false,
            is_file: true,
            children: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json.get("children").is_none());
    }

    #[test]
    fn workspace_flat_file_response_serialization() {
        let resp = WorkspaceFlatFileResponse {
            name: "lib.rs".into(),
            full_path: "/project/src/lib.rs".into(),
            relative_path: "src/lib.rs".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["name"], "lib.rs");
        assert_eq!(json["full_path"], "/project/src/lib.rs");
        assert_eq!(json["relative_path"], "src/lib.rs");
    }

    #[test]
    fn file_metadata_response_serialization() {
        let resp = FileMetadataResponse {
            name: "readme.md".into(),
            path: "/project/readme.md".into(),
            size: 1024,
            mime_type: "text/markdown".into(),
            last_modified: 1700000000000,
            is_directory: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["name"], "readme.md");
        assert_eq!(json["path"], "/project/readme.md");
        assert_eq!(json["size"], 1024);
        assert_eq!(json["type"], "text/markdown");
        assert_eq!(json["last_modified"], 1700000000000_i64);
        assert!(json.get("is_directory").is_none());
    }

    #[test]
    fn file_metadata_response_with_directory_flag() {
        let resp = FileMetadataResponse {
            name: "src".into(),
            path: "/project/src".into(),
            size: 0,
            mime_type: "".into(),
            last_modified: 1700000000000,
            is_directory: Some(true),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["is_directory"], true);
    }

    #[test]
    fn copy_files_response_serialization() {
        let resp = CopyFilesResponse {
            copied_files: vec!["/ws/a.txt".into()],
            failed_files: vec!["/missing.txt".into()],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["copied_files"][0], "/ws/a.txt");
        assert_eq!(json["failed_files"][0], "/missing.txt");
    }

    #[test]
    fn snapshot_mode_serialization() {
        assert_eq!(serde_json::to_value(SnapshotMode::GitRepo).unwrap(), "git-repo");
        assert_eq!(serde_json::to_value(SnapshotMode::Snapshot).unwrap(), "snapshot");
    }

    #[test]
    fn snapshot_mode_deserialization() {
        let mode: SnapshotMode = serde_json::from_str(r#""git-repo""#).unwrap();
        assert_eq!(mode, SnapshotMode::GitRepo);
        let mode: SnapshotMode = serde_json::from_str(r#""snapshot""#).unwrap();
        assert_eq!(mode, SnapshotMode::Snapshot);
    }

    #[test]
    fn snapshot_info_response_git_repo() {
        let resp = SnapshotInfoResponse {
            mode: SnapshotMode::GitRepo,
            branch: Some("main".into()),
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["mode"], "git-repo");
        assert_eq!(json["branch"], "main");
    }

    #[test]
    fn snapshot_info_response_snapshot_mode() {
        let resp = SnapshotInfoResponse {
            mode: SnapshotMode::Snapshot,
            branch: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["mode"], "snapshot");
        // API Spec: branch is always present, null when snapshot mode
        assert!(json["branch"].is_null());
    }

    #[test]
    fn snapshot_compare_response_serialization() {
        let resp = SnapshotCompareResponse {
            staged: vec![FileChangeInfoResponse {
                file_path: "/ws/a.txt".into(),
                relative_path: "a.txt".into(),
                operation: FileChangeOperation::Create,
            }],
            unstaged: vec![FileChangeInfoResponse {
                file_path: "/ws/b.txt".into(),
                relative_path: "b.txt".into(),
                operation: FileChangeOperation::Modify,
            }],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["staged"][0]["file_path"], "/ws/a.txt");
        assert_eq!(json["staged"][0]["relative_path"], "a.txt");
        assert_eq!(json["staged"][0]["operation"], "create");
        assert_eq!(json["unstaged"][0]["operation"], "modify");
    }

    #[test]
    fn snapshot_compare_response_deserialization() {
        let raw = json!({
            "staged": [
                { "file_path": "/ws/x.rs", "relative_path": "x.rs", "operation": "delete" }
            ],
            "unstaged": []
        });
        let resp: SnapshotCompareResponse = serde_json::from_value(raw).unwrap();
        assert_eq!(resp.staged.len(), 1);
        assert_eq!(resp.staged[0].operation, FileChangeOperation::Delete);
        assert!(resp.unstaged.is_empty());
    }

    // -- FileDiffEntryResponse / WorkspaceDiffResponse (Task 18) --

    #[test]
    fn file_diff_entry_response_round_trip() {
        let entry = FileDiffEntryResponse {
            relative_path: "src/main.rs".into(),
            patch: "--- a/src/main.rs\n+++ b/src/main.rs\n@@ -1 +1 @@\n-old\n+new\n".into(),
            additions: 1,
            deletions: 1,
            operation: "Modify".into(),
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["relative_path"], "src/main.rs");
        assert_eq!(json["additions"], 1);
        assert_eq!(json["deletions"], 1);
        assert_eq!(json["operation"], "Modify");
        assert!(json["patch"].as_str().unwrap().contains("--- a/src/main.rs"));

        let round: FileDiffEntryResponse = serde_json::from_value(json).unwrap();
        assert_eq!(round.relative_path, "src/main.rs");
        assert_eq!(round.additions, 1);
        assert_eq!(round.deletions, 1);
        assert_eq!(round.operation, "Modify");
    }

    #[test]
    fn workspace_diff_response_empty_is_valid() {
        let resp = WorkspaceDiffResponse { files: vec![] };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json["files"].is_array());
        assert_eq!(json["files"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn workspace_diff_response_deserialization() {
        let raw = json!({
            "files": [
                {
                    "relative_path": "a.txt",
                    "patch": "--- a/a.txt\n+++ b/a.txt\n@@ -1 +1 @@\n-x\n+y\n",
                    "additions": 1,
                    "deletions": 1,
                    "operation": "Modify"
                },
                {
                    "relative_path": "b.txt",
                    "patch": "--- /dev/null\n+++ b/b.txt\n@@ -0,0 +1 @@\n+brand new\n",
                    "additions": 1,
                    "deletions": 0,
                    "operation": "Create"
                }
            ]
        });
        let resp: WorkspaceDiffResponse = serde_json::from_value(raw).unwrap();
        assert_eq!(resp.files.len(), 2);
        assert_eq!(resp.files[0].relative_path, "a.txt");
        assert_eq!(resp.files[0].operation, "Modify");
        assert_eq!(resp.files[1].relative_path, "b.txt");
        assert_eq!(resp.files[1].operation, "Create");
    }

    // -- Restore plan (forge-5-02-03) -----------------------------------

    #[test]
    fn restore_path_operation_uses_snake_case() {
        assert_eq!(
            serde_json::to_value(RestorePathOperation::Create).unwrap(),
            json!("create")
        );
        assert_eq!(
            serde_json::to_value(RestorePathOperation::Modify).unwrap(),
            json!("modify")
        );
        assert_eq!(
            serde_json::to_value(RestorePathOperation::Delete).unwrap(),
            json!("delete")
        );
        assert_eq!(
            serde_json::to_value(RestorePathOperation::Unknown).unwrap(),
            json!("unknown")
        );
    }

    #[test]
    fn restore_plan_response_found_round_trips() {
        let resp = ToolCallRestorePlanResponse {
            tool_call_id: "tc-1".into(),
            found: true,
            actionable: true,
            plan: Some(ToolCallRestorePlanDetail {
                commit_sha: "abc".into(),
                paths: vec![RestorePathEntryResponse {
                    path: "a.txt".into(),
                    operation: RestorePathOperation::Modify,
                    prior_content_restorable: true,
                    preview_blocked: false,
                    source_commit_sha: Some("def".into()),
                    warnings: vec![],
                    errors: vec![],
                }],
                warnings: vec![],
                errors: vec![],
            }),
            unsupported_coverage: RestorePlanUnsupportedCoverage::default(),
        };
        let value = serde_json::to_value(&resp).unwrap();
        assert_eq!(value["tool_call_id"], "tc-1");
        assert_eq!(value["found"], true);
        assert_eq!(value["plan"]["paths"][0]["operation"], "modify");
        assert_eq!(value["unsupported_coverage"]["run_shell_not_snapshotted"], true);
    }

    #[test]
    fn restore_plan_response_not_found_has_no_plan() {
        let resp = ToolCallRestorePlanResponse {
            tool_call_id: "missing".into(),
            found: false,
            actionable: false,
            plan: None,
            unsupported_coverage: RestorePlanUnsupportedCoverage::default(),
        };
        let value = serde_json::to_value(&resp).unwrap();
        assert_eq!(value["found"], false);
        assert!(value["plan"].is_null());
    }
}
