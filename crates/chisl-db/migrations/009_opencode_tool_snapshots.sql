-- Migration 009: Per-tool-call snapshot ledger for OpenCode sessions.
--
-- Each row records a Git commit captured immediately AFTER a tool call
-- mutated the user's working tree. The `commit_sha` lets the snapshot
-- service fast-forward or revert back to that exact point; the
-- `files_changed_json` blob stores the list of touched paths (so the UI
-- can render a per-tool summary without a `git show` round-trip).
--
-- Rows are cascade-deleted with their parent conversation so a workspace
-- reset also wipes the snapshot history.
CREATE TABLE IF NOT EXISTS opencode_tool_snapshots (
    tool_call_id       TEXT    PRIMARY KEY NOT NULL,
    conversation_id    TEXT    NOT NULL,
    commit_sha         TEXT    NOT NULL,
    files_changed_json TEXT    NOT NULL,
    created_at         INTEGER NOT NULL,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_opencode_tool_snapshots_conv_created
    ON opencode_tool_snapshots(conversation_id, created_at);
