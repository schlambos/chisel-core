-- Migration 011: Inverse patches for per-tool-call revert.
-- Each row stores the inverse of a completed tool call's patch so the
-- working tree can be restored to its pre-call state. The `patch` column
-- is the unified diff the tool applied; `inverse_patch` is its inverse.
-- Rows are cascade-deleted with their parent conversation.
CREATE TABLE IF NOT EXISTS edit_inverses (
    tool_call_id      TEXT    PRIMARY KEY NOT NULL,
    conversation_id   TEXT    NOT NULL,
    file_path         TEXT    NOT NULL,
    patch             TEXT    NOT NULL,
    inverse_patch     TEXT    NOT NULL,
    base_hash         TEXT    NOT NULL,
    created_at        INTEGER NOT NULL,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_edit_inverses_conv_created
    ON edit_inverses(conversation_id, created_at);
