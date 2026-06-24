-- C04: tool-host mode for remote OpenCode agents.
--
-- "local"  (default): Chisl injects a local-fs MCP and denies the server's
--           built-in tools, so the agent operates on the user's local files.
-- "server": no local-fs MCP, no pre-deny; the agent uses the OpenCode server's
--           own tools against the server's working tree, with permission
--           prompts flowing through the normal `permission.asked` handler.
--
-- Default "local" preserves existing behaviour for every current row.
ALTER TABLE remote_agents ADD COLUMN tool_host TEXT NOT NULL DEFAULT 'local';
