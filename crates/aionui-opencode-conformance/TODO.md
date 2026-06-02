# OpenCode SSE Protocol — Open Work

Newest-first scratchpad for events and protocol questions the conformance suite has surfaced but has not yet promoted to a dispatch arm. Each entry is one of:

- **`unobserved-in-capture-2026-06-02`** — listed in the v1/v2 SDK `Event` union but not emitted by the live `opencode serve` (binary `1.15.13`) we captured against. Recorded here so promotion is a follow-up commit, not a protocol surprise.
- **`KNOWN_IGNORED rationale`** — pinned text from `agent.rs:243-271` so reviewers can decide when to promote an ignored event to a real dispatch arm.
- **`open question`** — protocol questions T0 (`Projects/CodexClaudeParity/T0-protocol-conformance.md`) flagged for upstream / future work.

When a row is resolved (event promoted, fixture captured, or question answered), strike it and add a dated note. Do not delete entries.

---

## 1. `KNOWN_IGNORED_EVENTS` — pinned rationale

Source: `crates/aionui-ai-agent/src/manager/remote/agent.rs:243-271` doc-comment. Membership only changes the log level (`trace` vs `debug`); it never alters stream behaviour.

### 1a. Server / global scoped (per-conversation manager would fan out N-fold)

| Event | Why ignored today | Promotion candidate? |
| --- | --- | --- |
| `server.connected` | Lifecycle hook — flips `connection_status` in `run_event_reader` (`agent.rs:523`) and resets reconnect backoff. No per-conversation surface. | No — wired at the reader layer where it belongs. |
| `server.heartbeat` | Liveness only; resets the 30 s `HEARTBEAT_TIMEOUT` in `run_event_reader` (`agent.rs:484`). | No. |
| `server.instance.disposed` | Reader-terminal; handled at `agent.rs:528`. | No. |
| `global.disposed` | Fired alongside `server.instance.disposed` during global shutdown. | No. |
| `account.added` / `removed` / `switched` | Account management is server-owned; the client surfaces credentials through its own registry. | Only if Phase 3.1 surfaces a server-driven account banner. |
| `file.edited` | Per-conversation managers would fan out N-fold. | Phase 4.x — promote to a workspace-scoped fan-in surface. |
| `file.watcher.updated` | Same fan-out concern. | Same. |
| `command.executed` | Slash-command echo; the originating manager already has client-side state. | Phase 1.3 (`opencode-17`) if cross-client `/` echoes need rendering. |
| `lsp.updated` / `lsp.client.diagnostics` | Server-side LSP; renderer LSP bridge lives in Phase 4.2. | Phase 4.2. |
| `mcp.tools.changed` | Tool catalog rediscovery already lives in `opencode_mcp.rs`. | Phase 1.2 `T1-permission-ux` may consume this for per-tool prompts. |
| `mcp.browser.open.failed` | Surface text-only; not currently actionable. | If MCP OAuth UX (Phase 3.1) wants a toast. |
| `project.updated` | Not in `KNOWN_IGNORED_EVENTS` today; falls through to the `debug` fingerprint path. **Promote candidate** — see row 2 below. | See row 2. |
| `vcs.branch.updated` | Header chip is renderer-derived; no need to react server-side per conversation. | Phase 2.x if a branch-switch banner is added. |
| `workspace.failed` / `ready` / `status` | Workspace lifecycle owned by the server-side workspace registry; no Chisl surface yet. | Phase 3.x. |
| `worktree.failed` / `ready` | Worktree lifecycle. | Phase 3.2 `T3-worktree-mcp-sandbox`. |
| `pty.created` / `updated` / `exited` / `deleted` | Server-managed PTY lifecycle; Chisl uses its own node-pty terminal + the watchable PTY MCP (Phase 4.3). | Only if the Phase 4.3 design picks up the server channel. |
| `tui.*` | TUI remote control is out of scope for Chisl. | No. |

### 1b. V2 streaming mirrors of the `message.part.*` path

Listed in `KNOWN_IGNORED_EVENTS` because they duplicate information the dispatcher already consumes through `message.part.updated` / `message.part.delta` / `message.updated`. Handling them too would double-process every turn.

| Event family | Mirrored by |
| --- | --- |
| `session.next.prompted` / `synthetic` / `retried` | `message.part.updated` for the user/synthetic/retry parts. Retry surface is the Phase 1.1 `opencode-05` follow-up. |
| `session.next.step.started` / `ended` / `failed` | `message.part.updated` with `part.type == "step-start"` / `"step-finish"`. |
| `session.next.text.started` / `delta` / `ended` | `message.part.delta` (`field == "text"`) + `message.part.updated` for the text part. |
| `session.next.reasoning.started` / `delta` / `ended` | `message.part.delta` (`field == "reasoning"`) + `message.part.updated` (`part.type == "reasoning"`). |
| `session.next.shell.started` / `ended` | `message.part.updated` carrying the shell tool's tool part. |
| `session.next.tool.called` / `success` / `failed` | `message.part.updated` with `part.type == "tool"` and per-state metadata. |
| `session.next.compaction.started` / `delta` / `ended` | `session.compacted` + `message.part.updated` (`part.type == "compaction"`). |

### 1c. Session-scoped feature stubs delegated to later plans

| Event | Owner plan |
| --- | --- |
| `session.updated` | M06 — title sync. Listed in `KNOWN_IGNORED_EVENTS`. |
| `session.deleted` | M06. |
| `session.diff` | M05 — on-demand diff view fetches via `routes_aux.rs:37`. |
| `session.created` (root case) | Quietly acknowledged; the child case is the registration trigger at `agent.rs:1489`. |
| `message.removed` / `message.part.removed` | M07 (`opencode-08`) — renderer-side reconciliation. |
| `question.*` | M09 — fully handled today at `agent.rs:2106`/`:2160`. |

---

## 2. Events observed in capture-2026-06-02 that are **not** in `KNOWN_IGNORED_EVENTS`

Each of these has a recorded fixture but falls through to the `debug` fingerprint path in `agent.rs:2286`. Either promote to `KNOWN_IGNORED_EVENTS` (cheap, silences the diagnostic) or wire a dispatch arm.

- `project.updated` *(v2)* — emitted once at session create. Currently logged as "Unhandled OpenCode event". **Recommended:** add to `KNOWN_IGNORED_EVENTS` (no per-conversation action) unless a Phase 3.x project-banner surface needs it. Fixture: `fixtures/project.updated.jsonl`.

Promotion is a follow-up commit; Phase 0 deliberately does not implement handlers for newly-discovered events (master plan §6 Phase 0 acceptance).

---

## 3. Events in the v1/v2 SDK `Event` union but `unobserved-in-capture-2026-06-02`

Reason: the live capture session exercised text streaming, thinking, bash/write/grep tool calls, todo writes, permission ask + reply, compact, abort, session delete, and server disposal — but did not exercise these surfaces. They remain part of the protocol contract; promotion requires capturing them against the relevant server feature.

### V2-only events (in `dist/v2/gen/types.gen.d.ts`)

- `EventGlobalDisposed` *(observed at shutdown, captured as `global.disposed` — actually OK; tracked above)*.
- `EventQuestionAsked` / `EventQuestionReplied` / `EventQuestionRejected` — requires an agent that uses the `ask` tool. The dispatcher is fully wired (`agent.rs:2106`/`:2160`); add fixtures from a session that exercises the question flow.
- `EventMcpToolsChanged` — requires a runtime MCP server registration. Capture via `POST /mcp/{name}/connect` against the live server.
- `EventMcpBrowserOpenFailed` — requires an MCP OAuth flow with a broken browser open. Synthetic capture path TBD.
- `EventWorkspaceReady` / `WorkspaceFailed` / `WorkspaceStatus` — multi-workspace surface; the capture session ran with a single directory.
- `EventWorktreeReady` / `WorktreeFailed` — requires a worktree-aware server config.
- `EventCatalogModelUpdated` — emitted on `models.dev` refresh or an explicit model update. The dispatcher invalidates `model_context_limits` (`agent.rs:2191`); capture during the next models.dev refresh.
- `EventModelsDevRefreshed` — same source.
- `EventAccountAdded` / `EventAccountRemoved` / `EventAccountSwitched` — emitted on `opencode auth` flows. Capture during a fresh provider login.
- `EventSessionNextPrompted` — observed only when a model emits a prompted-text mirror; the capture model used `message.part.*` instead.
- `EventSessionNextSynthetic` — synthetic-text mirror; same shape concern.
- `EventSessionNextRetried` — requires a model that triggers a recoverable retry. Capture via a synthetic outage (e.g. by misconfiguring the provider during a turn).
- `EventSessionNextShellStarted` / `ShellEnded` — requires the v2 shell-streaming path; the capture model routed shell through the bash tool's `message.part.updated`.
- `EventSessionNextStepStarted` / `StepEnded` / `StepFailed` — same v2-streaming-path concern.
- `EventSessionNextTextStarted` / `TextDelta` / `TextEnded` — same.
- `EventSessionNextReasoningStarted` / `ReasoningDelta` / `ReasoningEnded` — same.
- `EventSessionNextToolCalled` / `ToolSuccess` / `ToolFailed` — same.
- `EventSessionNextCompactionStarted` / `CompactionDelta` / `CompactionEnded` — capture during a manual `/compact`.

### V1-only events (in `dist/gen/types.gen.d.ts`)

- `EventPermissionUpdated` — superseded by `permission.asked` in v2. Falls through to the `debug` fingerprint path today. Promote only if a pre-v2 server target is reintroduced.
- `EventInstallationUpdated` / `EventInstallationUpdateAvailable` — handled at `agent.rs:2236`, fixture not yet captured (requires an in-progress server upgrade).
- `EventMessageRemoved` / `EventMessagePartRemoved` — in `KNOWN_IGNORED_EVENTS`; fixture deferred to M07.

### Synthetic / vendor-specific

- `skill.updated` — emitted by 1.15.x but not in the SDK `Event` union. The dispatcher invalidates the per-manager skill cache at `agent.rs:2252`. Capture against a live `SKILL.md` write.

---

## 4. Open questions from `T0-protocol-conformance.md`

1. **Version handshake.** OpenCode's `GET /global/health` returns `{ healthy, version }`. The SSE handshake (`server.connected`) does NOT include a version. Decide whether to tag the conformance suite with a hard upper-bound version (and what to do when an upstream PR widens the union). File an upstream request if a stable version field on `server.connected` becomes a release blocker.

2. **Cost distinct from tokens.** `session.next.step.ended` carries `cost: number` alongside the `tokens` block; the canonical `AssistantMessage` exposes both. No distinct `cost.updated` event is observed in capture-2026-06-02 — Chisl computes the rolling figure from `Session.cost` updates in `session.updated`. If a future server emits a dedicated cost event, add a fixture + handler.

---

## 5. V2 sync mirror channel

The 1.15.x server emits a parallel "sync" channel: every canonical event is also wrapped as `{type:"sync", syncEvent:{type:"<name>.1", id, seq, aggregateID, data}, id}`. `seq` is a monotonically increasing per-aggregate ordering hint. **`seq` is not a resume cursor** — no `?since=seq` endpoint exists upstream; reconnect remains full re-subscribe + `GET /session/{id}/message?limit=N` backfill.

The conformance suite pins this shape (`fixtures/_sync.jsonl`) so a future "v2 cursor replay" promotion can land without protocol surprises. **Phase 0 explicitly does not add cursor-based replay logic** — see master plan §10 Validation Correction #1.
