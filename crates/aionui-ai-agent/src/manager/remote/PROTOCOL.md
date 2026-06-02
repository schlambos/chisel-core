# OpenCode SSE Protocol Surface (Chisl-pinned)

> **Status:** Phase 0 conformance pin
> **Last verified:** 2026-06-02 against `opencode-ai-sdk-1.15.11.tgz` (v1 + v2 unions) and a live `opencode serve` (binary version `1.15.13`).
> **Companion crate:** `aionui-opencode-conformance` (test-only; recorded JSONL fixtures + forward-compatible parser test, gated in CI by `.github/workflows/conformance.yml`).
> **Companion plan:** [`Plans/Projects/CodexClaudeParity/T0-protocol-conformance.md`](../../../../../../Plans/Projects/CodexClaudeParity/T0-protocol-conformance.md).

This document enumerates every SSE event Chisl's remote-OpenCode adapter must recognise. Each row is keyed by the event's discriminator (`type` value after `unwrap_event` strips the `payload` wrapper) and lists the JSON shape's required field set, the live-adapter handling site (file:line) or the reason it is intentionally unhandled, and a status. **This is the contract the CI conformance suite locks.** Adding an unknown event type or unknown field on a known event is forward-compatible (the test warns but does not fail); removing a required field from a known event is breaking (the test fails red).

## Conventions

- **Envelope wrappers.** `/global/event` wraps every event under `{directory, project, payload}` (and tags global-only events with `directory:"global"`). `/event` (legacy) emits the inner event raw. The adapter normalises both via `unwrap_event` (`agent.rs:233`) before dispatching. **All fixtures in `aionui-opencode-conformance/fixtures/` are stored in the post-unwrap shape** (`{id, type, properties}` or `{type:"sync", syncEvent:{...}, id}` for sync mirrors).
- **Event-path probe.** `resolve_event_path` (`agent.rs:553`) probes `GET /doc` once at connect time, prefers `/global/event` when listed, and only downgrades to `/event` when it can positively confirm the canonical path is absent.
- **Reader lifecycle.** `run_event_reader` (`agent.rs:455`) flips `connection_status` to `Connected` on the first `server.connected` (`agent.rs:523`) and short-circuits to `ReaderExit::ServerDisposed` on `server.instance.disposed` (`agent.rs:528`). Heartbeat-timeout / EOF / transport-error reconnect uses exponential backoff between `RECONNECT_DELAY_MIN` (250 ms) and `RECONNECT_DELAY_MAX` (5 s) (`agent.rs:384-386`); `HEARTBEAT_TIMEOUT` is 30 s (`agent.rs:397`).
- **V2 sync mirrors.** The 1.15.x server emits a parallel "sync" channel: every canonical event `X` is mirrored as `{type:"sync", syncEvent:{type:"X.1", id, seq, aggregateID, data}, id}`. The `seq` is a monotonically increasing per-aggregate ordering hint — **not** a resume cursor (no `?since=seq` endpoint exists upstream; reconnect is full re-subscribe + `GET /session/{id}/message?limit=N` backfill, `agent.rs:419` for child backfill). The adapter consumes the canonical path, so sync mirrors fall through to the unhandled-fingerprint debug path today. This is intentional Phase 0 scope; promoting sync consumption is a later phase.
- **Known-ignored events.** Events the dispatcher recognises but takes no action on are listed in `KNOWN_IGNORED_EVENTS` (`agent.rs:272`) and routed through `trace` so the global stream stays quiet. Genuinely-new event types fall through to a `debug` log with a non-sensitive property-key fingerprint (`event_property_fingerprint`, `agent.rs:350`).
- **Session-gate.** `handle_opencode_sse_event` (`agent.rs:1430`) gates every session-scoped event by `sessionID` against the owning session and its registered children. Events without a `sessionID` (`server.*`, `installation.*`, `account.*`, `global.disposed`, etc.) bypass the gate.
- **Status values:**
  - `handled` — has a dedicated dispatch arm that emits a canonical `AgentStreamEvent` or mutates manager state.
  - `partial` — recognised by the dispatcher (typically via `KNOWN_IGNORED_EVENTS`) but state mutation is delegated to a later plan; the JSON shape is still pinned here.
  - `ignored` — in `KNOWN_IGNORED_EVENTS` and intentionally silent today (rationale in `agent.rs:243-271` and per-row below).
  - `unhandled` — not recognised; falls through to the `debug` fingerprint path. Tracked in `aionui-opencode-conformance/TODO.md`.

---

## Reader-lifecycle events (no `sessionID`; bypass the session gate)

| Event | JSON shape (post-`unwrap_event`) | Handling site | Status |
| --- | --- | --- | --- |
| `server.connected` | `{ id, type:"server.connected", properties:{} }`. First event of every SSE pass. | `agent.rs:523` (flips `connection_status` to `Connected`; resets reconnect backoff). | handled |
| `server.heartbeat` | `{ id, type:"server.heartbeat", properties:{} }`. Observed in 1.15.13 capture but NOT in the v1/v2 SDK `Event` union — the SDK omits it because clients only need it for liveness. | Resets the 30 s `HEARTBEAT_TIMEOUT` in `run_event_reader` (`agent.rs:484`). Listed in `KNOWN_IGNORED_EVENTS` for dispatcher routing (`agent.rs:275`). | ignored (lifecycle-only) |
| `server.instance.disposed` | `{ id, type:"server.instance.disposed", properties:{ directory } }`. | `agent.rs:528` short-circuits the reader with `ReaderExit::ServerDisposed`; terminal — supervisor does not reconnect. Also listed in `KNOWN_IGNORED_EVENTS` (`agent.rs:276`) for the dispatcher fallback path. | handled |
| `global.disposed` | `{ id, type:"global.disposed", properties:{} }`. Fired alongside `server.instance.disposed` when the global process is winding down (1.15.13). | `KNOWN_IGNORED_EVENTS` (`agent.rs:277`). | ignored |

## Connection / installation / account / catalog (global; no `sessionID`)

| Event | JSON shape | Handling site | Status |
| --- | --- | --- | --- |
| `installation.updated` | `{ id, type, properties:{ version } }` | `agent.rs:2236` — `info!` log only; deliberately no per-conversation banner. | handled (info log) |
| `installation.update-available` | `{ id, type, properties:{ version } }` | Same arm as `installation.updated`. | handled (info log) |
| `account.added` | `{ id, type, properties:{ account: AccountV2Info } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:278`). Account management is server-owned; the client surfaces credentials through its own registry. | ignored |
| `account.removed` | `{ id, type, properties:{ account } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:279`). | ignored |
| `account.switched` | `{ id, type, properties:{ serviceID, from?, to? } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:280`). | ignored |
| `catalog.model.updated` | `{ id, type, properties:{ model: ModelV2Info } }` (v2) | `agent.rs:2191` — invalidates the per-manager `model_context_limits` cache. | handled |
| `models-dev.refreshed` | `{ id, type, properties:{} }` (v2) | Same arm as `catalog.model.updated`. | handled |
| `project.updated` | `{ id, type, properties: Project }` (v2; v1 has no equivalent) | Fall-through `debug` log today (not in `KNOWN_IGNORED_EVENTS`). Captured in conformance fixtures; promote when a project-aware surface needs it. | unhandled (logged) |
| `lsp.client.diagnostics` | `{ id, type, properties:{ serverID, path } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:284`). Server-side LSP diagnostics; renderer LSP bridge lives in Phase 4.2. | ignored |
| `lsp.updated` | `{ id, type, properties:{ [k]:unknown } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:285`). | ignored |
| `mcp.tools.changed` | `{ id, type, properties:{ server } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:286`). Already-registered MCP slot rediscovery is handled by `opencode_mcp.rs`. | ignored |
| `mcp.browser.open.failed` | `{ id, type, properties:{ mcpName, url } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:287`). | ignored |

## File / VCS / workspace / worktree / TUI / PTY (global; no `sessionID`)

These are listed in `KNOWN_IGNORED_EVENTS` because acting on them per-conversation would fan out N-fold across open conversations. The shapes are pinned for the conformance gate; promotion is deferred to feature-specific plans (M10–M19 in the source-plan trail).

| Event | JSON shape | Handling site | Status |
| --- | --- | --- | --- |
| `file.edited` | `{ id, type, properties:{ file } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:281`). | ignored |
| `file.watcher.updated` | `{ id, type, properties:{ file, event:"add"\|"change"\|"unlink" } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:282`). | ignored |
| `vcs.branch.updated` | `{ id, type, properties:{ branch? } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:289`). | ignored |
| `workspace.ready` | `{ id, type, properties:{ name } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:291`). | ignored |
| `workspace.failed` | `{ id, type, properties:{ message } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:290`). | ignored |
| `workspace.status` | `{ id, type, properties:{ workspaceID, status } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:292`). | ignored |
| `worktree.ready` | `{ id, type, properties:{ name, branch? } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:294`). | ignored |
| `worktree.failed` | `{ id, type, properties:{ message } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:293`). | ignored |
| `tui.prompt.append` | `{ id, type, properties:{ … } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:300`). TUI remote control out of Chisl scope. | ignored |
| `tui.command.execute` | `{ id, type, properties:{ … } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:299`). | ignored |
| `tui.toast.show` | `{ id, type, properties:{ … } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:302`). | ignored |
| `tui.session.select` | `{ id, type, properties:{ … } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:301`). | ignored |
| `pty.created` | `{ id, type, properties:{ info: Pty } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:295`). Server-managed PTY lifecycle; Chisl uses node-pty + watchable PTY MCP (Phase 4.3). | ignored |
| `pty.updated` | `{ id, type, properties:{ info: Pty } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:298`). | ignored |
| `pty.exited` | `{ id, type, properties:{ id, exitCode } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:297`). | ignored |
| `pty.deleted` | `{ id, type, properties:{ id } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:296`). | ignored |
| `command.executed` | `{ id, type, properties:{ name, sessionID, arguments, messageID } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:283`). | ignored |

## Session lifecycle (`sessionID`-gated unless noted)

| Event | JSON shape | Handling site | Status |
| --- | --- | --- | --- |
| `session.created` | `{ id, type, properties:{ sessionID, info: Session } }` (v2; v1 lacks `sessionID`) | `agent.rs:1489` — used as the registration trigger for child (sub-agent) sessions via `subagent::try_register_from_session_created`; the event itself is consumed and not forwarded once the child is registered. Listed in `KNOWN_IGNORED_EVENTS` (`agent.rs:328`) for the root-session case where no further action is needed. | handled (sub-agent registration) / ignored (root) |
| `session.updated` | `{ id, type, properties:{ sessionID, info: Session } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:329`); M06 title-sync is the canonical follow-up plan. | partial |
| `session.deleted` | `{ id, type, properties:{ sessionID, info: Session } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:330`). | partial (M06) |
| `session.status` | `{ id, type, properties:{ sessionID, status:{ type:"idle"\|"busy"\|"retry" } } }` (v2) | `agent.rs:1526` — emits additive canonical `session_status { sessionId, status, reason? }`; `busy` still arms `root_turn_active` and emits canonical `Start`; `idle` still emits canonical `Finish` for the root or marks the child completed via `subagent::mark_completed`. | handled |
| `session.idle` | `{ id, type, properties:{ sessionID, reason?, at? } }` (v2; live capture only had `sessionID`) | `agent.rs:1588` — emits additive canonical `session_idle { sessionId, reason, at }`; same end-of-turn semantics as `session.status idle`; child idle marks the sub-agent completed, root idle emits root-turn `Finish`. | handled |
| `session.error` | `{ id, type, properties:{ sessionID?, error?:{ name, data:{ message, ...metadata } \| message }, recoverable?, messageID?, partID?, recoveryAction? } }` (v2) | `agent.rs:1614` — surfaces legacy user-visible `error.message` plus additive typed fields `{ kind, metadata?, recoverable }`; recoverable message/part-correlated errors emit `session_error_recovered`. API bodies are truncated to 500 chars and token-like strings are redacted before broadcast. | handled |
| `session.compacted` | `{ id, type, properties:{ sessionID, summary?, tokensReclaimed?, originalRange?:{ startMessageId, endMessageId } } }` (v2) | `agent.rs:2259` — emits `AgentStreamEvent::OpencodeSessionCompacted` with summary + token-reclaim metrics (M22). Note: the SDK type union has the bare `{sessionID}` shape; Chisl tolerates the extra `summary`/`tokensReclaimed`/`originalRange` fields the 1.15.x server emits because `unwrap_event` + per-field extraction is defensive. | handled |
| `session.diff` | `{ id, type, properties:{ sessionID, diff: Array<SnapshotFileDiff> } }` (v2) | `KNOWN_IGNORED_EVENTS` (`agent.rs:331`); the on-demand diff view fetches via `routes_aux.rs:37` rather than reacting to push events. M05. | partial |

## Session-`next` lifecycle (turn-internal lifecycle on `/global/event`)

| Event | JSON shape | Handling site | Status |
| --- | --- | --- | --- |
| `session.next.prompted` | v2: `{ id, type, properties:{ timestamp, sessionID, prompt: Prompt } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:304`). The same prompt arrives through `message.updated`. | ignored (v2-mirror) |
| `session.next.synthetic` | v2: `{ timestamp, sessionID, text }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:305`). | ignored |
| `session.next.retried` | v2: `{ timestamp, sessionID, attempt, error?: SessionNextRetryError, messageID?, partID?, reason?, retryAfter?, providerHint? }` | `agent.rs:1667` — emits additive canonical `retry { messageId, partId, attempt, reason, retryAfter?, providerHint?, replay? }`. Unknown reasons default to `unknown`; missing correlation is dropped with a debug log. | handled |
| `session.next.agent.switched` | v2: `{ timestamp, sessionID, agent }` | `agent.rs:1698` — surfaces a canonical agent-switch event for renderer/state. | handled |
| `session.next.model.switched` | v2: `{ timestamp, sessionID, model:{ id, providerID, variant } }` | `agent.rs:1667` — stamps the current model into manager state (used by `acp_context_usage` to look up the context window). | handled |
| `session.next.step.started` | v2: `{ timestamp, sessionID, agent, model:{…}, snapshot? }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:307`). | ignored (v2-mirror) |
| `session.next.step.ended` | v2: `{ timestamp, sessionID, finish, cost, tokens:{ input, output, reasoning, cache:{ read, write } }, snapshot? }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:308`). Cost/token aggregation is done from the rolling `Session.cost`/`Session.tokens` in `session.updated` today. | ignored (v2-mirror) |
| `session.next.step.failed` | v2: `{ timestamp, sessionID, error:{ type:"unknown", message } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:309`). | ignored (v2-mirror) |
| `session.next.text.started` | v2: `{ timestamp, sessionID }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:310`). Text streaming consumed via `message.part.delta` / `message.part.updated`. | ignored (v2-mirror) |
| `session.next.text.delta` | v2: `{ timestamp, sessionID, delta }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:311`). | ignored (v2-mirror) |
| `session.next.text.ended` | v2: `{ timestamp, sessionID, text }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:312`). | ignored (v2-mirror) |
| `session.next.reasoning.started` | v2: `{ timestamp, sessionID, reasoningID }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:313`). Reasoning surfaced through `message.part.updated` with `part.type == "reasoning"`. | ignored (v2-mirror) |
| `session.next.reasoning.delta` | v2: `{ timestamp, sessionID, reasoningID, delta }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:314`). | ignored (v2-mirror) |
| `session.next.reasoning.ended` | v2: `{ timestamp, sessionID, reasoningID, text }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:315`). | ignored (v2-mirror) |
| `session.next.shell.started` | v2: `{ timestamp, sessionID, callID, command }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:316`). | ignored (v2-mirror) |
| `session.next.shell.ended` | v2: `{ timestamp, sessionID, callID, output }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:317`). | ignored (v2-mirror) |
| `session.next.tool.input.started` | v2: `{ timestamp, sessionID, callID, name }`; Chisl's adapter also tolerates the legacy `properties:{ messageID, partID, toolName, startedAt }` shape it ships on `/event`. | `agent.rs:2069` → `opencode_stream::translate_tool_input` (`opencode_stream.rs:32`) → `AgentStreamEvent::ToolInput` phase=Started. | handled |
| `session.next.tool.input.delta` | v2: `{ timestamp, sessionID, callID, delta }`; legacy: `{ messageID, partID, inputDelta }`. | Same arm as above → phase=Delta. | handled |
| `session.next.tool.input.ended` | v2: `{ timestamp, sessionID, callID, text }`; legacy: `{ messageID, partID, finalInput }`. | Same arm as above → phase=Ended. | handled |
| `session.next.tool.called` | v2: `{ timestamp, sessionID, callID, tool, input, provider }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:318`). The tool-call lifecycle is consumed via `message.part.updated` with `part.type == "tool"`. | ignored (v2-mirror) |
| `session.next.tool.progress` | v2: `{ timestamp, sessionID, callID, structured, content:[…] }`; legacy: `{ messageID, partID, toolName, progress, at }`. | `agent.rs:2092` → `opencode_stream::translate_tool_progress` (`opencode_stream.rs:86`) → `AgentStreamEvent::ToolProgress` with `apply_patch` bodies stripped. | handled |
| `session.next.tool.success` | v2: `{ timestamp, sessionID, callID, structured, content:[…], provider }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:319`). | ignored (v2-mirror) |
| `session.next.tool.failed` | v2: `{ timestamp, sessionID, callID, error, provider }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:320`). | ignored (v2-mirror) |
| `session.next.compaction.started` | v2: `{ timestamp, sessionID, reason:"auto"\|"manual" }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:321`). | ignored (v2-mirror) |
| `session.next.compaction.delta` | v2: `{ timestamp, sessionID, text }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:322`). | ignored (v2-mirror) |
| `session.next.compaction.ended` | v2: `{ timestamp, sessionID, text, include? }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:323`). | ignored (v2-mirror) |

## Message lifecycle

| Event | JSON shape | Handling site | Status |
| --- | --- | --- | --- |
| `message.updated` | v2: `{ id, type, properties:{ sessionID, info: Message } }` | `agent.rs:1812` — drives the AssistantMessage/UserMessage state machine (tokens, cost, finish reason). | handled |
| `message.removed` | v2: `{ id, type, properties:{ sessionID, messageID } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:332`). M07 (`opencode-08`) is the follow-up; renderer-side reconciliation pending. | partial |
| `message.part.updated` | v2: `{ id, type, properties:{ sessionID, part: Part, time } }` | `agent.rs:1728` — primary content channel for text / reasoning / tool / patch / step-start / step-finish / snapshot / agent / retry / compaction / subtask parts. Sub-agent progress is rolled in via `tick_subagent_progress` (`agent.rs:2317`). Historical `RetryPart` replay is handled during `/session/{id}/message` backfill (`services/remote.rs:1084`). | handled |
| `message.part.delta` | v2: `{ id, type, properties:{ sessionID, messageID, partID, field, delta } }` | `agent.rs:1706` — incremental text/reasoning delta batched by `opencode_delta_batcher`. | handled |
| `message.part.removed` | v2: `{ id, type, properties:{ sessionID, messageID, partID } }` | `KNOWN_IGNORED_EVENTS` (`agent.rs:333`). | partial (M07) |

## Permission / question (`sessionID`-gated)

| Event | JSON shape | Handling site | Status |
| --- | --- | --- | --- |
| `permission.asked` | v2: `{ id, type, properties: PermissionRequest = { id, type:"…", pattern?, sessionID, messageID?, callID?, title?, metadata, time?, … } }`. Live capture shape includes `permission`, `patterns:[…]`, `always:[…]`, and a `tool:{ messageID, callID }` block. | `agent.rs:1896` — auto-resolves blessed paths/sessions, otherwise queues a `Confirmation` for the Approvals queue. | handled |
| `permission.replied` | v2: `{ id, type, properties:{ sessionID, requestID, reply:"once"\|"always"\|"reject" } }` | `agent.rs:2207` — drops any still-pending `Confirmation` for the request id and stamps the dedup map so a late local reply is suppressed (mirrors `permission.tsx`'s `responded` Map upstream). | handled |
| `permission.updated` *(v1 only)* | v1: `{ type, properties: Permission }` (no `sessionID`-discriminator at the envelope level) | Fall-through `debug` log today (not in `KNOWN_IGNORED_EVENTS`). 1.15.x emits `permission.asked` instead; promote only if a pre-v2 server target is reintroduced. | unhandled (v1-only) |
| `question.asked` | v2: `{ id, type, properties: QuestionRequest }` | `agent.rs:2106` → `opencode_question::parse_question_request` + per-question `Confirmation` enqueue (M09). | handled |
| `question.replied` | v2: `{ id, type, properties: QuestionReplied }` | `agent.rs:2160` — drops the pending question state on echo/cross-client reply. | handled |
| `question.rejected` | v2: `{ id, type, properties: QuestionRejected }` | Same arm as `question.replied`. | handled |

## Todo

| Event | JSON shape | Handling site | Status |
| --- | --- | --- | --- |
| `todo.updated` | v2: `{ id, type, properties:{ sessionID, todos: Array<Todo> } }` | `agent.rs:1882` — emits the canonical todo-plan into the renderer. | handled |

## Skill catalog

| Event | JSON shape | Handling site | Status |
| --- | --- | --- | --- |
| `skill.updated` | Not in v1/v2 SDK union but emitted by 1.15.x when the server's `SKILL.md` discovery cache changes. Properties unspecified upstream. | `agent.rs:2252` — invalidates the per-manager `opencode_skills` cache (M10). | handled |

## V2 sync mirror channel

| Event | JSON shape | Handling site | Status |
| --- | --- | --- | --- |
| `sync` *(wrapper)* | `{ type:"sync", syncEvent:{ type:"<name>.1", id, seq, aggregateID, data }, id }`. Mirrors every canonical event above with a monotonic `seq` per `aggregateID` (session id, project id, etc.). | Falls through to the unhandled-fingerprint `debug` path today. No `KNOWN_IGNORED_EVENTS` entry; the conformance fixture (`_sync.jsonl`) pins the shape so a future "v2 cursor replay" promotion can land without protocol surprises. | unhandled (forward-compatible) |

---

## Endpoints surface (REST contracts the adapter uses)

The conformance crate locks the SSE event surface above; the REST contracts below are documented here for cross-reference and are not part of the JSONL fixtures.

| Endpoint | Purpose | Handling site |
| --- | --- | --- |
| `GET /doc` | Probe OpenAPI document for `/global/event` presence. | `agent.rs:553` (`resolve_event_path`). |
| `GET /global/health` | Liveness + version. | `agent.rs:1171`. |
| `GET /global/event` *(preferred)* / `GET /event` *(legacy fallback)* | SSE stream. | `agent.rs:455` (`run_event_reader`). |
| `POST /permission/{requestID}/reply` body `{ reply:"once"\|"always"\|"reject", message? }` | Canonical permission reply. Deprecated session-scoped alias `POST /session/{sessionID}/permissions/{permissionID}` is intentionally avoided. | `agent.rs:723` (`build_permission_reply_request`). |
| `GET /session/{id}/children` | Child-session backfill on connect (sub-agent registry rehydration). | `agent.rs:419` (`fetch_child_sessions`). |

---

## Open questions (tracked in `aionui-opencode-conformance/TODO.md`)

1. **Version handshake.** OpenCode's `GET /global/health` returns `{ healthy, version }`. The SSE handshake (`server.connected`) does **not** include a version. The conformance suite pins against `opencode-ai-sdk-1.15.11` + live 1.15.13; deciding whether to tag the suite with a hard upper-bound version (and what to do when an upstream PR widens the union) is a follow-up. File an upstream request if a stable version field on `server.connected` becomes a release blocker.
2. **Cost-distinct-from-tokens.** `session.next.step.ended` carries `cost: number` alongside the `tokens` block; the canonical `AssistantMessage` exposes both. No distinct `cost.updated` event is observed in capture-2026-06-02 — Chisl computes the rolling figure from `Session.cost` updates in `session.updated`. If a future server emits a dedicated cost event, add a fixture + handler.
