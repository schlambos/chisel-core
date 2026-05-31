<p align="center">
  <img src="https://raw.githubusercontent.com/schlambos/chisel-ui/main/packages/desktop/src/renderer/assets/logos/brand/wordmark.png" alt="Chisl" width="360" />
</p>

<p align="center">
  <strong>The Rust backend that powers Chisl — agent orchestration, conversations, and the API the desktop client talks to.</strong>
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/schlambos/chisel-ui/main/packages/desktop/src/renderer/assets/logos/brand/app.png" alt="Chisl app icon" width="72" />
</p>

<p align="center">
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-b4480c?style=flat-square" /></a>
  <img alt="Rust" src="https://img.shields.io/badge/Rust-2024-b4480c?style=flat-square" />
  <img alt="Axum" src="https://img.shields.io/badge/Axum-0.8-607848?style=flat-square" />
  <img alt="Tokio" src="https://img.shields.io/badge/Tokio-async-305460?style=flat-square" />
  <img alt="SQLite" src="https://img.shields.io/badge/SQLite-sqlx-c08418?style=flat-square" />
</p>

<p align="center">
  <strong>Chisl palette</strong><br />
  <img alt="rust #b4480c" src="https://img.shields.io/badge/rust-%23b4480c-b4480c?style=flat-square" />
  <img alt="parchment #f0e4b4" src="https://img.shields.io/badge/parchment-%23f0e4b4-f0e4b4?style=flat-square&labelColor=303024" />
  <img alt="ink #303024" src="https://img.shields.io/badge/ink-%23303024-303024?style=flat-square" />
  <img alt="olive #607848" src="https://img.shields.io/badge/olive-%23607848-607848?style=flat-square" />
  <img alt="slate #3c786c" src="https://img.shields.io/badge/slate-%233c786c-3c786c?style=flat-square" />
</p>

---

## What Chislcore Does

Chislcore is the backend half of [Chisl](https://github.com/schlambos/chisel-ui). It runs as its own process — the desktop client connects to it over HTTP and WebSocket — and owns everything that isn't the UI:

- **Agent orchestration** — managing agent lifecycles across multiple backends, serializing tool turns, and relaying streamed output.
- **Conversation state** — sessions, messages, confirmations, streaming responses, and persistence in SQLite.
- **Multi-protocol agents** — local ACP CLIs (Claude Code, Gemini CLI, Codex), remote OpenCode servers over HTTP/SSE, plus OpenClaw, Nanobot, and Aionrs adapters.
- **HTTP / WebSocket API** — an Axum server exposing `/api/*` REST routes and a single `/ws` event stream, the interface the desktop client and other clients consume.

It is built on **Axum** (HTTP), **Tokio** (async runtime), and **SQLite via sqlx** (persistence), and embeds a **bun** runtime so it can spawn JS-based agent tooling without a system install.

The desktop client lives in [`chisel-ui`](https://github.com/schlambos/chisel-ui). For the product pitch, start there.

## Features

| Capability             | What it does                                                                                                                                       |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Agent registry         | Hydrates available agent backends, probes CLIs on `$PATH`, and routes each conversation to the right agent manager.                                 |
| Conversation service   | Create, list, rename, archive, and delete conversations; stream assistant responses; persist messages and confirmations.                           |
| Remote OpenCode bridge | Full HTTP/SSE integration with `opencode serve`: sessions, prompts, permissions, questions, fork/revert/share/summarize, LSP/VCS, and global config. |
| Tool turns & MCP       | Serializes tool turns per server, bridges the client filesystem over MCP, and manages server-side MCP definitions and OAuth.                        |
| Approvals & questions  | Surfaces tool-permission and `/question` requests to the client and relays replies back to the agent.                                              |
| Sub-agents             | Tracks OpenCode child sessions and forwards their lifecycle as subtask events.                                                                     |
| Channels               | Inbound/outbound integration with WeChat, DingTalk, Lark, and Telegram through a plugin system with pairing sessions.                              |
| Team & cron            | Multi-agent team scheduling with a mailbox system, plus a cron engine for scheduled job execution and event triggering.                            |
| Files & office         | File operations, watching, snapshots, git operations, compression, and Office document preview/conversion (Excel, PPT, Word).                       |
| Extensions & skills    | Extension registry and hub, plus skill discovery, import, and installation.                                                                        |
| Realtime events        | A broadcast event bus over a single `/ws` endpoint pushes `domain.action` events (conversation, cron, extensions, …) to connected clients.          |
| Auth & security        | JWT sessions, CSRF double-submit protection, bcrypt password hashing, tiered rate limiting, and an embedded-mode bypass for the desktop app.        |

## Agent Protocols

Conversations are routed to an agent manager by protocol. Each manager implements the same task interface, so the conversation layer stays protocol-agnostic.

| Backend         | How it connects                                                                                          |
| --------------- | -------------------------------------------------------------------------------------------------------- |
| ACP (local)     | Local coding-agent CLIs spoken to over the Agent Client Protocol — Claude Code, Gemini CLI, Codex.       |
| Remote OpenCode | A reachable `opencode serve` instance over HTTP + SSE (the most fully-featured remote path).             |
| OpenClaw        | OpenClaw agent backend.                                                                                  |
| Nanobot         | Nanobot agent backend.                                                                                   |
| Aionrs          | Aionrs agent backend.                                                                                    |
| Custom command  | A user-defined ACP-style command (command, args, env) registered from the desktop client.               |

## HTTP / WebSocket API

| Surface         | Detail                                                                                                          |
| --------------- | -------------------------------------------------------------------------------------------------------------- |
| REST            | All routes under the `/api/` prefix with kebab-case resource names; unified `ApiResponse<T>` / `ErrorResponse`. |
| Realtime        | A single `/ws` endpoint; messages are `{ name: "domain.action", data }`, broadcast via the event bus.           |
| Contracts       | Every request/response type lives in `aionui-api-types` — the single source of truth, with no HTTP-framework deps. |
| Errors          | `AppError` variants map to stable status codes and error codes (e.g. `NotFound` → 404 `NOT_FOUND`).             |

## Channels

Chislcore can drive agents from messaging platforms through a channel plugin system with pairing sessions.

| Channel  | Plugin     |
| -------- | ---------- |
| WeChat   | `weixin`   |
| DingTalk | `dingtalk` |
| Lark     | `lark`     |
| Telegram | `telegram` |

## Security

| Area          | Behavior                                                                                                            |
| ------------- | ------------------------------------------------------------------------------------------------------------------- |
| Sessions      | JWT (HMAC-SHA256, 24h), extracted from `Authorization: Bearer` or the `aionui-session` cookie; supports a blacklist. |
| CSRF          | Double Submit Cookie (`aionui-csrf-token` cookie vs `x-csrf-token` header); safe methods and login routes exempt.   |
| Passwords     | bcrypt (cost 12), with timing-attack and user-enumeration protection.                                               |
| Rate limiting | Tiered by client IP / user — auth (5/15min), public API (60/min), sensitive actions (20/min).                       |
| Local mode    | `--local` skips JWT/CSRF, opens CORS, and injects a fixed `system_default_user` — intended for the embedded desktop app. |

## Architecture

A Cargo workspace of 21 crates across four layers, with dependencies flowing strictly downward:

```
Composition  →  aionui-app                         (binary, router assembly)
Domain       →  conversation, channel, team, cron, file, office,
                system, mcp, ai-agent, extension, shell, assistant
Capability   →  aionui-auth (JWT/CSRF)   aionui-realtime (WebSocket/events)
Foundation   →  aionui-common  aionui-api-types  aionui-db  aionui-assets
                aionui-runtime  aionui-lsp
```

Domain crates are loosely coupled and interact only through trait abstractions. Database access goes through `I*Repository` traits (with `Sqlite*` implementations); migrations are embedded and run on startup. See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full layout and [`AGENTS.md`](./AGENTS.md) for conventions.

## Quick Start

Build the workspace:

```bash
cargo build --workspace
```

Run the server (defaults to `127.0.0.1:25808`):

```bash
cargo run --bin aioncore -- --local
```

Useful flags: `--host`, `--port`, `--data-dir <path>`, `--work-dir <path>`, `--log-level "info,aionui_mcp=trace"`. `--local` is the embedded mode the desktop app uses (no auth, open CORS).

> When run alongside the desktop client, `chisel-ui` resolves the `aioncore` binary (the shipped executable name) from `PATH` and launches it for you — you usually only run `bun run dev` in `chisel-ui` during development.

## CLI

| Command            | Purpose                                                              |
| ------------------ | -------------------------------------------------------------------- |
| _(default)_        | Start the HTTP + WebSocket server.                                   |
| `doctor`           | Probe every agent CLI on `$PATH` and print a per-agent availability table. |
| `mcp-bridge`       | Stdio ↔ TCP bridge for the team MCP server (spawned by the ACP agent CLI). |
| `mcp-guide-stdio`  | MCP stdio server for team-guide tools.                              |
| `mcp-team-stdio`   | MCP stdio server for team tools.                                    |

## Development Checks

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

To push with the full gate (format → clippy → test → push), use the workspace recipe instead of raw `git push`:

```bash
just push
```

## Documentation

| Topic                 | Path               |
| --------------------- | ------------------ |
| Architecture & layers | `ARCHITECTURE.md`  |
| Conventions & rules   | `AGENTS.md`        |
| Change history        | `CHANGELOG.md`     |

## Status

Chislcore is under active development. The API surface is still moving; treat it as unstable.

## License

Licensed under [Apache-2.0](LICENSE).

## Heritage

Chislcore is a fork of [AionCore](https://github.com/iOfficeAI/AionCore) by iOfficeAI. The crate layout, runtime infrastructure, and agent-management primitives come from that project; the remote-OpenCode integration, the agentic-coding repositioning, and the Chisl rebrand are the divergence. Original copyright and license notices are preserved in accordance with the Apache License 2.0. **Not affiliated with the AionCore project or its maintainers.**
