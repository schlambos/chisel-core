# Chisel Core

> Rust backend for [Chisel](https://github.com/schlambos/chisel-ui), the multi-agent agentic coding platform.

`chisel-core` is the backend half of Chisel. It runs as a separate process from the desktop client (`chisel-ui`) and owns:

- **Agent orchestration** — coordinating multiple AI agents working on a codebase together
- **Remote agent protocols** — integrating external agents (Claude Code, Codex, OpenCode, etc.) over the wire
- **Conversation state** — sessions, threads, artifacts, scheduled tasks
- **HTTP / WebSocket API** — the interface `chisel-ui` and other clients talk to

The desktop client is in [`chisel-ui`](https://github.com/schlambos/chisel-ui). For the project pitch and overall context, start there.

---

## Status

**Early-stage personal project.** The API is unstable, the codebase is reorganizing, and the public surface is changing fast. Don’t build against this yet.

No releases. No published crates. No deployment story.

---

## Architecture

Cargo workspace organized in four layers (Foundation → Capability → Domain → Composition) with dependencies flowing strictly downward. See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full layout and [`AGENTS.md`](./AGENTS.md) for code conventions, testing rules, and contribution guidance that apply to humans and AI agents alike.

Built on Axum (HTTP), Tokio (async runtime), and SQLite (persistence).

---

## Heritage

`chisel-core` is a personal-project fork of [AionCore](https://github.com/iOfficeAI/AionCore) by iOfficeAI. The crate layout, runtime infrastructure, and agent management primitives come from that project. The remote-OpenCode integration work, the agentic-coding repositioning, and ongoing rebranding are the divergence. **Not affiliated with the AionCore project or its maintainers.**

---

## License

Inherits [Apache-2.0](LICENSE) from the upstream AionCore project.
