# AGENTS.md

<!-- Maintenance rule: Only add content that tells AI assistants WHAT TO DO or WHAT NOT TO DO.
     Implementation details, design rationale, and "how the system works" belong in ARCHITECTURE.md.
     If a section doesn't contain an actionable rule or constraint, it doesn't belong here. -->

Project-specific rules and conventions for AI assistants and contributors.

## Operating Rules (HARD)

These rules exist because agents have done real damage to this project: rewriting `main`'s history, force-pushing, inventing branches, scrubbing history to "fix" secrets, and scope-creeping into unrelated refactors. Read this section before any action.

This is a **solo-developer fork**. The sole contributor is the user. No PRs from outside contributors, no co-authors, no release process the agent needs to drive. Operations that touch a remote or rewrite history are the user's job.

### Git Safety

**NEVER, without explicit per-operation user approval:**

- **Never push to any remote** — not `origin`, not `upstream`, not feature branches. Commit locally and stop. Wait for the user to say "push it."
- **Never force-push.** Not with `--force`, `--force-with-lease`, `+refspec`, or any equivalent. There is no scenario where an agent force-pushes.
- **Never rewrite pushed history.** No `git rebase` on pushed commits, no `git commit --amend` on pushed commits, no `git reset` that moves the branch backward.
- **Never run history-rewriting tools.** No `git filter-repo`, `git filter-branch`, BFG, or anything that rewrites commit SHAs. Even for scrubbing secrets — stop and ask.
- **Never create branches.** Commit to the currently checked-out branch. If a branch seems needed, ask. Do not run `git checkout -b`, `git switch -c`, or `git branch <name>`.
- **Never merge into `main`** — including fast-forwards, including from a branch the agent created. The user merges to `main`.
- **Never delete branches or tags** (local or remote).
- **Never touch the `upstream` remote** (iOfficeAI/AionUi). No fetch, no merge, no rebase onto it, no PR. This fork is one-way: upstream → fork, never the reverse. The user handles all upstream-facing work.

**ALWAYS:**

- Commit to the current branch: `git add <named files>` (never `-A` / `.`), then `git commit -m "<type>(<scope>): <subject>"`.
- When in doubt, stop and ask. The cost of pausing is small; the cost of an unauthorized rewrite is hours of recovery.
- If an operation feels like "cleanup," it is almost certainly destructive. Ask first.

### Secrets

- **Files never to commit:** `.env`, `.env.*`, `*-backup.json`, `credentials*`, `*.pem`, `*.key`, and anything containing `secret` or `token` in the name.
- **If a secret is found in repo history → STOP and tell the user.** Do not run `git filter-repo`, BFG, or any history-scrubber. The cure has caused more damage than the disease here.
- Never log, print, or `echo` environment-variable contents.

### Scope Discipline

- Fix only what the task asks. No "while I'm here" cleanups, no reformatting unrelated files, no opportunistic refactors.
- Don't add error handling, validation, or fallbacks for cases that can't happen. Trust internal code and framework guarantees.
- Don't introduce abstractions for hypothetical future needs. Three similar lines beats a premature helper.
- If the work expands beyond what was asked (a 2-file fix turns into 20 files), **STOP and check** with the user before continuing.

### Verification Before Claiming "Done"

- "The code looks right" is not done. Run the relevant tests / clippy / fmt before saying a task is complete.
- If the change cannot be validated by the agent in this environment, the agent **MUST give the operator a numbered test plan in plain English** — no file paths, no jargon, no "verify the X handler returns 201." Describe what the operator should DO and what they should SEE:

  ```
  1. Start the app and open a chat with any agent.
  2. Send a message — it should appear in the conversation within a second.
  3. Refresh the page — the message should still be there.
  ```

- "I think this works" is fine. "Verified" requires evidence (command + exit code, or test output).

### Test Discipline

- Do not delete failing tests to make them pass.
- Do not weaken specific assertions to vague ones (`assert_eq!(status, 201)` → `assert!(status.is_success())` is prohibited).
- Do not add `#[ignore]` to make CI green.
- If a test is genuinely wrong, fix it with a clear explanation of why — don't quietly mutate assertions.

(See also [Test Failure Handling](#test-failure-handling) below for the existing project rules.)

### Documentation & Comments

- Do not create new `.md` files (planning docs, decision logs, summaries, `NOTES.md`, etc.) unless explicitly asked.
- Do not add comments that restate what the code does.
- Do not add "PR description" comments in code — no `// added for issue #123`, no `// per user request`, no dated changelogs in source files. The commit message and `CHANGELOG.md` are the right places for "why."

### Stop-and-Ask Triggers

The agent **must stop and ask** the user when:

- Git is in an unexpected state — untracked files, unfamiliar branches, in-progress merge/rebase, detached HEAD. Do not "clean up." Ask.
- A test has been failing across multiple debug attempts. Stop debugging in circles; describe the symptom and ask.
- The task's scope is expanding beyond the original ask.
- Anything labeled "NEVER" elsewhere in this file would need to happen for the task to proceed.

### Shell & Process Hygiene

- No `rm -rf` outside the immediate working directory.
- No `git add -A` or `git add .` — always name files. Prevents accidentally staging `.env`, secret files, or unrelated work.
- No `--no-verify`, `--no-gpg-sign`, or any flag that bypasses commit hooks "to get unstuck." If a hook fails, fix the underlying issue.
- Don't pipe `curl` or `wget` directly into `sh` / `bash`.

### Honest Reporting

- Surface unexpected results — don't bury them under a clean-looking summary.
- If a step was skipped, say so explicitly.
- If the agent made a judgment call the user didn't ask for, flag it.
- End-of-task summary must answer: **what changed, what was verified, what wasn't, and what the operator should test.**

## No i18n / Localization Work

Do not spend time on i18n, localization, translation dictionaries, locale routing, language switchers, or abstraction layers for display strings unless I explicitly request it.

For now, the app is English-only. Use plain English UI copy directly where appropriate. Do not create locale files, i18n providers, translation hooks, or string-key systems.

The only acceptable consideration is avoiding obviously hostile future design choices, such as deeply coupling business logic to user-facing prose. But do not proactively implement i18n infrastructure.

Prioritize functional correctness, UX behavior, state management, architecture, bug fixing, and code maintainability over theoretical future localization.

## High-Priority Rules

### Do NOT add fields to `AcpAgentManager` unless every alternative is exhausted

`AcpAgentManager` (in `crates/aionui-ai-agent/src/acp_agent.rs`) is already large and carries multiple overlapping state holders (e.g. `runtime_snapshot`, `state`, `preferred_mode`, `config`). New fields tend to duplicate semantics that `AcpRuntimeSnapshot` or `AcpState` already model, which fragments the source of truth and makes resume/new paths diverge.

Before adding a field:

1. Can the value live in `AcpRuntimeSnapshot`? (runtime/session-scoped state, including user-selected current_mode/current_model/config_selections)
2. Can it be derived from existing fields (`metadata`, `config`, `runtime_snapshot`, `state`)?
3. Can it be persisted via `acp_session.session_config` + `preload_persisted` instead of a new in-memory field?
4. If it must be in-memory and transient, can it be scoped to the call site (local variable, channel, task state) rather than the manager?

Only after exhausting the above — and explicitly documenting why each option is insufficient — add a new field. When doing so, also document its lifecycle (who writes, who reads, when it is invalidated) in a doc comment on the field.

### Logging

When changing a critical path, explicitly evaluate whether logs are needed for development diagnosis and production troubleshooting. Add structured logs with appropriate levels:

- `debug` for detailed, high-frequency internal flow that helps verify behavior and diagnose issues in development
- `info` for low-volume lifecycle boundaries useful in production
- `warn` for malformed or unexpected data that is safely handled
- `error` for contract violations or failed operations

Production-visible logs must not include sensitive payloads such as prompts, tool input/output, file contents, command bodies, tokens, secrets, or raw provider requests/responses. If such payloads are needed for local debugging, they must be behind explicit development-only guards and never enabled by default.

## Architecture

> For detailed background and design decisions, see [ARCHITECTURE.md](./ARCHITECTURE.md).

Cargo workspace organized in four layers: Foundation → Capability → Domain → Composition. Dependencies flow strictly downward.

### Crate Hierarchy & Dependencies

- ✅ Upper layers may depend on lower layers (including cross-layer)
- ✅ Same-layer interaction through trait abstractions only
- ❌ No lower-layer depending on upper-layer
- ❌ No circular dependencies
- Changes to foundation crates require impact assessment

### Domain Crate Structure

Every domain crate must follow:

- `lib.rs` — module exports only, no business logic
- `routes.rs` — export `domain_routes(state) -> Router`, handlers do request/response transformation only
- `service.rs` — sole location for business logic, must not import axum
- `state.rs` — `#[derive(Clone)]` RouterState holding Arc-wrapped dependencies

### API Conventions

- Route prefix: `/api/`
- Resource names: kebab-case
- Response format: `ApiResponse<T>` (success) / `ErrorResponse` (failure)
- All request/response types defined in `aionui-api-types`
- `aionui-api-types` must NOT depend on axum/tower or any HTTP framework

### WebSocket Events

- Format: `domain.camelCaseAction` (two-level structure)
- Message type: `WebSocketMessage<T>` (name + data)
- Existing kebab-case or three-level names are legacy — new events must follow the convention

### Data Layer

- Repository traits in `aionui-db`, prefixed with `I`
- Concrete implementations prefixed with `Sqlite`
- Row models in `aionui-db/src/models/`
- Params objects co-located in repository files
- Migrations: `NNN_descriptive_name.sql`, no manual DB modifications
- Services depend on traits, never on concrete implementations

### Dependency Injection

- `AppServices` is the sole service construction center
- Domain crates only define RouterState, never construct their own dependencies
- All assembly happens in `aionui-app`'s `build_*_state()` functions

### Security

- New endpoints must be evaluated for auth middleware requirement
- State-changing operations must be CSRF-protected
- Sensitive operations should have rate limiting
- Error responses must not leak internal details
- Secrets must never be hardcoded

## Code Style

- Rust 2024 edition, stable toolchain (pinned in `rust-toolchain.toml`)
- Comments in English, commit messages in English
- Each `.rs` file follows single responsibility — one module, one concern
- Max 1000 lines per `.rs` file; split into submodules when approaching the limit

## Development Workflow

### Subprocess Spawning

New subprocess spawn sites must use `aionui_runtime::Builder::agent(program)` or `aionui_runtime::Builder::clean_cli(program)`. Do NOT use raw `tokio::process::Command`. See [ARCHITECTURE.md § Runtime Infrastructure](./ARCHITECTURE.md#runtime-infrastructure) for details.

### Committing Code

Always use `git commit` after completing the required verification steps.
Run formatting, linting, and tests before committing to prevent CI failures.
Use standard Git commit syntax with a clear English commit message (e.g. `git commit -m "Describe the change"`).

### Add Endpoint to Existing Crate

1. Request/response types → `aionui-api-types/src/{domain}.rs`
2. Handler function → `crates/aionui-{domain}/src/routes.rs`
3. Business logic → `crates/aionui-{domain}/src/service.rs`
4. Register route in `domain_routes()` function
5. Add test → `crates/aionui-{domain}/tests/` or `crates/aionui-app/tests/`

### Add Migration

1. Next number → `ls crates/aionui-db/migrations/`
2. Create `NNN_descriptive_name.sql` with `IF NOT EXISTS`

### Add WebSocket Event

1. Event type → `aionui-api-types`
2. Emit via `event_bus.broadcast()` in service
3. Naming: `domain.camelCaseAction`

## Test Organization

| Location                                 | What goes there                        |
| ---------------------------------------- | -------------------------------------- |
| Inline `#[cfg(test)]` in each `.rs` file | Unit tests for that module's internals |
| `crates/<crate>/tests/`                  | Integration / E2E tests for that crate |

### Testing Rules

- Database tests use `init_database_memory()`
- Prefer real in-memory DB over mocks; mock only to isolate unneeded dependencies
- New features must include tests

### Test Scope Requirements

**Happy Path (Critical Paths)**

Every new or modified feature must have integration tests covering its normal flow. Critical paths that always require test coverage:

- Authentication flow (login, token refresh, permission checks)
- Message sending and retrieval
- Agent session creation and interaction
- File upload/download
- WebSocket connection and event delivery

**Bad Path (Error Paths)**

New endpoints or business logic must include tests for these scenarios:

- Invalid input (missing fields, wrong types, oversized content)
- Resource not found (404)
- Insufficient permissions (unauthenticated, accessing another user's resources)
- Business rule violations (duplicate creation, operations not allowed in current state)

Bad path tests must assert specific error codes or error messages — asserting merely "not success" is not acceptable.

**Security Tests**

Endpoints involving authentication, authorization, or data isolation must include security tests:

- Unauthenticated requests are rejected (401)
- Cross-user data isolation (user A cannot access user B's resources)
- State-changing requests are rejected when CSRF token is missing or invalid
- Sensitive fields (passwords, tokens) never appear in responses

**WebSocket Event Tests**

New WebSocket events must verify:

- The event is emitted after the correct business operation
- Event payload conforms to `WebSocketMessage<T>` structure
- Events are only delivered to authorized subscribers (no leakage to unrelated users)

### Test Failure Handling

When a test fails, do NOT modify the test to make it pass. First determine:

1. **Test assertion still represents correct behavior** → fix implementation, not the test
2. **Requirements/interface intentionally changed** → may update test, but must confirm:
   - The change is intentional (not an unintended side effect)
   - New assertions still validate meaningful behavior
3. **Uncertain** → stop, trace back the change, clarify before proceeding

Prohibited:

- ❌ Deleting failing tests to "fix" the problem
- ❌ Weakening specific assertions to vague ones (e.g., `assert_eq!(status, 201)` → `assert!(status.is_success())`)

## Verification Strategy

> ⚠️ **When to run what:**
>
> - During development: only test the crate you're working on → `cargo test -p aionui-<crate>`
> - After implementation complete: full verification → `cargo test --workspace`
> - Do NOT run `cargo test --workspace` at the start of a task.
>
> ⚠️ **Performance:**
>
> - `cargo clippy --workspace` takes several minutes — use `run_in_background: true`.
> - `cargo test --workspace` takes 10+ minutes. MUST use `run_in_background: true` when calling via Bash tool, otherwise it will timeout.
> - `cargo clippy -p aionui-<crate>` and `cargo test -p aionui-<crate>` typically complete in under 1 minute.

### During Development (fast feedback loop)

```bash
cargo test -p aionui-<crate>                          # Test the crate you changed
cargo clippy -p aionui-<crate> -- -D warnings         # Lint the crate you changed
```

### Before Commit (affected crates)

```bash
cargo fmt --all -- --check                                                      # Format gate (instant)
cargo clippy -p aionui-<crate1> -p aionui-<crate2> -- -D warnings              # Lint affected crates
cargo test -p aionui-<crate1> -p aionui-<crate2>                               # Test affected crates
```

### Before Commit (full workspace)

```bash
cargo fmt --all -- --check                           # Format gate
cargo clippy --workspace -- -D warnings              # Full workspace lint
cargo test --workspace                               # Full workspace test
git status                                           # Review changed files
git add <files>                                      # Stage intended changes
git commit -m "Describe the change"                  # Commit with a clear English message
```

## Changelog

**Every commit must include an update to `~/chisl-full/CHANGELOG.md`** (the shared changelog at the root of the monorepo).

- Add an entry under today's date with the repo name (`AionUi` or `AionCore`) and a plain-English description of what changed and why.
- Be specific: what was broken, what was added, what decision was made. "Added new features" is not acceptable.
- The changelog lives at `~/chisl-full/CHANGELOG.md` and covers both repos in a single file.
- If you are an AI agent: update the changelog before running `git commit`. Do not skip this step.

> **AGENT DIRECTIVE — READ AND OBEY.** The changelog at `~/chisl-full/CHANGELOG.md`
> lives in `~/chisl-full`, which is **deliberately NOT a git repository**. This is
> **intentional and by design**. The owner is fully aware of its location and its
> untracked status. **NEVER** mention, flag, warn about, "note," or otherwise raise
> the changelog's location or the fact that it is untracked / not versioned. Do not
> suggest moving it into a repo. Do not treat it as a problem. Just update it and move
> on. Raising this topic wastes the owner's time and is explicitly forbidden.
