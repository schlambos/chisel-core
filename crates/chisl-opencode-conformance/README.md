# `aionui-opencode-conformance`

Test-only crate that pins the OpenCode SSE protocol surface Chisl's
remote-OpenCode adapter assumes. It is **not** wired into any runtime path; its
job is to lock the JSON shape of every SSE event the adapter recognises so
upstream protocol drift fails CI at PR time instead of surfacing as silent
rendering bugs in production.

The companion design document is
[`aionui-ai-agent/src/manager/remote/PROTOCOL.md`](../aionui-ai-agent/src/manager/remote/PROTOCOL.md);
the recorded fixtures live in `fixtures/`; the integration test that exercises
this library is in `tests/event_parsing.rs`.

## SDK version pin

The pinned `@opencode-ai/sdk` version is read from the chisl-root
`opencode-sdk-version.json` (single source of truth) by `build.rs` and
re-exported as `OPENCODE_SDK_VERSION` (and friends). The pin is asserted by:

- `tests::sdk_version_pin_is_populated` — the constant is non-empty and looks
  like a semver.
- `tests::sdk_version_pin_matches_package_name` — the package name is
  `@opencode-ai/sdk`.
- `tests::sdk_version_pin_matches_package_json_dev_dependency` — the
  `devDependencies."@opencode-ai/sdk"` entry in
  `<chisl-root>/AionUi/package.json` matches the JSON pin.

If any of the three checks fails, `cargo test -p aionui-opencode-conformance`
fails. The CI workflow
`<chisl-root>/AionCore/.github/workflows/conformance.yml` runs the suite on every
PR, so a bumped SDK that only updates the JSON, only `package.json`, or only
the installed `node_modules` will not merge.

The TS side mirrors this check: `AionUi/scripts/sync-opencode-types.js` reads
the same JSON, and the `--check` exit code (2) gates `bun run types:sync-opencode`
in CI for the renderer pipeline.

To bump the SDK see the step-by-step procedure in
[`PROTOCOL.md` → Version pin contract](../aionui-ai-agent/src/manager/remote/PROTOCOL.md#version-pin-contract).
