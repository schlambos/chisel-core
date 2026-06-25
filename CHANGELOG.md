# Changelog

## [0.1.12](https://github.com/schlambos/chisel-core/compare/v0.1.11...v0.1.12) (2026-06-25)


### Features

* **ledger:** add tool-call restore plan foundation ([88361f7](https://github.com/schlambos/chisel-core/commit/88361f7a4fc55d497e10d5e6e0379dc4b8301616))
* **ledger:** expose tool-call restore plan route ([8e36d8b](https://github.com/schlambos/chisel-core/commit/8e36d8b295372ea4c5475a267b0dcdf0ad244831))
* **local-opencode:** Phase 4 process manager for local opencode serve instances ([1d56501](https://github.com/schlambos/chisel-core/commit/1d56501e4ffd5a379e29b81d0af7d5df9f881fc9))
* **lsp:** LSP session lifecycle hardening + remove broken PowerShell entry ([5a1c59f](https://github.com/schlambos/chisel-core/commit/5a1c59ff2a546295220d284b5f1c4802705f5b3f))
* **opencode:** extend remote provider auth routes for M12 API parity ([48f1ed3](https://github.com/schlambos/chisel-core/commit/48f1ed32985f9ec696901644f77e4ad11405c2e5))
* **opencode:** first-class remote API integration (C03, M12, M13, M20, V2 context) ([b15e930](https://github.com/schlambos/chisel-core/commit/b15e930e9ae43062dd6e1e4eb6d6bd3cd234a8d2))
* **P0:** pin OpenCode SSE protocol surface + recorded conformance suite + CI gate ([1d3cdf9](https://github.com/schlambos/chisel-core/commit/1d3cdf9c87f19c6ad2dcd943cfe473c110f5b0e5))
* **P1.1:** canonical stop/idle/retry/error SSE events + synced-row workspace fix ([e488b50](https://github.com/schlambos/chisel-core/commit/e488b50d8b0e7b180328f320e4b078b95045b441))
* **P1.2a:** question/permission reconcile + metadata + cross-client settle + subagent timeout/inheritance/pending-prompts aggregator ([0617a50](https://github.com/schlambos/chisel-core/commit/0617a5037fa3518b2e37ed48ca9018174e71c8be))
* **permissions:** resubmit-shell + conversation-scoped permission sync endpoints ([591174f](https://github.com/schlambos/chisel-core/commit/591174f4de9441036c194bcb20250af01b6c94d9))
* persist and resume remote OpenCode sessions ([47274f4](https://github.com/schlambos/chisel-core/commit/47274f4716c153bd09eb41616f7aefbc2099f104))
* **phase-3:** Voice Mode, Background Processes, SideCars, and Event Reactions ([9293eb0](https://github.com/schlambos/chisel-core/commit/9293eb08b7625d9ef44f9cae0fa838f37ff08264))
* **plugin:** add streaming-latency trace instrumentation for Phase 2 Metric 3 ([7a6e403](https://github.com/schlambos/chisel-core/commit/7a6e4030856286fc89d4097c24e475473f7a3fa0))
* **plugin:** eagerly start plugin webserver on first remote agent session ([29327a2](https://github.com/schlambos/chisel-core/commit/29327a2997734f5ff020960495af2b3624c42ad5))
* **remote/opencode:** /question flow via Approvals queue (M09) ([faccf98](https://github.com/schlambos/chisel-core/commit/faccf985cc2df4c331b4e3f6996b36ac27283bef))
* **remote/opencode:** backfill child sessions on reconnect (M08) ([d0d602b](https://github.com/schlambos/chisel-core/commit/d0d602b652a7b5d32b220e73d2e00f883be14f8d))
* **remote/opencode:** discover server agents as selectable modes (M10) ([fa636a4](https://github.com/schlambos/chisel-core/commit/fa636a465d91375d2dbb23a49a89ab3264c4aa1c))
* **remote/opencode:** full SSE event coverage with classified fallback (E02) ([562e9df](https://github.com/schlambos/chisel-core/commit/562e9dff6fd88a5944705209c41049a25accc54f))
* **remote/opencode:** global config read/write + effective-config endpoint (M19) ([c47671b](https://github.com/schlambos/chisel-core/commit/c47671bb6466a307ad76bbb1613a477e11217f2b))
* **remote/opencode:** log forwarding + LSP/VCS endpoints (M14/M15/M16) ([546c740](https://github.com/schlambos/chisel-core/commit/546c740f2a3847cb1c10258616f3c5b6835c242b))
* **remote/opencode:** message edit/delete groundwork + endpoints (M07) ([97ddafa](https://github.com/schlambos/chisel-core/commit/97ddafa16dbffcdd9258351f80c39b8f3d7616bd))
* **remote/opencode:** per-agent health probe endpoint (A02) ([8535432](https://github.com/schlambos/chisel-core/commit/85354325bfe6e2e1ac9bb5b292d18513f66a6c60))
* **remote/opencode:** propagate conversation rename/archive to server session (M06) ([6c3333b](https://github.com/schlambos/chisel-core/commit/6c3333be0fc2e8f342b72c18717eef5ddfe597e6))
* **remote/opencode:** skills catalog, fork/revert/diff, share/summarize, inject_skills (M10/M03/M04/M09) ([93299bd](https://github.com/schlambos/chisel-core/commit/93299bdee890b2d324c68c94932ea5c327614c4e))
* **remote/opencode:** SSE heartbeat + supervised auto-reconnect (C02) ([1011292](https://github.com/schlambos/chisel-core/commit/101129247e81c619a53e83bcfeda6f8356f0fac1))
* **remote/opencode:** stream tool output and fix parallel permissions ([2d7f297](https://github.com/schlambos/chisel-core/commit/2d7f297d04e7953407b233c27bf3f22fd6d323a1))
* **remote/opencode:** subscribe to /global/event + unwrap payload (C01+E01) ([fbcee5f](https://github.com/schlambos/chisel-core/commit/fbcee5fd02081deb944a1150f9869212c053c5e6))
* **remote/opencode:** support generic basic auth (A01) ([f283ffb](https://github.com/schlambos/chisel-core/commit/f283ffb7ab03a1b12cc1a47bb765aea5e8ecd95a))
* **remote/opencode:** tool-host local/server mode (C04) ([b106e51](https://github.com/schlambos/chisel-core/commit/b106e512ca8a7dca3d1e6229946c24345f4d3baa))
* **remote/opencode:** V2 session API + sync collaboration (M20/M22) ([6dbb164](https://github.com/schlambos/chisel-core/commit/6dbb164241886dfac9c043b1a48bd65294e176fe))
* **remote:** A1 directory-scoped reply hotfix + A5 connect error classification and server-tools workspace fail-fast ([a403cfa](https://github.com/schlambos/chisel-core/commit/a403cfa7fc2d5f74392e1bc2c49e5d9f775ed21c))
* **remote:** bridge client filesystem to remote OpenCode via MCP ([7064839](https://github.com/schlambos/chisel-core/commit/7064839e0dcf47cb8e8093e921e0d70574230362))
* **remote:** deny local tool permissions and inject workspace system hint for remote agents ([754345f](https://github.com/schlambos/chisel-core/commit/754345f70009c35daf275f1ffaa1ea73f2010a20))
* **remote:** emit producing model id per OpenCode assistant message ([b8e220e](https://github.com/schlambos/chisel-core/commit/b8e220e1aa7f993a2b75002bd68cf4178deba2be))
* **remote:** map OpenCode todo.updated events to Plan stream events ([3dcabc3](https://github.com/schlambos/chisel-core/commit/3dcabc3e2100de2b36699699f3f5629935bae6d0))
* **remote:** plugin webserver control-plane channel for Chisl OpenCode plugin ([2604135](https://github.com/schlambos/chisel-core/commit/26041356515571f7ae7f6022a29566af73d2755f))
* **remote:** run shell commands locally via MCP with user confirmation ([49cf9d3](https://github.com/schlambos/chisel-core/commit/49cf9d304ed4e9fde74e04472af9633553bfbcbc))
* **remote:** synthesize acp_context_usage for OpenCode turns ([ade38d2](https://github.com/schlambos/chisel-core/commit/ade38d2094889192739c1928aff36f74151eaef4))
* **remote:** verify and self-heal local fs MCP reachability ([d595d9d](https://github.com/schlambos/chisel-core/commit/d595d9dd606945aebb80bf3ded6cd9fe6e61ee4b))
* **revert:** add per-hunk/per-file edit revert and verify hook ([b10bc42](https://github.com/schlambos/chisel-core/commit/b10bc42aca43fec5437df19d2d1549817b13f4db))
* surface OpenCode slash commands via GET /command ([e9b2df6](https://github.com/schlambos/chisel-core/commit/e9b2df6e8a5968fabd17a758db3c8549a6f5baf1))
* **sync:** background session listing uses paginated V2 API ([bee8f00](https://github.com/schlambos/chisel-core/commit/bee8f008c9506b2218bb19bb59e176f1c17dd311))
* **Task14.1:** add opencode_tool_snapshots DB layer ([7b45ac2](https://github.com/schlambos/chisel-core/commit/7b45ac29c45d3c6287f0b01f09dbbea09ef8f38d))
* **Task14.2:** per-tool-call snapshot Git layer ([204e487](https://github.com/schlambos/chisel-core/commit/204e4875dfa88ebdcc8fa6515550f8f06e08b4c5))
* **Task14.3:** hook local_fs_mcp and expose revert-tool-call route ([9ad0f57](https://github.com/schlambos/chisel-core/commit/9ad0f573a55a4ecafa0bda6adca8fdc06aefb957))
* **terminal:** migrate terminal backend to Rust portable-pty ([afb15e0](https://github.com/schlambos/chisel-core/commit/afb15e04bbf9434917aa7e7076cba72f8635cc14))
* **vcs:** add workspace VCS status and init routes ([2bd89a5](https://github.com/schlambos/chisel-core/commit/2bd89a5ebceff17bcb7edb7d819ad413159dbcc0))


### Bug Fixes

* **aionrs:** drop orphaned tool_call history on session resume (ELECTRON-1HV, ELECTRON-1J6) ([#330](https://github.com/schlambos/chisel-core/issues/330)) ([880722f](https://github.com/schlambos/chisel-core/commit/880722fd3b2f4e37fa5654cc5ed210cddbfd14b5))
* **aionrs:** preserve tool call correlation across aborts ([#335](https://github.com/schlambos/chisel-core/issues/335)) ([d65c8ed](https://github.com/schlambos/chisel-core/commit/d65c8ed49be4a558aff99e907e359264d6729d1c))
* **conversation:** persist revert extra and broadcast listChanged ([27009ff](https://github.com/schlambos/chisel-core/commit/27009ffe5e1221b45ffad0b0d7e356f0532281f3))
* **conversation:** unify provider/model resolution across send/cron paths (ELECTRON-1HX, ELECTRON-1HM) ([#326](https://github.com/schlambos/chisel-core/issues/326)) ([71e275a](https://github.com/schlambos/chisel-core/commit/71e275ae3295d88c9da5eacf9f959d4683b4043d))
* **events:** add missing ErrorEventData fields to test constructors ([79d4586](https://github.com/schlambos/chisel-core/commit/79d458608df21cd7fd3012d9a443f5a7a270fb6d))
* filter remote OpenCode SSE events by owning session id ([59f7a88](https://github.com/schlambos/chisel-core/commit/59f7a882d14e6c0e25ec5374e0b6487f187c447b))
* **mcp:** improve local fs MCP reliability and model context ([4578454](https://github.com/schlambos/chisel-core/commit/4578454cb2035ab7bed36cdb70e5831683990084))
* **opencode:** stop sending unsupported v2 prompt override fields ([85dc28a](https://github.com/schlambos/chisel-core/commit/85dc28a81e5a587af179de3fcd8cba90f9de0043))
* **plugin:** probe-hello guard, opencode-entry snippet, bearer auth, fixed port 64921 ([09c851b](https://github.com/schlambos/chisel-core/commit/09c851b06d1c4ec14faf3d56dbd0b814642c9137))
* **remote/opencode:** collapse fs MCP to single slot + serialize turns ([92dfbfa](https://github.com/schlambos/chisel-core/commit/92dfbfa514c7b7325830b6e76817f03a2bd458d6))
* **remote/opencode:** persist sessionKey at session-create to prevent duplicate conversation (F02) ([f091f17](https://github.com/schlambos/chisel-core/commit/f091f17d1c6eb71dbfa517b88f1107902a7fec38))
* **remote/opencode:** silence too_many_arguments + cover new event in test match ([5a6f2aa](https://github.com/schlambos/chisel-core/commit/5a6f2aa26de5ec808f37b82b0c7b1da46d54ad1e))
* **remote/opencode:** use canonical /permission/{id}/reply endpoint ([746a4d2](https://github.com/schlambos/chisel-core/commit/746a4d28303fc3d8169f68ed579c4b0b6fabcd53))
* **remote:** re-register local fs MCP on OpenCode session resume ([f8e43c2](https://github.com/schlambos/chisel-core/commit/f8e43c241759f4181250c33c88c33fb4a895b4e1))
* **sidecar:** security review fixes — stream responses, warn on non-loopback bind, strip Connection-listed headers, drop Referer ([4578baf](https://github.com/schlambos/chisel-core/commit/4578baf60b06f731b43dd96ddb9c027c3aca8921))
* split streamed message segments around tool boundaries ([#339](https://github.com/schlambos/chisel-core/issues/339)) ([476b1cc](https://github.com/schlambos/chisel-core/commit/476b1cc86f2adef8998477a666809dda50afca3e))
* **sync:** dispatch replayed sync events to renderer after SSE reconnect ([5682813](https://github.com/schlambos/chisel-core/commit/5682813440b8500dc9249fdeef7a31f03a4bdc61))
* **team-mcp:** use fixed server name to stay within 64-char tool limit (ELECTRON-1JY) ([#336](https://github.com/schlambos/chisel-core/issues/336)) ([eaa3aa0](https://github.com/schlambos/chisel-core/commit/eaa3aa098816191d8531ef0f1de12292e5e47cc5))


### Performance Improvements

* **remote/opencode:** batch SSE message.part.delta on 16 ms frame (E03) ([09202f0](https://github.com/schlambos/chisel-core/commit/09202f0132641d34fcc6ec8eefa2bb90eeff1de9))


### Code Refactoring

* **brand:** rename aionui-* crates to chisl-* ([0bcb228](https://github.com/schlambos/chisel-core/commit/0bcb22889abecb6f55b95e01e7fc13ceab1feab8))
* **brand:** rename backend binary aioncore to chislcore ([49b5bce](https://github.com/schlambos/chisel-core/commit/49b5bced2a06725d7d5d72b7201f9213de7a17ed))
* **channels:** remove WeChat/WeCom/DingTalk/Lark plugins, routes, enums, deps ([9ab2872](https://github.com/schlambos/chisel-core/commit/9ab28722efcba22f3ad6e7e6154c3b37dde32048))
* **models:** re-point Qwen/Dashscope fetchers to international endpoint ([45373b3](https://github.com/schlambos/chisel-core/commit/45373b32106e5507326cc4a7e53f70649c688f37))
* **Task18:** add workspace_diff git2 helper to aionui-file ([7255125](https://github.com/schlambos/chisel-core/commit/7255125b67aa527fa5c41491a5164c294b151261))


### Documentation

* **agents:** add Operating Rules (HARD) section ([e506608](https://github.com/schlambos/chisel-core/commit/e506608caff63f53179335d7b93222741000b802))
* **agents:** forbid raising changelog location/untracked status ([81300da](https://github.com/schlambos/chisel-core/commit/81300daf83122f280cd66b51e76924e8ca0c2e14))
* brand backend as Chislcore in prose (keep aioncore binary/commands) ([704e5d1](https://github.com/schlambos/chisel-core/commit/704e5d18cf5a7a1ef1867333bd7c86f280417f40))
* **brand:** add Chisel Core readme ([1651d80](https://github.com/schlambos/chisel-core/commit/1651d80a5a4366c4db491f7af5ad5f6ccd6272c2))
* **brand:** rebrand AionCore docs to Chislcore and chisl-* crates ([5aa756a](https://github.com/schlambos/chisel-core/commit/5aa756a3c7cac24c3bdbbc57820174dd101d6900))
* comprehensive product README (features, protocols, API, architecture) ([a7ed63d](https://github.com/schlambos/chisel-core/commit/a7ed63d2aaea2b104780ae3a4a7b248e44e3fb67))
* fix missed Aion brand references ([0f25077](https://github.com/schlambos/chisel-core/commit/0f25077662c1bff09098e4a275d25312c974ce5a))
* **readme:** sync Chislcore and Chisl branding ([20378b6](https://github.com/schlambos/chisel-core/commit/20378b6fe5fc0e33aea1aecae0885b9ddd672bbd))
* rebrand developer documentation to Chisl ([d5acb0a](https://github.com/schlambos/chisel-core/commit/d5acb0a2f2d2f8cebb49741202fb157889e9447a))
* require changelog update on every commit, add CHANGELOG.md reference ([dd3c030](https://github.com/schlambos/chisel-core/commit/dd3c0301abc67c44b811b2aeb1db1ce3927fa5c5))

## [0.1.11](https://github.com/iOfficeAI/AionCore/compare/v0.1.10...v0.1.11) (2026-05-25)

### Bug Fixes

- **acp:** load user MCP servers and emit empty-finish diagnostic (ELECTRON-1JG) ([#327](https://github.com/iOfficeAI/AionCore/issues/327)) ([2a6c2e9](https://github.com/iOfficeAI/AionCore/commit/2a6c2e943683a72eebaaa1d608be10fe5f795634))
- **acp:** track close reason to avoid reporting user cancel as crash (ELECTRON-1K0) ([#328](https://github.com/iOfficeAI/AionCore/issues/328)) ([9506f9d](https://github.com/iOfficeAI/AionCore/commit/9506f9d1666e26b8659e3339dbfa8f13568f54ce))
- **ai-agent:** rebuild ACP session when CLI rejects stale sid (ELECTRON-1HQ) ([#320](https://github.com/iOfficeAI/AionCore/issues/320)) ([b4d8a75](https://github.com/iOfficeAI/AionCore/commit/b4d8a7505e78c48ed26af364b6e13ad4302b4727))
- **assistant:** default agent_type to aionrs and resolve by provider (ELECTRON-1J1, ELECTRON-1KV) ([#325](https://github.com/iOfficeAI/AionCore/issues/325)) ([5c7fa04](https://github.com/iOfficeAI/AionCore/commit/5c7fa04bef47cf5bf2ea6badc66f723f0aafe1ec))
- **db:** serialize migrations with fs2 file lock to avoid concurrent race (ELECTRON-1KK) ([#329](https://github.com/iOfficeAI/AionCore/issues/329)) ([8550851](https://github.com/iOfficeAI/AionCore/commit/85508518b1df99b48d9ea09f474ed4d64437e8af))
- **extension:** fall back to directory copy when Windows symlink fails (Sentry I1) ([#331](https://github.com/iOfficeAI/AionCore/issues/331)) ([d65a0a1](https://github.com/iOfficeAI/AionCore/commit/d65a0a13449f0941a68adbeae950f094e2545bfe))
- **realtime:** forward id and read nested data in subscribe-show-open ([#323](https://github.com/iOfficeAI/AionCore/issues/323)) ([7dc222f](https://github.com/iOfficeAI/AionCore/commit/7dc222fd444e3869e7b44101fa709e4704ad0a7e))

## [0.1.10](https://github.com/iOfficeAI/AionCore/compare/v0.1.9...v0.1.10) (2026-05-24)

### Miscellaneous

- **deps:** bump aionrs from v0.1.25 to v0.1.26

## [0.1.9](https://github.com/iOfficeAI/AionCore/compare/v0.1.8...v0.1.9) (2026-05-22)

### Features

- **acp,conversation:** elevate ACP protocol + assistant lineage logs to info ([#318](https://github.com/iOfficeAI/AionCore/issues/318)) ([fbcb299](https://github.com/iOfficeAI/AionCore/commit/fbcb29962da5ca4f52516663d592b57815875873))

## [0.1.8](https://github.com/iOfficeAI/AionCore/compare/v0.1.7...v0.1.8) (2026-05-21)

### Features

- add is_full_url flag for provider URL resolution ([#307](https://github.com/iOfficeAI/AionCore/issues/307)) ([3aa15da](https://github.com/iOfficeAI/AionCore/commit/3aa15da0c70a15da097e5bd839b83c4c0c720bf1))

### Bug Fixes

- **ai-agent:** prevent stuck session after ACP cancel ([#313](https://github.com/iOfficeAI/AionCore/issues/313)) ([3a84bfe](https://github.com/iOfficeAI/AionCore/commit/3a84bfec1bfffd589d091efdd7b157ea1c3b2960))
- **runtime:** create node symlink in bundled bun directory (ELECTRON-1EY) ([#310](https://github.com/iOfficeAI/AionCore/issues/310)) ([c0ad26b](https://github.com/iOfficeAI/AionCore/commit/c0ad26bb74008609a8dac815758aabc2284a8066))

## [0.1.7](https://github.com/iOfficeAI/AionCore/compare/v0.1.6...v0.1.7) (2026-05-19)

### Bug Fixes

- **ai-agent:** surface ACP startup crashes and accept work_dir paths (ELECTRON-1BT) ([#305](https://github.com/iOfficeAI/AionCore/issues/305)) ([7aa29a7](https://github.com/iOfficeAI/AionCore/commit/7aa29a78a2fa5013b9a4845217ba89d4b045822b))

## [0.1.6](https://github.com/iOfficeAI/AionCore/compare/v0.1.5...v0.1.6) (2026-05-19)

### Bug Fixes

- **ai-agent:** force-kill ACP processes on Windows (ELECTRON-1E9) ([#303](https://github.com/iOfficeAI/AionCore/issues/303)) ([e60fdd3](https://github.com/iOfficeAI/AionCore/commit/e60fdd31332512398715ed056a7f60eeee42a752))
- **ai-agent:** make find_native_claude cross-platform (ELECTRON-1CG) ([#299](https://github.com/iOfficeAI/AionCore/issues/299)) ([fda9239](https://github.com/iOfficeAI/AionCore/commit/fda92398caa9384d8f0cdc11cf0a3616047448af))
- **ai-agent:** return 409 when remote WS not connected on cancel (ELECTRON-1CV) ([#302](https://github.com/iOfficeAI/AionCore/issues/302)) ([dc87f1c](https://github.com/iOfficeAI/AionCore/commit/dc87f1c37352be6cd820503ed4c38be4098d26ed))

### Documentation

- catch up with aionui-backend → AionCore rename ([#301](https://github.com/iOfficeAI/AionCore/issues/301)) ([40a7e83](https://github.com/iOfficeAI/AionCore/commit/40a7e83618bb62b145378e333e26b66dc0061c89))

## [0.1.5](https://github.com/iOfficeAI/AionCore/compare/v0.1.4...v0.1.5) (2026-05-19)

### Features

- **ai-agent:** add cc-switch provider env injection for Claude ACP ([#291](https://github.com/iOfficeAI/AionCore/issues/291)) ([a7b93e7](https://github.com/iOfficeAI/AionCore/commit/a7b93e7dde78a7b254e26e2d2e25d7b9b885ad5b))

### Bug Fixes

- **channel:** pass model via extra for non-aionrs conversations ([#298](https://github.com/iOfficeAI/AionCore/issues/298)) ([eb65dfe](https://github.com/iOfficeAI/AionCore/commit/eb65dfed2a9f2ea3d9cb11699c276ba76690c03e))

### Code Refactoring

- rename binary from aioncli to aioncore ([#293](https://github.com/iOfficeAI/AionCore/issues/293)) ([ae78cd1](https://github.com/iOfficeAI/AionCore/commit/ae78cd19f599fb3c8845ba5d3e208a75bf310368))

## [0.1.4](https://github.com/iOfficeAI/AionCLI/compare/v0.1.3...v0.1.4) (2026-05-16)

### Features

- **ai-agent:** log every CLI detection + add doctor subcommand ([#285](https://github.com/iOfficeAI/AionCLI/issues/285)) ([5ef6d0a](https://github.com/iOfficeAI/AionCLI/commit/5ef6d0a4d99345a502a9073dfdfa0d07cfa52a8c))
- **runtime:** full shell-style command in spawn logs ([#278](https://github.com/iOfficeAI/AionCLI/issues/278)) ([dd51616](https://github.com/iOfficeAI/AionCLI/commit/dd516165ae9e22fcb0573ae9d8d3aa094e54cff2))

### Bug Fixes

- **ai-agent:** negotiate OpenClaw protocol v3..v4 ([#288](https://github.com/iOfficeAI/AionCLI/issues/288)) ([dfeece0](https://github.com/iOfficeAI/AionCLI/commit/dfeece0e6a465093090c0efdfa1f5aa93d9fa6e8))
- **team:** model routing + schema unification + lazy warm mode persistence ([#286](https://github.com/iOfficeAI/AionCLI/issues/286)) ([199a392](https://github.com/iOfficeAI/AionCLI/commit/199a392caca600ef215bb2ae71bfd82bda7bb744))

### Performance Improvements

- **team:** lazy warm — only start agent processes on first message ([#282](https://github.com/iOfficeAI/AionCLI/issues/282)) ([6281f31](https://github.com/iOfficeAI/AionCLI/commit/6281f31ac6a2656c1af51891589770f4583e00c2))

### Code Refactoring

- **app:** extract CLI definitions to cli.rs ([#280](https://github.com/iOfficeAI/AionCLI/issues/280)) ([5685d52](https://github.com/iOfficeAI/AionCLI/commit/5685d5237b8f51c70e80895b1c654325c958196e))
- **app:** introduce commands/ module with layered bootstrap for subcommands ([#283](https://github.com/iOfficeAI/AionCLI/issues/283)) ([1216597](https://github.com/iOfficeAI/AionCLI/commit/12165971cfae61d85376c102ef9f9afc5a7c5bbf))
- **app:** replace argv sniffing with clap Subcommand for mcp-\* helpers ([#277](https://github.com/iOfficeAI/AionCLI/issues/277)) ([c3d137c](https://github.com/iOfficeAI/AionCLI/commit/c3d137c9e5fdcb12e29d5ca7abd6a0585bbc6c8d))
- **app:** split monolithic lib.rs/main.rs into per-module files ([#284](https://github.com/iOfficeAI/AionCLI/issues/284)) ([f3462cb](https://github.com/iOfficeAI/AionCLI/commit/f3462cbb1d6d830a3a368a76b2d9ea6424f21b64))
- rename binary from aionui-backend to aioncli ([#289](https://github.com/iOfficeAI/AionCLI/issues/289)) ([30eeca3](https://github.com/iOfficeAI/AionCLI/commit/30eeca37661441ba9474aa7dc51ca911abda0bfb))

## [0.1.3](https://github.com/iOfficeAI/aionui-backend/compare/v0.1.2...v0.1.3) (2026-05-15)

### Bug Fixes

- **acp:** apply AvailableCommands event to session aggregate ([#270](https://github.com/iOfficeAI/aionui-backend/issues/270)) ([a46b561](https://github.com/iOfficeAI/aionui-backend/commit/a46b561b20421a59fd73e9629ef452c624781ef2))
- **assistant:** pin user_data_dir to runtime --data-dir ([#274](https://github.com/iOfficeAI/aionui-backend/issues/274)) ([0d49022](https://github.com/iOfficeAI/aionui-backend/commit/0d49022f90d7950e00e0dfdb60e389116177182d))
- **db:** cast REAL timestamps to INTEGER in conversations table ([#275](https://github.com/iOfficeAI/aionui-backend/issues/275)) ([92e5fa9](https://github.com/iOfficeAI/aionui-backend/commit/92e5fa9f75065b85b5533476d0fbb836b0145b4e))
- **runtime:** make CLI detection work on Windows ([#276](https://github.com/iOfficeAI/aionui-backend/issues/276)) ([35bd121](https://github.com/iOfficeAI/aionui-backend/commit/35bd1217425a2e0d51f3e8f8e2f53ea37151c1eb))
- **team:** pass workspace from CreateTeamRequest to agent conversations ([#273](https://github.com/iOfficeAI/aionui-backend/issues/273)) ([f4e3f32](https://github.com/iOfficeAI/aionui-backend/commit/f4e3f32e3a1a9f8fa34769205fa031b6037af00e))

## [0.1.2](https://github.com/iOfficeAI/aionui-backend/compare/v0.1.1...v0.1.2) (2026-05-14)

### Features

- **aionrs:** expose slash commands API ([c9d30ca](https://github.com/iOfficeAI/aionui-backend/commit/c9d30ca63b7840fd997048bb4ffbe1b4976eb63c))
- **aionrs:** expose slash commands via get_slash_commands() ([e6e120a](https://github.com/iOfficeAI/aionui-backend/commit/e6e120a883c522a045360325b325a81033c9d28d))
- **cli:** add --work-dir argument for conversation workspaces ([ed2d394](https://github.com/iOfficeAI/aionui-backend/commit/ed2d3942582245b243d7ab0e25175528a5db7d40))
- **cli:** add --work-dir argument for conversation workspaces ([fdfbbf5](https://github.com/iOfficeAI/aionui-backend/commit/fdfbbf5e36658f6aa4454f3cb5c38332a93f544b))

### Bug Fixes

- **ai-agent:** surface upstream ACP error messages without status prefix ([#268](https://github.com/iOfficeAI/aionui-backend/issues/268)) ([532f7e3](https://github.com/iOfficeAI/aionui-backend/commit/532f7e3bbee7e8389499f4d7bbda198c22363e13))
- **aionrs:** abort engine.run() on cancel ([9eeb0a8](https://github.com/iOfficeAI/aionui-backend/commit/9eeb0a8620d10a3e2de74fa9d37907f3c8ab043a))
- **aionrs:** abort engine.run() on cancel instead of only emitting events ([74024c3](https://github.com/iOfficeAI/aionui-backend/commit/74024c3af6a8277588c4dd28e8453e1822789e15))
- **ci:** allow too_many_arguments on JobExecutor::new ([26918a0](https://github.com/iOfficeAI/aionui-backend/commit/26918a04b265a73298e216bda504b79bd47c852a))
- **ci:** auto-update Cargo.lock in release-please PR ([a3d6147](https://github.com/iOfficeAI/aionui-backend/commit/a3d614713cf0999f2471472dcfa6a8af4f9c0b8f))
- **ci:** auto-update Cargo.lock in release-please PR ([91f4495](https://github.com/iOfficeAI/aionui-backend/commit/91f44956ed24c8cb370d4ea71d9f62cd29e09fe7))
- **ci:** resolve clippy warnings in aionui-api-types and aionui-realtime ([7b8c1c8](https://github.com/iOfficeAI/aionui-backend/commit/7b8c1c82976284b149195ae67707a1d62bf01f0f))
- **conversation:** kill agent process on conversation delete ([#267](https://github.com/iOfficeAI/aionui-backend/issues/267)) ([456ff32](https://github.com/iOfficeAI/aionui-backend/commit/456ff322845b96fd70583dcf1fc2fb12c2371030))
- **runtime:** include nvm node bins in startup path ([#261](https://github.com/iOfficeAI/aionui-backend/issues/261)) ([00c5762](https://github.com/iOfficeAI/aionui-backend/commit/00c57627592a567eb71fbc4edc564e2b579b86ee))

### Code Refactoring

- **acp:** replace first-message flag with PromptPipeline + hooks ([#262](https://github.com/iOfficeAI/aionui-backend/issues/262)) ([d1f3c95](https://github.com/iOfficeAI/aionui-backend/commit/d1f3c95eebea4053c45b56dcd973fe4e44f0fe6c))

## [0.1.1](https://github.com/iOfficeAI/aionui-backend/compare/v0.1.0...v0.1.1) (2026-05-13)

### Features

- **logging:** integrate aionrs independent file logging ([da16d97](https://github.com/iOfficeAI/aionui-backend/commit/da16d97975202808c2b24ea884dff6f43c2de4d3))
- **logging:** integrate aionrs independent file logging ([dc950c8](https://github.com/iOfficeAI/aionui-backend/commit/dc950c8781b3f5fdc4aaa435c9f69e27b079ccb2))

### Bug Fixes

- **office:** stabilize flaky port_timeout_on_no_listener test ([30df119](https://github.com/iOfficeAI/aionui-backend/commit/30df119eec0ae5b125b2613d4573b6432ed42094))
- revert console_layer to match main (remove .with_ansi(false)) ([e1dfe73](https://github.com/iOfficeAI/aionui-backend/commit/e1dfe73db029685bac99f2f293cfab586db1f0b1))
- **team:** remove 30s heartbeat polling from agent event loop ([752be98](https://github.com/iOfficeAI/aionui-backend/commit/752be981a487c1281fee48bf0b21d4d9c1574bbf))
- **team:** remove redundant 30s heartbeat polling from event loop ([88672eb](https://github.com/iOfficeAI/aionui-backend/commit/88672ebb59aa9eb25e3396ed312bd1d807df4e07))

### Code Refactoring

- **ai-agent,conversation:** move session ops, tighten visibility, fix idle scanner + backfill ACP metadata ([#254](https://github.com/iOfficeAI/aionui-backend/issues/254)) ([299c5d3](https://github.com/iOfficeAI/aionui-backend/commit/299c5d30e7674d91136139886c9b02a99b932515))

### Documentation

- **assistants:** add word-form-creator to preset-id-whitelist ([#252](https://github.com/iOfficeAI/aionui-backend/issues/252)) ([343b15b](https://github.com/iOfficeAI/aionui-backend/commit/343b15bc5ab362c566ae0d8e2ed61921d58b9497))
