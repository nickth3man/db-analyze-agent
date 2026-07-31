# AGENTS.md

## Project identity

- Project: `db-analyze-agent` (Cargo package `nba-agent`)
- Purpose: A Rust/Axum service that answers natural-language NBA questions by driving a tool-calling LLM (OpenRouter) against a large read-only DuckDB warehouse, with a self-contained SPA frontend.
- Primary language/runtime: Rust, edition 2024; clippy MSRV `1.85.0`; CI pins `dtolnay/rust-toolchain@stable`. Async runtime is Tokio.
- Default branch: `main` (CI also builds `master`; remote is `origin` → `https://github.com/nickth3man/db-analyze-agent.git`)
- Package/build system: Cargo. The crate lives in `nba-agent/`, **not** at the repo root — every cargo command must run from `nba-agent/`.

## Repository architecture

```text
nba-agent/                     — the only Cargo package; all Rust code and its config live here
nba-agent/src/main.rs          — binary entrypoint: env_config(), build_app(), dotenvy + tracing init, TCP bind
nba-agent/src/lib.rs           — HTTP layer: AppState, build_state(), build_router(), all route handlers,
                                 RateLimiter (60 req / 60 s, XFF honored only for TRUSTED_PROXIES),
                                 ApiAuth bearer middleware, CORS allowlist, gzip compression
nba-agent/src/agent.rs         — LLM reasoning loop: system prompt, 19 tool schemas, tool dispatch,
                                 OpenRouter chat-completions client (sync + SSE streaming),
                                 session store persisted to SESSIONS_PATH, sliding context window,
                                 MAX_ITERATIONS reasoning cap
nba-agent/src/db.rs            — DbContext: r2d2-pooled DuckDB (AccessMode::ReadOnly, max_size 16),
                                 moka result cache, query history, insight-card generation,
                                 SQL auto-heal (sqlparser AST rewrite from DuckDB "Candidate bindings")
nba-agent/static/index.html    — the entire SPA; served via ServeDir fallback (no build step, no bundler)
nba-agent/tests/               — 12 integration test binaries (~3,550 lines): live/temp DuckDB unit tests
                                 plus router-level tests using wiremock + tower::ServiceExt
nba-agent/.cargo/config.toml   — sets RUST_TEST_THREADS=2 (libtest only) to bound DuckDB memory in tests
nba-agent/rustfmt.toml         — edition 2024, max_width 120, use_small_heuristics = "Max"
nba-agent/clippy.toml          — msrv 1.85.0, cognitive-complexity-threshold 25
data/nba-data.duckdb           — 18 GB read-only warehouse; gitignored, NOT provisioned by this repo
data/sessions.json             — persisted chat sessions (runtime artifact, default SESSIONS_PATH)
.tasks/                        — task lifecycle system (see "Task system")
.github/workflows/ci.yml       — the single CI workflow
.research/project-assessment/  — background research notes; not part of the build
README.md                      — user-facing overview; currently STALE (see "Known documentation drift")
```

Runtime boundaries: everything is one process. The LLM is the only outbound network dependency
(`OPENROUTER_BASE_URL`). The database is opened read-only — no migrations, no writes, no schema
ownership. Deployment unit is the single `nba-agent` binary plus the `static/` directory, which must
be present in the process working directory.

Public interfaces (all under the rate limiter, gzip, CORS, and — when `API_KEY` is set — bearer auth):

```text
POST /api/chat                 — synchronous turn, returns the full ConversationTrace
GET  /api/chat/stream          — SSE stream of step/tool/token events
POST /api/reset                — clear a session's history
GET  /api/health               — liveness + DB status
GET  /api/test-query           — fixed diagnostic query
GET  /api/insights             — pre-computed insight cards
GET  /api/export?session=...   — session as a Markdown report
GET  /api/history              — recent SQL query history
GET  /api/stats                — aggregate runtime/query stats
POST /api/feedback             — user feedback on a turn
GET  /*                        — ServeDir("static") fallback
```

Agent tools registered in `agent.rs` (19): `run_sql`, `list_tables`, `search_tables`,
`describe_table`, `explain_query`, `generate_chart`, `compare_players`, `compare_teams`,
`find_streaks`, `get_player_profile`, `rank_performance`, `find_leaders`, `get_game_summary`,
`get_head_to_head`, `check_data_coverage`, `export_query_result`, `era_adjusted_compare`,
`game_reconstruction`, `expand_player_profile`. Adding or renaming a tool means touching the schema
list, the dispatch match, and the system prompt in `agent.rs` — all three, or the tool silently
misbehaves.

## Required development commands

All commands run from `nba-agent/`.

```text
Install:     Not applicable — `cargo` resolves dependencies on first build. DuckDB is vendored
             (`features = ["bundled"]`), so no system libduckdb is needed. A C/C++ toolchain is
             required to compile it.
Build:       cargo build            (CI uses `cargo check`)
Format:      cargo fmt              (CI gate: `cargo fmt --check`)
Lint:        cargo clippy -- -D warnings
Type-check:  cargo check            (Rust has no separate type-check step)
Unit tests:  cargo test --lib --bins        (the `#[cfg(test)]` modules inside src/)
Integration: cargo test --tests             (nba-agent/tests/*.rs)
End-to-end:  cargo test --test coverage_gap_e2e
             — router-level E2E through tower::ServiceExt with a wiremock OpenRouter stub.
             There is no browser/UI E2E suite.
Run locally: cargo run              (serves http://0.0.0.0:3000)
```

Notes that will bite you if ignored:

- `cargo test` requires `data/nba-data.duckdb` for the DB-backed tests. Tests that need it skip
  cleanly when the file is absent, so a green run on a machine without the warehouse proves less
  than it looks. Say which case you were in when reporting results.
- `RUST_TEST_THREADS=2` is set in `nba-agent/.cargo/config.toml` on purpose. Each test thread opens
  its own DuckDB instance against an 18 GB file. Raising it can thrash or freeze the machine.
- Coverage is measured with `cargo-tarpaulin`; the committed reports are
  `nba-agent/cobertura.xml` and `nba-agent/tarpaulin-report.html`.

## Coding conventions

- Follow existing style and architecture before introducing a new pattern.
- Keep changes scoped to the approved task.
- Preserve public compatibility unless the approved plan explicitly changes it.
- Do not introduce dependencies, generated files, migrations, or configuration changes without documenting them in the task plan.
- Add or update tests for changed behavior.
- Update documentation when behavior, configuration, APIs, or developer workflows change.

Repository-specific:

- `cargo fmt --check` and `cargo clippy -- -D warnings` are hard CI gates. Never commit code that
  fails either; never silence clippy with a blanket `#[allow]` when the lint is correct.
- Configuration is read from the environment at construction time with a hardcoded default
  (`std::env::var("X").unwrap_or_else(...)`). Follow that shape; do not add a config-file layer.
- The DuckDB connection is `AccessMode::ReadOnly`. Any code path that writes to the warehouse is a bug.
- SQL results are row-capped (`ROW_CAP`, default 50) and cached (moka, `CACHE_MAX_CAPACITY` /
  `CACHE_TTL_SECS`). Preserve the cap when adding query paths — it is what keeps a 18 GB warehouse
  from being pulled into a chat response.
- Tests use `wiremock` to stub OpenRouter and `tower::ServiceExt` to drive the router in-process.
  Never let a test reach the real OpenRouter API.
- The frontend is a single hand-written `static/index.html`. Do not add a bundler, framework, or
  build step without an approved plan.

### Environment variables

| Variable | Read in | Default | Notes |
|---|---|---|---|
| `OPENROUTER_API_KEY` | `agent.rs` | — | Required unless `OPENAI_API_KEY` is set; startup fails without one |
| `OPENAI_API_KEY` | `agent.rs` | — | Fallback for the above |
| `OPENROUTER_BASE_URL` | `agent.rs` | `https://openrouter.ai/api/v1/chat/completions` | |
| `MODEL` | `agent.rs` | `qwen/qwen3.7-flash` | |
| `MAX_ITERATIONS` | `agent.rs` | `12` | Reasoning-loop cap per turn |
| `DATABASE_PATH` | `main.rs`, tests | `../data/nba-data.duckdb` | Relative to `nba-agent/` |
| `BIND_ADDRESS` | `main.rs` | `0.0.0.0` | |
| `PORT` | `main.rs` | `3000` | |
| `SESSIONS_PATH` | `main.rs` | `data/sessions.json` | |
| `API_KEY` | `lib.rs` | unset | Unset ⇒ unauthenticated dev mode (warns at startup) |
| `TRUSTED_PROXIES` | `lib.rs` | unset | Comma-separated CIDRs; `X-Forwarded-For` is trusted only from these |
| `CORS_ALLOWED_ORIGINS` | `lib.rs` | unset | Unset ⇒ permissive dev mode |
| `ROW_CAP` | `db.rs` | `50` | Max rows per SQL result |
| `DUCKDB_MEMORY_LIMIT` | `db.rs` | `2GB` | Per DuckDB instance |
| `CACHE_MAX_CAPACITY` | `db.rs` | `200` | |
| `CACHE_TTL_SECS` | `db.rs` | `60` | |
| `RUST_LOG` | `tracing_subscriber` | — | e.g. `info,nba_agent=debug` |

`nba-agent/.env.example` is the accurate template. The root `.env.example` is a stale subset.

### Known documentation drift

Verify against source before trusting these; fix them when a task touches the relevant area.

- `README.md` claims 6 agent tools; `agent.rs` registers 19.
- `README.md` claims the DB path and `0.0.0.0:3000` bind are hardcoded; `main.rs::env_config()` reads
  `DATABASE_PATH`, `BIND_ADDRESS`, and `PORT`.
- `README.md` lists `GET /api/sessions`; that route does not exist. `POST /api/feedback` exists and is
  undocumented there.
- `README.md` links `STACK.md`, which is not present in the repository.

## Repository safety

- Never discard, overwrite, stash, commit, or absorb unrelated user changes.
- Never modify secrets, credentials, production data, or deployment settings unless the approved task explicitly requires it.
- Do not run destructive commands without explicit user authorization.
- Do not bypass tests, branch protection, required checks, or security controls.
- Stop and ask when repository state is ambiguous or unsafe.

Repository-specific:

- `data/nba-data.duckdb` is an 18 GB irreplaceable local asset that this repo cannot regenerate.
  Never delete, move, truncate, or open it read-write.
- Never commit `.env`, real API keys, or `data/sessions.json` (may contain user chat content).
- Do not raise `RUST_TEST_THREADS` or `DUCKDB_MEMORY_LIMIT` without understanding the memory
  arithmetic in `nba-agent/.cargo/config.toml`; the failure mode is freezing the host.

## Dependencies and generated artifacts

- **Adding/upgrading dependencies:** edit `nba-agent/Cargo.toml`, justify it in the task plan.
  `duckdb` is pinned with `features = ["bundled", "r2d2"]` — changing that feature set changes the
  build's system requirements and is a plan-level decision, not an incidental edit.
- **Lockfiles:** `nba-agent/Cargo.lock` is committed and must be committed alongside any dependency
  change. Never hand-edit it; let cargo regenerate it.
- **Generated code:** none. No build scripts, codegen, or macro-expansion artifacts are checked in.
- **Database migrations:** Not applicable — the warehouse is read-only and owned externally.
- **API or schema generation:** Not applicable — routes and JSON shapes are hand-written with
  `serde`; the LLM tool schemas are hand-written JSON literals in `agent.rs`.
- **Vendored files:** DuckDB's C++ source is vendored by the `duckdb` crate's `bundled` feature, not
  by this repository. Nothing is vendored in-tree.
- **Binary assets:** none in git. `data/*.duckdb`, `target/`, `*.log`, and `.env` are gitignored.
  `nba-agent/cobertura.xml` and `nba-agent/tarpaulin-report.html` are committed coverage reports —
  regenerate rather than hand-edit, and only refresh them when the task is about coverage.

## Git and GitHub conventions

- Follow the task branch and commit rules in `.tasks/AGENTS.md`.
- Branch pattern: `task/{id}-{slug}`; commit subject pattern: `{id}: {summary}` (from `.tasks/config.yaml`).
- Repository-specific merge strategy: `repository_default`, with `squash` as the configured fallback.
- Required GitHub workflows: `CI` (`.github/workflows/ci.yml`) — runs `cargo fmt --check`,
  `cargo clippy -- -D warnings`, `cargo check`, `cargo test` on push and PR to `main`/`master`.
  `.tasks/config.yaml` sets `github.required_checks: []`, so treat CI as advisory-but-mandatory in
  practice: do not merge on red.
- PR template or labels: Not applicable — none defined in the repository.
- Release-note requirement: Not applicable — no release process or changelog is defined.

## Task system

All feature, bug-fix, refactor, performance, security, audit, research, documentation, dependency, data, UI/UX, and scaffolding work must use the lifecycle in `.tasks/AGENTS.md` unless `.tasks/config.yaml` contains an explicit repository override.

`task.yaml` is authoritative for task state. Task Markdown files provide the human-readable record and evidence. Repository rules in this file and lifecycle rules in `.tasks/AGENTS.md` are both mandatory. If they conflict, stop and ask the user rather than choosing silently.

Current state: task system version `1.0.0`, ID format `TASK-{year}-{NNN}`, `.tasks/active` and
`.tasks/archive` are empty, and `.tasks/index.yaml` is an empty generated view. The next task is the
first one.

## Repository-specific task overrides

`.tasks/config.yaml` sets `overrides: {}` — there are no lifecycle overrides. The full lifecycle
profile applies (`lifecycle.full_profile_required: true`), with chat approvals required at
`findings`, `plan`, `pull_request`, and `merge`.

Two config values are unset placeholders rather than deliberate overrides, and should be corrected
in a task that touches the task system:

- `repository.name: "replace-with-repository-name"`: still the template default; should be
  `db-analyze-agent`.
- `commands.{format,lint,typecheck,unit_test,integration_test,end_to_end_test,build}: null`: the real
  commands are listed under "Required development commands" above. They are prose here only because
  the config has not been filled in; the machine-readable values belong in `.tasks/config.yaml`.
