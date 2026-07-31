# db-analyze-agent (NBA Database Agent)

A Rust agent that answers natural-language questions about NBA stats by driving a tool-calling LLM (via OpenRouter) against a large, read-only DuckDB warehouse. Chat, streaming, and a small analysis toolset live behind an Axum HTTP API with a self-contained SPA frontend.

See [`STACK.md`](STACK.md) for the full architecture/stack breakdown.

## What it does

- Exposes a chat API (`/api/chat`, `/api/chat/stream` via SSE) backed by an LLM given 6 tools to explore the schema and run SQL:
  `run_sql`, `list_tables`, `search_tables`, `describe_table`, `explain_query`, `generate_chart`.
- Auto-heals common SQL mistakes: on a DuckDB "column not found" error, it parses the engine's own `Candidate bindings` suggestion and retries with the corrected identifier (AST rewrite via `sqlparser`, falling back to string replacement).
- Persists conversations to `data/sessions.json` across restarts, and can export any session as a Markdown report (`/api/export?session=...`).
- Generates pre-computed "insight cards" at startup (total games, players, etc.) surfaced via `/api/insights` and folded into the system prompt.
- Enforces a hard 12-iteration reasoning cap per turn and a 20-message sliding context window per session, so no single request can run away.

## Requirements

- Rust (edition 2024 toolchain; CI pins `stable` via `dtolnay/rust-toolchain`)
- A DuckDB file at `../data/nba-data.duckdb` relative to the `nba-agent` crate (i.e. `data/nba-data.duckdb` at the repo root) — the connection opens `AccessMode::ReadOnly`
- An OpenRouter (or OpenAI-compatible) API key

## Setup

```sh
cp .env.example .env
# edit .env and set OPENROUTER_API_KEY
cd nba-agent
cargo run
```

The server listens on `http://localhost:3000` (hardcoded — see caveat below) and serves the SPA from `nba-agent/static/` as a fallback route.

### Environment variables

| Variable | Read by | Required | Notes |
|---|---|---|---|
| `OPENROUTER_API_KEY` | `agent.rs` | Yes (or `OPENAI_API_KEY`) | Bearer token for the OpenRouter chat-completions API |
| `OPENAI_API_KEY` | `agent.rs` | Fallback | Used only if `OPENROUTER_API_KEY` is unset |
| `OPENROUTER_BASE_URL` | `agent.rs` | No | Defaults to `https://openrouter.ai/api/v1/chat/completions` |
| `RUST_LOG` | `tracing_subscriber` | No | Standard env-filter syntax, e.g. `info,nba_agent=debug` |
| `DATABASE_PATH` | test harness only | No | Only consulted by `tests/*.rs`; the running server always opens the hardcoded `../data/nba-data.duckdb` |

> **Caveat:** `.env.example` also lists `DATABASE_PATH` and `PORT` as server config, but `nba-agent/src/main.rs` does not read either — the DB path and the `0.0.0.0:3000` bind address are hardcoded. Only the tests consult `DATABASE_PATH`.

## API

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | SPA (`static/index.html`) |
| `/api/chat` | POST | Synchronous chat turn, returns full `ConversationTrace` |
| `/api/chat/stream?message=...&session_id=...` | GET | SSE stream of step/tool/token events |
| `/api/reset` | POST | Clears a session's history |
| `/api/health` | GET | Liveness + DB status |
| `/api/test-query` | GET | Diagnostic fixed query |
| `/api/insights` | GET | Pre-computed insight cards |
| `/api/export?session=...` | GET | Downloads the session as a Markdown report |
| `/api/sessions` | GET | Active session IDs + message counts |
| `/api/history` | GET | Recent SQL query history (last 50) |
| `/api/stats` | GET | Aggregate runtime/query stats |

All routes except the static fallback pass through a 60-requests/60-second rate limiter, gzip compression, and permissive CORS.

## Development

```sh
cd nba-agent
cargo fmt --check      # rustfmt.toml: edition 2024, max_width 120
cargo clippy -- -D warnings   # clippy.toml: MSRV 1.85.0, cognitive-complexity 25
cargo check
cargo test
```

These are exactly the steps CI (`.github/workflows/ci.yml`) runs on every push/PR to `main`/`master`. Coverage is tracked with `cargo-tarpaulin` (`nba-agent/cobertura.xml`, `nba-agent/tarpaulin-report.html`).

Tests live in `nba-agent/tests/` (8 files, ~1,870 lines): unit tests against a live/temp DuckDB file, plus router-level integration tests that mock the OpenRouter API with `wiremock` and drive the Axum app through `tower::ServiceExt`.
