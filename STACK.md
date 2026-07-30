# NBA Database Agent - Architecture & Stack Specification

## Overview
An advanced, production-grade AI Agent built in Rust for deep analytical interaction with large DuckDB databases (specifically the 17.3 GB, 588-table NBA Warehouse). Powered by OpenRouter (`qwen/qwen3.7-flash`), Axum, and a rich tool-calling execution loop.

---

## 1. Tech Stack Selection

| Component | Library / Tool | Version | Purpose |
|---|---|---|---|
| **Language** | Rust | Edition 2024 | Memory safety, high throughput, zero-overhead concurrency |
| **Web Server** | `axum` | 0.8.x | High-performance async HTTP framework with native SSE support |
| **Async Runtime** | `tokio` | 1.x (full) | Non-blocking I/O and task dispatching |
| **Database Engine** | `duckdb` | 1.10505.0 | Embedded columnar database engine with zero-copy Arrow support |
| **LLM Client** | `async-openai` | 0.23.4 | OpenAI API client configured for OpenRouter endpoint |
| **LLM Model** | `qwen/qwen3.7-flash` | via OpenRouter | 1M context window multimodal reasoning model |
| **Streaming** | Server-Sent Events (SSE) | Native Axum | Real-time token generation & tool execution stream |
| **Serialization** | `serde` & `serde_json` | 1.0.x | Type-safe JSON request/response encoding |
| **State & Session** | In-Memory Arc<DashMap/RwLock> | Custom | Multi-turn conversation state tracking across requests |
| **Frontend UI** | Single Page Application | HTML5 / Tailwind / Chart.js / Marked | Scoreboard dark-theme chat UI with interactive charts & table previews |
| **Dev Tooling** | `cargo clippy`, `rustfmt`, `dotenvy` | Standard | CI/CD readiness, linting rules, environment management |

---

## 2. Agent Toolset Architecture

Rather than dumping all 588 tables into the system prompt (which bloats context and causes hallucinations), the agent uses an **agentic exploration pattern**:

1. **`search_tables`**: Performs keyword and semantic pattern matching across table names and columns to discover relevant schema elements.
2. **`describe_table`**: Inspects a specific table's schema (column names, types, sample rows, nullability) on demand.
3. **`run_sql`**: Executes sanitized SQL queries against DuckDB (capped to 50 rows per preview with row count metadata).
4. **`explain_query`**: Validates SQL syntax and query execution plan without running expensive scans.
5. **`generate_chart`**: Formats query results into Chart.js visualization specifications (bar, line, scatter, pie).

---

## 3. Resilience & Error Self-Correction Loop

- **Self-Healing SQL Loop**: If a query returns a DuckDB SQL syntax error or missing table/column error, the agent receives the error string and automatically attempts query modification (up to 12 iterations).
- **Reasoning Handling**: Configured with `reasoning: { exclude: false }` or graceful fallback handling for OpenRouter reasoning tokens.
- **Read-Only Safety**: Database handles are opened with `AccessMode::ReadOnly` to ensure queries cannot alter warehouse data.

---

## 4. API Endpoints

- `GET /` — Serves the interactive web interface.
- `POST /api/chat` — Synchronous chat endpoint (returns full `ConversationTrace`).
- `GET /api/chat/stream?message=...&session_id=...` — Real-time SSE event stream (`text/event-stream`).
- `POST /api/reset` — Resets session conversation history.
- `GET /api/health` — Health check endpoint returning DuckDB status & table count.
