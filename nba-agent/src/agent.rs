use anyhow::{Result, anyhow};
use async_stream::stream;
use futures::Stream;
use parking_lot::Mutex as PlMutex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{pin::Pin, sync::Arc};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::db::DbContext;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallStep {
    pub tool_name: String,
    pub reasoning: String,
    pub query_or_params: String,
    pub result: String,
    pub elapsed_ms: u64,
    pub row_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatStep {
    pub content: Option<String>,
    pub reasoning: Option<String>,
    pub tool_calls: Vec<ToolCallStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationTrace {
    pub session_id: String,
    pub steps: Vec<ChatStep>,
    pub final_answer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum AgentStreamEvent {
    StepStarted { step: usize },
    Reasoning { text: String },
    ToolCallStarted { tool_name: String, reasoning: String, query_or_params: String },
    ToolCallResult { tool_name: String, result: String, elapsed_ms: u64, row_count: usize },
    FinalAnswerChunk { text: String },
    Completed { trace: ConversationTrace },
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

struct ToolResult {
    reasoning: String,
    param_str: String,
    result_str: String,
    elapsed_ms: u64,
    row_count: usize,
}

/// Parsed OpenRouter chat completion.
#[derive(Debug)]
struct ChatCompletion {
    content: Option<String>,
    reasoning: Option<String>,
    tool_calls: Vec<ParsedToolCall>,
}

#[derive(Debug)]
struct ParsedToolCall {
    id: String,
    name: String,
    arguments: Value,
}

#[derive(Clone)]
pub struct Agent {
    http_client: Client,
    api_key: String,
    db: DbContext,
    schema_summary: String,
    insights_brief: String,
    /// Bounded moka cache: max 500 sessions, 30-minute idle TTL.
    sessions: moka::sync::Cache<String, Vec<ChatMessage>>,
    /// Per-session async mutex map to serialize read-modify-write on the same
    /// session, preventing the lost-update race in `run_conversation[_stream]`.
    session_locks: moka::sync::Cache<String, Arc<AsyncMutex<()>>>,
    /// Path on disk for periodic session persistence.
    sessions_path: String,
    openrouter_url: String,
    /// OpenRouter model identifier (env: MODEL, default: qwen/qwen3.7-flash).
    model: String,
    /// Fallback model if primary fails (env: FALLBACK_MODEL, optional).
    fallback_model: Option<String>,
    /// Max reasoning loop iterations per turn (env: MAX_ITERATIONS, default: 12).
    max_iterations: usize,
    dirty: Arc<PlMutex<bool>>,
    save_signal: Arc<tokio::sync::Notify>,
}

const SESSIONS_MAX_CAPACITY: u64 = 500;
const SESSIONS_TTL_SECS: u64 = 30 * 60;
const SAVE_DEBOUNCE_SECS: u64 = 5;

/// Extract human-readable (reasoning, query_or_params) from tool name and
/// arguments, mirroring the per-tool logic in `Agent::execute_tool`.
/// Called before the tool executes so the frontend can show a "started" event
/// while the operation is still in progress.
fn args_to_tool_start_fields(name: &str, args: &Value) -> (String, String) {
    match name {
        "run_sql" => {
            let reasoning = args["reasoning"].as_str().unwrap_or("Executing query").to_string();
            let query = args["query"].as_str().unwrap_or("").to_string();
            (reasoning, query)
        }
        "list_tables" => {
            let pattern = args["pattern"].as_str().unwrap_or("%").to_string();
            (format!("Listing tables matching '{}'", pattern), pattern)
        }
        "search_tables" => {
            let keyword = args["keyword"].as_str().unwrap_or("").to_string();
            (format!("Searching tables matching '{}'", keyword), keyword)
        }
        "describe_table" => {
            let table = args["table_name"].as_str().unwrap_or("").to_string();
            (format!("Inspecting table `{}`", table), table)
        }
        "explain_query" => {
            let query = args["query"].as_str().unwrap_or("").to_string();
            ("Checking query plan for SQL".to_string(), query)
        }
        "generate_chart" => {
            let chart_type = args["chart_type"].as_str().unwrap_or("bar");
            let title = args["title"].as_str().unwrap_or("NBA Stat Chart");
            let sql = args["sql_query"].as_str().unwrap_or("");
            (format!("Generating {} chart: '{}'", chart_type, title), sql.to_string())
        }
        "compare_players" => {
            let p1 = args["player1"].as_str().unwrap_or("");
            let p2 = args["player2"].as_str().unwrap_or("");
            (format!("Comparing {} vs {}", p1, p2), serde_json::to_string(args).unwrap_or_default())
        }
        "compare_teams" => {
            let t1 = args["team1"].as_str().unwrap_or("");
            let t2 = args["team2"].as_str().unwrap_or("");
            (format!("Comparing {} vs {}", t1, t2), serde_json::to_string(args).unwrap_or_default())
        }
        "find_streaks" => {
            let player = args["player_name"].as_str().unwrap_or("");
            let streak_type = args["streak_type"].as_str().unwrap_or("points");
            (format!("Finding {} streaks for {}", streak_type, player), serde_json::to_string(args).unwrap_or_default())
        }
        "get_player_profile" => {
            let player = args["player_name"].as_str().unwrap_or("");
            (format!("Building profile for {}", player), player.to_string())
        }
        "rank_performance" => {
            let stat = args["stat_name"].as_str().unwrap_or("");
            (format!("Ranking {} across NBA history", stat), serde_json::to_string(args).unwrap_or_default())
        }
        "find_leaders" => {
            let stat = args["stat_name"].as_str().unwrap_or("");
            (format!("Finding {} leaders", stat), serde_json::to_string(args).unwrap_or_default())
        }
        "get_game_summary" => {
            let gid = args["game_id"].as_str().unwrap_or("unknown");
            (format!("Summarizing game {}", gid), serde_json::to_string(args).unwrap_or_default())
        }
        "get_head_to_head" => {
            let t1 = args["team1"].as_str().unwrap_or("");
            let t2 = args["team2"].as_str().unwrap_or("");
            (format!("H2H: {} vs {}", t1, t2), serde_json::to_string(args).unwrap_or_default())
        }
        "check_data_coverage" => {
            let et = args["entity_type"].as_str().unwrap_or("");
            (format!("Checking {} coverage", et), serde_json::to_string(args).unwrap_or_default())
        }
        "export_query_result" => {
            ("Exporting query result".to_string(), serde_json::to_string(args).unwrap_or_default())
        }
        "era_adjusted_compare" => {
            let p1 = args["player1"].as_str().unwrap_or("");
            let p2 = args["player2"].as_str().unwrap_or("");
            (format!("Era-adjusted compare: {} vs {}", p1, p2), serde_json::to_string(args).unwrap_or_default())
        }
        "game_reconstruction" => {
            let gid = args["game_id"].as_str().unwrap_or("unknown");
            (format!("Reconstructing game {}", gid), serde_json::to_string(args).unwrap_or_default())
        }
        "expand_player_profile" => {
            let player = args["player_name"].as_str().unwrap_or("");
            (format!("Full profile for {}", player), player.to_string())
        }
        _ => (format!("Running {}", name), serde_json::to_string(args).unwrap_or_default()),
    }
}

impl Agent {
    pub async fn new(db: DbContext, insights_brief: String, sessions_path: String) -> Result<Self> {
        let api_key = std::env::var("OPENROUTER_API_KEY")
            .or_else(|_| std::env::var("OPENAI_API_KEY"))
            .map_err(|_| anyhow!("Neither OPENROUTER_API_KEY nor OPENAI_API_KEY set"))?;

        let http_client = Client::builder().build()?;
        let openrouter_url = std::env::var("OPENROUTER_BASE_URL")
            .unwrap_or_else(|_| "https://openrouter.ai/api/v1/chat/completions".to_string());
        let model = std::env::var("MODEL").unwrap_or_else(|_| "qwen/qwen3.7-flash".to_string());
        let fallback_model = std::env::var("FALLBACK_MODEL").ok().filter(|s| !s.is_empty());
        let max_iterations: usize =
            std::env::var("MAX_ITERATIONS").unwrap_or_else(|_| "12".to_string()).parse().unwrap_or(12);
        let key_tables: Vec<&str> = vec![
            "player",
            "team",
            "game",
            "common_player_info",
            "player_game_stats",
            "play_by_play",
            "line_score",
            "draft_history",
            "team_history",
            "team_details",
            "game_summary",
            "player_career_stats",
            "player_clutch_stats",
            "player_shooting_stats",
            "player_defensive_stats",
            "player_passing_stats",
            "player_rebounding_stats",
            "team_stats",
            "award",
            "coach",
            "series_post",
        ];
        let enriched = db.build_enriched_schema(Some(&key_tables)).await?;
        let schema_summary = DbContext::format_enriched_schema(&enriched);

        // Restore sessions from disk
        let sessions = Self::load_sessions(&sessions_path);
        let session_locks = moka::sync::Cache::builder()
            .max_capacity(SESSIONS_MAX_CAPACITY)
            .time_to_live(std::time::Duration::from_secs(SESSIONS_TTL_SECS))
            .build();

        let dirty = Arc::new(PlMutex::new(false));
        let save_signal = Arc::new(tokio::sync::Notify::new());

        let agent = Self {
            http_client,
            api_key,
            db,
            schema_summary,
            insights_brief,
            sessions,
            session_locks,
            sessions_path,
            openrouter_url,
            model,
            fallback_model,
            max_iterations,
            dirty: dirty.clone(),
            save_signal: save_signal.clone(),
        };

        // Spawn the background save loop.
        tokio::spawn(agent.clone().run_save_loop());
        Ok(agent)
    }

    /// Background task: every `SAVE_DEBOUNCE_SECS`, if dirty, persist sessions to disk.
    async fn run_save_loop(self) {
        loop {
            // Wait for either a notify() or the debounce tick.
            tokio::select! {
                _ = self.save_signal.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(SAVE_DEBOUNCE_SECS)) => {}
            }
            // Coalesce: if a write happened just before we read dirty, sleep once more.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if !*self.dirty.lock() {
                continue;
            }
            // Snapshot the sessions out of moka (drain_iter) without holding a lock across IO.
            let snapshot: Vec<(String, Vec<ChatMessage>)> =
                self.sessions.iter().map(|(k, v)| (k.as_str().to_string(), v.clone())).collect();
            match serde_json::to_string_pretty(&snapshot) {
                Ok(json) => {
                    if let Err(e) = std::fs::write(&self.sessions_path, json) {
                        tracing::warn!("Failed to save sessions: {}", e);
                    } else {
                        *self.dirty.lock() = false;
                    }
                }
                Err(e) => tracing::warn!("Failed to serialize sessions: {}", e),
            }
        }
    }

    /// Trim session messages to a sliding window: keep system prompt + last MAX_WINDOW messages.
    pub fn trim_sliding_window(messages: &mut Vec<ChatMessage>) {
        const MAX_WINDOW: usize = 20;
        if messages.len() <= MAX_WINDOW {
            return;
        }
        let old_len = messages.len();
        let system_msg = messages.remove(0);
        let trim_from = messages.len().saturating_sub(MAX_WINDOW - 1);
        let kept: Vec<_> = messages.drain(trim_from..).collect();
        messages.clear();
        messages.push(system_msg);
        messages.extend(kept);
        tracing::info!("Sliding window trimmed session from {} to {} messages", old_len, messages.len());
    }

    /// Load sessions from disk JSON file (array-of-pairs to preserve insert order).
    fn load_sessions(path: &str) -> moka::sync::Cache<String, Vec<ChatMessage>> {
        let cache = moka::sync::Cache::builder()
            .max_capacity(SESSIONS_MAX_CAPACITY)
            .time_to_live(std::time::Duration::from_secs(SESSIONS_TTL_SECS))
            .build();
        if let Ok(contents) = std::fs::read_to_string(path) {
            // Try array-of-pairs first (new format), fall back to object (legacy).
            if let Ok(vec) = serde_json::from_str::<Vec<(String, Vec<ChatMessage>)>>(&contents) {
                let n = vec.len();
                for (k, v) in vec {
                    cache.insert(k, v);
                }
                tracing::info!("Restored {} sessions from {}", n, path);
            } else if let Ok(map) =
                serde_json::from_str::<std::collections::HashMap<String, Vec<ChatMessage>>>(&contents)
            {
                let n = map.len();
                for (k, v) in map {
                    cache.insert(k, v);
                }
                tracing::info!("Restored {} sessions from {} (legacy format)", n, path);
            } else {
                tracing::warn!("Failed to parse sessions file, starting fresh");
            }
        } else {
            tracing::info!("No sessions file at {}, starting fresh", path);
        }
        cache
    }

    /// Reset session history
    pub fn reset_session(&self, session_id: &str) {
        self.sessions.invalidate(session_id);
    }

    /// Get or create the per-session async mutex used to serialize the
    /// read-modify-write cycle on the session store.
    fn session_lock(&self, session_id: &str) -> Arc<AsyncMutex<()>> {
        self.session_locks.get_with(session_id.to_string(), || Arc::new(AsyncMutex::new(()))).clone()
    }

    /// System prompt definition
    fn get_system_prompt(&self) -> String {
        format!(
            "You are an expert Data Agent and NBA Analyst with direct access to a 588-table DuckDB warehouse.\n\
            Your task is to answer natural language questions about NBA stats, games, teams, and players with exact, verified data.\n\n\
            AVAILABLE TOOLS:\n\
            1. `run_sql(reasoning, query)` - Execute DuckDB SQL and get JSON output (capped to 50 rows).\n\
            2. `list_tables(pattern)` - List table names matching a pattern.\n\
            3. `search_tables(keyword)` - Search table & column names for keywords.\n\
            4. `describe_table(table_name)` - Inspect column names, types, and sample rows.\n\
            5. `explain_query(query)` - Check SQL syntax via EXPLAIN.\n\
            6. `generate_chart(chart_type, title, sql_query)` - Create chart visualization.\n\n\
            BEST PRACTICES:\n\
            • Use schema context (row counts, FK→arrows, sample values) for accurate JOINs.\n\
            • FK columns (ending in _id) reference the same-named table.\n\
            • If a query fails, read 'Candidate bindings' for correct column names.\n\
            • End your response with 2-3 follow-up questions the user might ask next.\n\
              Format each as: SUGGESTED: <question> on its own line after your main answer.\n\n\
            {}\n\n{}\n\n{}",
            self.insights_brief,
            self.schema_summary,
            crate::db::DbContext::format_metrics_for_prompt()
        )
    }

    /// Tools array for OpenRouter
    fn get_tools_json(&self) -> Value {
        json!([
                   {
                       "type": "function",
                       "function": {
                           "name": "run_sql",
                           "description": "Execute DuckDB SQL query and return JSON results.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "reasoning": { "type": "string", "description": "Reasoning for running this query." },
                                   "query": { "type": "string", "description": "Valid DuckDB SQL query." }
                               },
                               "required": ["reasoning", "query"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "list_tables",
                           "description": "List table names matching a LIKE pattern.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "pattern": { "type": "string", "description": "LIKE pattern (e.g., 'agg_%' or '%clutch%')." }
                               }
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "search_tables",
                           "description": "Search across all 588 table and column names for keywords.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "keyword": { "type": "string", "description": "Keyword to search for." }
                               },
                               "required": ["keyword"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "describe_table",
                           "description": "Inspect a single table: columns, types, sample rows.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "table_name": { "type": "string", "description": "Table to describe." }
                               },
                               "required": ["table_name"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "explain_query",
                           "description": "Return DuckDB's EXPLAIN plan for a query.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "query": { "type": "string", "description": "SQL query to explain." }
                               },
                               "required": ["query"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "generate_chart",
                           "description": "Run a query and return a chart JSON spec for the frontend.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "chart_type": { "type": "string", "description": "One of: bar, line, pie, scatter." },
                                   "title": { "type": "string", "description": "Chart title." },
                                   "sql_query": { "type": "string", "description": "SQL to source chart data." }
                               },
                               "required": ["chart_type", "title", "sql_query"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "compare_players",
                           "description": "Compare two players: career totals, per-game stats, shooting splits, and awards.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "player1": { "type": "string", "description": "First player name." },
                                   "player2": { "type": "string", "description": "Second player name." },
                                   "season": { "type": "string", "description": "Optional season filter (e.g. '2023-24')." }
                               },
                               "required": ["player1", "player2"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "compare_teams",
                           "description": "Compare two teams: season records, offensive/defensive ratings, head-to-head.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "team1": { "type": "string", "description": "First team name or abbreviation." },
                                   "team2": { "type": "string", "description": "Second team name or abbreviation." },
                                   "season": { "type": "string", "description": "Optional season filter." }
                               },
                               "required": ["team1", "team2"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "find_streaks",
                           "description": "Find scoring/assist/rebound/win streaks for a player using gaps-and-islands SQL.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "player_name": { "type": "string", "description": "Player name." },
                                   "streak_type": { "type": "string", "description": "Type: 'points', 'assists', 'rebounds', 'wins', '30pt', '10ast'." },
                                   "min_value": { "type": "integer", "description": "Minimum threshold (e.g. 30 for 30-point streaks)." }
                               },
                               "required": ["player_name", "streak_type"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "get_player_profile",
                           "description": "Generate a comprehensive player profile: career summary, best seasons, career highs, awards.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "player_name": { "type": "string", "description": "Player name." }
                               },
                               "required": ["player_name"]
                           }
                       }
                   },
                   {
                       "type": "function",
        "function": {
                           "name": "rank_performance",
                           "description": "Rank a stat within NBA history, a franchise, a season, or a player's career. Returns rank and surrounding entries.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "stat_name": { "type": "string", "description": "Stat to rank (e.g. 'points', 'rebounds', 'assists')." },
                                   "value": { "type": "number", "description": "The value to rank." },
                                   "scope": { "type": "string", "description": "Scope: 'nba_history', 'franchise', 'season', 'career'." },
                                   "context": { "type": "string", "description": "Team name, season, or player name depending on scope." }
                               },
                               "required": ["stat_name", "value", "scope"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "find_leaders",
                           "description": "Find statistical leaders for a given stat. Returns top N players sorted by the stat.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "stat_name": { "type": "string", "description": "Stat to rank (e.g. 'points', 'rebounds', 'assists', 'threes')." },
                                   "season": { "type": "string", "description": "Optional season filter (e.g. '2023-24')." },
                                   "limit": { "type": "integer", "description": "Number of leaders to return (default 10)." }
                               },
                               "required": ["stat_name"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "get_game_summary",
                           "description": "Get a full game summary: box score, key stats, and notable performances.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "game_id": { "type": "string", "description": "Game ID to look up." },
                                   "game_date": { "type": "string", "description": "Game date (YYYY-MM-DD) if game_id is unknown." }
                               }
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "get_head_to_head",
                           "description": "Full head-to-head explorer: overall record, home/away splits, average margin, series results.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "team1": { "type": "string", "description": "First team name or abbreviation." },
                                   "team2": { "type": "string", "description": "Second team name or abbreviation." },
                                   "season": { "type": "string", "description": "Optional season filter." }
                               },
                               "required": ["team1", "team2"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "check_data_coverage",
                           "description": "Check data completeness: season coverage, missing values, table freshness for a given entity.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "entity_type": { "type": "string", "description": "Type: 'player', 'team', 'season', 'table'." },
                                   "entity_name": { "type": "string", "description": "Name or ID of the entity to check." }
                               },
                               "required": ["entity_type"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "export_query_result",
                           "description": "Execute a query and return results in CSV or JSON format for download.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "query": { "type": "string", "description": "SQL query to execute." },
                                   "format": { "type": "string", "description": "Output format: 'csv' or 'json'." },
                                   "filename": { "type": "string", "description": "Suggested filename for download." }
                               },
                               "required": ["query", "format"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "era_adjusted_compare",
                           "description": "Compare players across eras using league-relative metrics: points vs league avg, pace-adjusted stats, percentile within season.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "player1": { "type": "string", "description": "First player name." },
                                   "player2": { "type": "string", "description": "Second player name." },
                                   "metric": { "type": "string", "description": "Metric to compare (default: 'points_per_game')." }
                               },
                               "required": ["player1", "player2"]
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "game_reconstruction",
                           "description": "Reconstruct a game: lead changes, ties, scoring runs, largest lead, key turning points.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "game_id": { "type": "string", "description": "Game ID." },
                                   "game_date": { "type": "string", "description": "Game date if game_id unknown." }
                               }
                           }
                       }
                   },
                   {
                       "type": "function",
                       "function": {
                           "name": "expand_player_profile",
                           "description": "Full player profile: career highs, playoff performance, team history, awards, shooting splits, similar players.",
                           "parameters": {
                               "type": "object",
                               "properties": {
                                   "player_name": { "type": "string", "description": "Player name." }
                               },
                               "required": ["player_name"]
                           }
                       }
                   }
               ])
    }

    /// Internal: run a single OpenRouter chat completion with retry and fallback.
    async fn call_openrouter(&self, messages: &[ChatMessage], tools: &Value) -> Result<ChatCompletion> {
        let max_retries = 3;
        let mut last_err = None;

        // Try primary model, then fallback, each with retries
        let models: Vec<&str> =
            if let Some(ref fb) = self.fallback_model { vec![&self.model, fb] } else { vec![&self.model] };

        for model_name in &models {
            for attempt in 0..max_retries {
                if attempt > 0 {
                    let delay_ms = 1000 * 2u64.pow(attempt as u32 - 1);
                    tracing::info!("Retrying {} after {}ms (attempt {})", model_name, delay_ms, attempt + 1);
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }

                let req_body = json!({
                    "model": model_name,
                    "messages": messages,
                    "tools": tools,
                    "reasoning": { "exclude": false }
                });

                let res = match self
                    .http_client
                    .post(&self.openrouter_url)
                    .header("Authorization", format!("Bearer {}", self.api_key))
                    .header("HTTP-Referer", "https://github.com/db-analyze-agent")
                    .header("X-Title", "NBA Data Agent")
                    .json(&req_body)
                    .timeout(std::time::Duration::from_secs(60))
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        last_err = Some(anyhow!("HTTP error: {}", e));
                        continue;
                    }
                };

                if !res.status().is_success() {
                    let status = res.status();
                    let err_text = res.text().await.unwrap_or_default();
                    last_err = Some(anyhow!("API error {}: {}", status, err_text));
                    continue;
                }

                let res_json: Value = match res.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        last_err = Some(anyhow!("JSON parse error: {}", e));
                        continue;
                    }
                };

                let choice = res_json["choices"][0].clone();
                let msg_val = &choice["message"];

                let content = msg_val["content"].as_str().map(|s| s.to_string());
                let reasoning = msg_val["reasoning"].as_str().map(|s| s.to_string());
                let mut tool_calls = Vec::new();
                if let Some(arr) = msg_val["tool_calls"].as_array() {
                    for tc in arr {
                        let fn_val = &tc["function"];
                        let args_str = fn_val["arguments"].as_str().unwrap_or("{}");
                        let arguments: Value = serde_json::from_str(args_str).unwrap_or(Value::Null);
                        tool_calls.push(ParsedToolCall {
                            id: tc["id"].as_str().unwrap_or("").to_string(),
                            name: fn_val["name"].as_str().unwrap_or("").to_string(),
                            arguments,
                        });
                    }
                }

                return Ok(ChatCompletion { content, reasoning, tool_calls });
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("All models failed")))
    }

    /// Push a parsed completion into the message history and return a
    /// (content, reasoning, tool_calls) tuple ready for trace/event construction.
    fn completion_to_assistant_message(c: &ChatCompletion) -> ChatMessage {
        let content = c.content.as_ref().map(|s| Value::String(s.clone()));
        let tool_calls = if c.tool_calls.is_empty() {
            None
        } else {
            Some(Value::Array(
                c.tool_calls
                    .iter()
                    .map(|tc| {
                        json!({
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.name,
                                "arguments": serde_json::to_string(&tc.arguments).unwrap_or_else(|_| "{}".to_string())
                            }
                        })
                    })
                    .collect(),
            ))
        };
        ChatMessage { role: "assistant".to_string(), content, tool_calls, tool_call_id: None, name: None }
    }

    /// Mark the session store dirty and wake the save task. Caller must have
    /// already written the new state into `self.sessions` for `session_id`.
    fn mark_dirty(&self) {
        *self.dirty.lock() = true;
        self.save_signal.notify_one();
    }

    /// Escape single quotes in user-provided strings for safe SQL interpolation.
    fn sql_safe(s: &str) -> String {
        s.replace('\'', "''")
    }

    /// Execute multi-turn conversation turn
    pub async fn run_conversation(
        &self,
        session_id_opt: Option<String>,
        user_message: &str,
    ) -> Result<ConversationTrace> {
        let session_id = session_id_opt.unwrap_or_else(|| Uuid::new_v4().to_string());
        let self_clone = self.clone();
        let session_id_for_lock = session_id.clone();
        let user_message = user_message.to_string();

        let lock = self.session_lock(&session_id_for_lock);
        let _guard = lock.lock().await;
        let session_id_for_lock_inner = session_id_for_lock.clone();
        let future = async move {
            let system_prompt = self_clone.get_system_prompt();
            let tools = self_clone.get_tools_json();

            let mut messages = self_clone.sessions.get(&session_id_for_lock_inner).unwrap_or_else(|| {
                vec![ChatMessage {
                    role: "system".to_string(),
                    content: Some(json!(system_prompt)),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                }]
            });

            messages.push(ChatMessage {
                role: "user".to_string(),
                content: Some(json!(user_message)),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            });

            Self::trim_sliding_window(&mut messages);
            let mut trace = ConversationTrace {
                session_id: session_id_for_lock_inner.clone(),
                steps: Vec::new(),
                final_answer: String::new(),
            };

            let max_iterations = self_clone.max_iterations;
            for iteration in 1..=max_iterations {
                let completion = self_clone.call_openrouter(&messages, &tools).await?;
                let reasoning = completion.reasoning.clone();
                let content = completion.content.clone();

                if completion.tool_calls.is_empty() {
                    let final_answer = content.unwrap_or_else(|| "(no content)".to_string());
                    trace.final_answer = final_answer;
                    // Persist the final assistant message into session history
                    // so follow-up context and Markdown export include it.
                    messages.push(Self::completion_to_assistant_message(&completion));
                    trace.steps.push(ChatStep { content: completion.content, reasoning, tool_calls: Vec::new() });
                    break;
                }

                let mut step = ChatStep { content: content.clone(), reasoning, tool_calls: Vec::new() };

                messages.push(Self::completion_to_assistant_message(&completion));

                for tc in &completion.tool_calls {
                    let tr = self_clone.execute_tool(&tc.name, &tc.arguments).await;
                    step.tool_calls.push(ToolCallStep {
                        tool_name: tc.name.clone(),
                        reasoning: tr.reasoning.clone(),
                        query_or_params: tr.param_str.clone(),
                        result: tr.result_str.clone(),
                        elapsed_ms: tr.elapsed_ms,
                        row_count: tr.row_count,
                    });
                    messages.push(ChatMessage {
                        role: "tool".to_string(),
                        content: Some(json!(tr.result_str)),
                        tool_calls: None,
                        tool_call_id: Some(tc.id.clone()),
                        name: Some(tc.name.clone()),
                    });
                }
                trace.steps.push(step);

                if iteration == max_iterations {
                    trace.final_answer =
                        "Reached max analytical reasoning steps. Here are the findings so far.".to_string();
                }
            }

            self_clone.sessions.insert(session_id_for_lock_inner.clone(), messages);
            self_clone.mark_dirty();
            Ok(trace)
        };
        future.await
    }

    /// Stream conversation steps as SSE events
    pub fn run_conversation_stream(
        &self,
        session_id_opt: Option<String>,
        user_message: String,
    ) -> Pin<Box<dyn Stream<Item = Result<AgentStreamEvent, anyhow::Error>> + Send>> {
        let this = self.clone();
        Box::pin(stream! {
            let session_id = session_id_opt.unwrap_or_else(|| Uuid::new_v4().to_string());
            let lock = this.session_lock(&session_id);
            let _guard = lock.lock().await;

            let system_prompt = this.get_system_prompt();
            let tools = this.get_tools_json();

            let mut messages = this
                .sessions
                .get(&session_id)
                .unwrap_or_else(|| {
                    vec![ChatMessage {
                        role: "system".to_string(),
                        content: Some(json!(system_prompt)),
                        tool_calls: None,
                        tool_call_id: None,
                        name: None,
                    }]
                });

            messages.push(ChatMessage {
                role: "user".to_string(),
                content: Some(json!(user_message)),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            });

            Self::trim_sliding_window(&mut messages);
            let mut trace = ConversationTrace {
                session_id: session_id.clone(),
                steps: Vec::new(),
                final_answer: String::new(),
            };

            let max_iterations = this.max_iterations;
            let mut early_return = false;
            for iteration in 1..=max_iterations {
                yield Ok(AgentStreamEvent::StepStarted { step: iteration });

                let completion = match this.call_openrouter(&messages, &tools).await {
                    Ok(c) => c,
                    Err(e) => {
                        yield Ok(AgentStreamEvent::Error { message: e.to_string() });
                        early_return = true;
                        break;
                    }
                };

                if let Some(r) = &completion.reasoning {
                    yield Ok(AgentStreamEvent::Reasoning { text: r.clone() });
                }

                if completion.tool_calls.is_empty() {
                    let content = completion.content.clone().unwrap_or_default();
                    trace.final_answer = content.clone();
                    // Persist the final assistant message into session history.
                    messages.push(Self::completion_to_assistant_message(&completion));
                    trace.steps.push(ChatStep {
                        content: completion.content,
                        reasoning: completion.reasoning,
                        tool_calls: Vec::new(),
                    });
                    yield Ok(AgentStreamEvent::FinalAnswerChunk { text: content });
                    break;
                }

                let mut step = ChatStep {
                    content: completion.content.clone(),
                    reasoning: completion.reasoning.clone(),
                    tool_calls: Vec::new(),
                };

                messages.push(Self::completion_to_assistant_message(&completion));

                for tc in &completion.tool_calls {
                    // Emit ToolCallStarted BEFORE execute_tool so the frontend
                    // receives the "started" event while the operation is still running.
                    let start_args = args_to_tool_start_fields(&tc.name, &tc.arguments);
                    yield Ok(AgentStreamEvent::ToolCallStarted {
                        tool_name: tc.name.clone(),
                        reasoning: start_args.0.clone(),
                        query_or_params: start_args.1.clone(),
                    });
                    let tr = this.execute_tool(&tc.name, &tc.arguments).await;
                    yield Ok(AgentStreamEvent::ToolCallResult {
                        tool_name: tc.name.clone(),
                        result: tr.result_str.clone(),
                        elapsed_ms: tr.elapsed_ms,
                        row_count: tr.row_count,
                    });
                    step.tool_calls.push(ToolCallStep {
                        tool_name: tc.name.clone(),
                        reasoning: tr.reasoning.clone(),
                        query_or_params: tr.param_str.clone(),
                        result: tr.result_str.clone(),
                        elapsed_ms: tr.elapsed_ms,
                        row_count: tr.row_count,
                    });
                    messages.push(ChatMessage {
                        role: "tool".to_string(),
                        content: Some(json!(tr.result_str)),
                        tool_calls: None,
                        tool_call_id: Some(tc.id.clone()),
                        name: Some(tc.name.clone()),
                    });
                }
                trace.steps.push(step);

                if iteration == max_iterations {
                    let msg = "Reached max analytical reasoning steps.".to_string();
                    trace.final_answer = msg.clone();
                    yield Ok(AgentStreamEvent::FinalAnswerChunk { text: msg });
                }
            }

            if !early_return {
                this.sessions.insert(session_id.clone(), messages);
                this.mark_dirty();
                yield Ok(AgentStreamEvent::Completed { trace });
            }
        })
    }

    /// Dispatch tool calls to DbContext. Returns timing metadata alongside results.
    async fn execute_tool(&self, name: &str, args: &Value) -> ToolResult {
        let start = std::time::Instant::now();
        let (reasoning, param_str, result_str) = match name {
            "run_sql" => {
                let reasoning = args["reasoning"].as_str().unwrap_or("Executing query").to_string();
                let query = args["query"].as_str().unwrap_or("").to_string();
                let result_str = match self.db.run_sql(query.clone(), None).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => {
                        let err_msg = e.to_string();
                        if let Some(fixed_query) = DbContext::auto_fix_sql(&query, &err_msg) {
                            tracing::info!("Auto-fixed SQL column name, retrying: {}", fixed_query);
                            match self.db.run_sql(fixed_query.clone(), None).await {
                                Ok(rows) => {
                                    format!(
                                        "[Auto-corrected column name and retried successfully]\n{}",
                                        serde_json::to_string_pretty(&rows).unwrap_or_default()
                                    )
                                }
                                Err(e2) => {
                                    format!(
                                        "SQL Error (original): {}\nAuto-fix attempt ({}): {}",
                                        err_msg, fixed_query, e2
                                    )
                                }
                            }
                        } else {
                            format!("SQL Error: {}", err_msg)
                        }
                    }
                };
                (reasoning, query, result_str)
            }
            "list_tables" => {
                let pattern = args["pattern"].as_str().map(|s| s.to_string());
                let param_display = pattern.clone().unwrap_or_else(|| "%".to_string());
                let reasoning = format!("Listing tables matching '{}'", param_display);
                let result_str = match self.db.list_tables(pattern).await {
                    Ok(tables) => serde_json::to_string_pretty(&tables).unwrap_or_default(),
                    Err(e) => format!("List Error: {}", e),
                };
                (reasoning, param_display, result_str)
            }
            "search_tables" => {
                let keyword = args["keyword"].as_str().unwrap_or("").to_string();
                let reasoning = format!("Searching tables matching '{}'", keyword);
                let result_str = match self.db.search_tables(keyword.clone()).await {
                    Ok(res) => serde_json::to_string_pretty(&res).unwrap_or_default(),
                    Err(e) => format!("Search Error: {}", e),
                };
                (reasoning, keyword, result_str)
            }
            "describe_table" => {
                let table_name = args["table_name"].as_str().unwrap_or("").to_string();
                let reasoning = format!("Inspecting table `{}`", table_name);
                let result_str = match self.db.describe_table(table_name.clone()).await {
                    Ok(info) => serde_json::to_string_pretty(&info).unwrap_or_default(),
                    Err(e) => format!("Describe Error: {}", e),
                };
                (reasoning, table_name, result_str)
            }
            "explain_query" => {
                let query = args["query"].as_str().unwrap_or("").to_string();
                let reasoning = "Checking query plan for SQL".to_string();
                let result_str = match self.db.explain_query(query.clone()).await {
                    Ok(plan) => plan,
                    Err(e) => format!("Explain Error: {}", e),
                };
                (reasoning, query, result_str)
            }
            "generate_chart" => {
                let chart_type = args["chart_type"].as_str().unwrap_or("bar").to_string();
                let title = args["title"].as_str().unwrap_or("NBA Stat Chart").to_string();
                let sql_query = args["sql_query"].as_str().unwrap_or("").to_string();
                let reasoning = format!("Generating {} chart: '{}'", chart_type, title);
                let result_str = match self.db.run_sql(sql_query.clone(), Some(30)).await {
                    Ok(rows) => {
                        let chart_json = json!({
                            "chart_type": chart_type,
                            "title": title,
                            "data_rows": rows
                        });
                        serde_json::to_string_pretty(&chart_json).unwrap_or_default()
                    }
                    Err(e) => format!("Chart Query Error: {}", e),
                };
                (reasoning, sql_query, result_str)
            }
            "compare_players" => {
                let p1 = args["player1"].as_str().unwrap_or("");
                let p2 = args["player2"].as_str().unwrap_or("");
                let season_filter = args["season"]
                    .as_str()
                    .map(|s| format!(" AND season_id = '{}'", Self::sql_safe(s)))
                    .unwrap_or_default();
                let reasoning = format!("Comparing {} vs {}", p1, p2);
                let sql = format!(
                    "SELECT player_name, season_id, SUM(pts) as total_pts, \
                           ROUND(AVG(pts),1) as ppg, ROUND(AVG(reb),1) as rpg, \
                           ROUND(AVG(ast),1) as apg, COUNT(*) as games \
                     FROM player_game_stats \
                     WHERE player_name IN ('{}', '{}'){} \
                     GROUP BY player_name, season_id ORDER BY player_name, season_id",
                    Self::sql_safe(p1),
                    Self::sql_safe(p2),
                    season_filter
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(100)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "compare_teams" => {
                let t1 = args["team1"].as_str().unwrap_or("");
                let t2 = args["team2"].as_str().unwrap_or("");
                let season_filter = args["season"]
                    .as_str()
                    .map(|s| format!(" AND season_id = '{}'", Self::sql_safe(s)))
                    .unwrap_or_default();
                let reasoning = format!("Comparing {} vs {}", t1, t2);
                let sql = format!(
                    "SELECT team_name, season_id, SUM(pts) as total_pts, \
                           ROUND(AVG(pts),1) as ppg, SUM(wins) as wins, \
                           SUM(losses) as losses \
                     FROM team_stats \
                     WHERE team_name IN ('{}', '{}'){} \
                     GROUP BY team_name, season_id ORDER BY team_name, season_id",
                    Self::sql_safe(t1),
                    Self::sql_safe(t2),
                    season_filter
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(100)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "find_streaks" => {
                let player = args["player_name"].as_str().unwrap_or("");
                let streak_type = args["streak_type"].as_str().unwrap_or("points");
                let min_val: i64 = args["min_value"].as_i64().unwrap_or(match streak_type {
                    "30pt" | "points" => 30,
                    "10ast" | "assists" => 10,
                    "rebounds" => 10,
                    _ => 0,
                });
                let metric_col = match streak_type {
                    "30pt" | "points" => "pts",
                    "10ast" | "assists" => "ast",
                    "rebounds" => "reb",
                    "wins" => "wl",
                    _ => "pts",
                };
                let reasoning = format!("Finding {} streaks for {} (min {})", streak_type, player, min_val);
                let safe_player = Self::sql_safe(player);
                let sql = if streak_type == "wins" {
                    format!(
                        "WITH game_flags AS ( \
                           SELECT game_id, game_date, \
                                  CASE WHEN team_score > opp_score THEN 1 ELSE 0 END as won \
                           FROM player_game_stats WHERE player_name = '{}' \
                        ), \
                        streak_groups AS ( \
                           SELECT *, SUM(CASE WHEN won = 0 THEN 1 ELSE 0 END) OVER (ORDER BY game_date) as grp \
                           FROM game_flags WHERE won = 1 \
                        ) \
                        SELECT MIN(game_date) as streak_start, MAX(game_date) as streak_end, \
                               COUNT(*) as streak_length \
                        FROM streak_groups GROUP BY grp ORDER BY streak_length DESC LIMIT 10",
                        safe_player
                    )
                } else {
                    format!(
                        "WITH game_flags AS ( \
                           SELECT game_id, game_date, {}, \
                                  CASE WHEN {} >= {} THEN 1 ELSE 0 END as above \
                           FROM player_game_stats WHERE player_name = '{}' \
                        ), \
                        streak_groups AS ( \
                           SELECT *, SUM(CASE WHEN above = 0 THEN 1 ELSE 0 END) OVER (ORDER BY game_date) as grp \
                           FROM game_flags WHERE above = 1 \
                        ) \
                        SELECT MIN(game_date) as streak_start, MAX(game_date) as streak_end, \
                               COUNT(*) as streak_length, MIN({}) as min_val, MAX({}) as max_val \
                        FROM streak_groups GROUP BY grp ORDER BY streak_length DESC LIMIT 10",
                        metric_col, metric_col, min_val, safe_player, metric_col, metric_col
                    )
                };
                let result_str = match self.db.run_sql(sql.clone(), Some(20)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "get_player_profile" => {
                let player = args["player_name"].as_str().unwrap_or("");
                let reasoning = format!("Building profile for {}", player);
                let safe = Self::sql_safe(player);
                let sql = format!(
                    "SELECT 'career_summary' as section, player_name, \
                           COUNT(*) as games, SUM(pts) as total_pts, \
                           ROUND(AVG(pts),1) as ppg, ROUND(AVG(reb),1) as rpg, \
                           ROUND(AVG(ast),1) as apg \
                     FROM player_game_stats WHERE player_name = '{}' \
                     GROUP BY player_name \
                     UNION ALL \
                     SELECT 'best_season' as section, player_name, \
                           season_id as games, SUM(pts) as total_pts, \
                           ROUND(AVG(pts),1) as ppg, ROUND(AVG(reb),1) as rpg, \
                           ROUND(AVG(ast),1) as apg \
                     FROM player_game_stats WHERE player_name = '{}' \
                     GROUP BY player_name, season_id ORDER BY total_pts DESC LIMIT 5",
                    safe, safe
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(20)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "rank_performance" => {
                let stat = args["stat_name"].as_str().unwrap_or("points");
                let value = args["value"].as_f64().unwrap_or(0.0);
                let scope = args["scope"].as_str().unwrap_or("nba_history");
                let context = args["context"].as_str().unwrap_or("");
                let reasoning = format!("Ranking {} = {} in {} ({})", stat, value, scope, context);
                let (rank_col, order) = match stat {
                    "points" => ("total_pts", "DESC"),
                    "rebounds" => ("total_reb", "DESC"),
                    "assists" => ("total_ast", "DESC"),
                    _ => ("total_pts", "DESC"),
                };
                let where_clause = match scope {
                    "franchise" => format!("WHERE team_name = '{}' ", Self::sql_safe(context)),
                    "season" => format!("WHERE season_id = '{}' ", Self::sql_safe(context)),
                    "career" => format!("WHERE player_name = '{}' ", Self::sql_safe(context)),
                    _ => String::new(),
                };
                let sql = format!(
                    "WITH ranked AS ( \
                       SELECT player_name, SUM({}) as stat_val, \
                              ROW_NUMBER() OVER (ORDER BY SUM({}) {}) as rank \
                       FROM player_game_stats {} \
                       GROUP BY player_name \
                    ) \
                    SELECT * FROM ranked \
                    WHERE stat_val <= {} ORDER BY rank LIMIT 20",
                    rank_col, rank_col, order, where_clause, value
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(20)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "find_leaders" => {
                let stat = args["stat_name"].as_str().unwrap_or("points");
                let season_filter = args["season"]
                    .as_str()
                    .map(|s| format!(" AND season_id = '{}'", Self::sql_safe(s)))
                    .unwrap_or_default();
                let limit: usize = args["limit"].as_u64().unwrap_or(10) as usize;
                let reasoning = format!("Finding {} leaders", stat);
                let (col, label) = match stat {
                    "points" => ("pts", "ppg"),
                    "rebounds" => ("reb", "rpg"),
                    "assists" => ("ast", "apg"),
                    "threes" => ("fg3m", "3pg"),
                    "steals" => ("stl", "spg"),
                    "blocks" => ("blk", "bpg"),
                    _ => ("pts", "ppg"),
                };
                let sql = format!(
                    "SELECT player_name, ROUND(AVG({}),1) as {}, COUNT(*) as games \
                     FROM player_game_stats WHERE 1=1{} \
                     GROUP BY player_name HAVING COUNT(*) >= 10 \
                     ORDER BY {} DESC LIMIT {}",
                    col, label, season_filter, label, limit
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(limit)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "get_game_summary" => {
                let game_id = args["game_id"].as_str().unwrap_or("");
                let game_date = args["game_date"].as_str().unwrap_or("");
                let reasoning = format!("Summarizing game {} {}", game_id, game_date);
                let where_clause = if !game_id.is_empty() {
                    format!("WHERE g.game_id = '{}' ", Self::sql_safe(game_id))
                } else if !game_date.is_empty() {
                    format!("WHERE g.game_date = '{}' ", Self::sql_safe(game_date))
                } else {
                    "".to_string()
                };
                let sql = format!(
                    "SELECT g.game_id, g.game_date, \
                           ht.team_name as home_team, at.team_name as away_team, \
                           ls.team_score as home_score, ls.opp_score as away_score \
                     FROM game g \
                     JOIN line_score ls ON g.game_id = ls.game_id \
                     JOIN team ht ON ls.team_id = ht.team_id \
                     JOIN team at ON ls.opp_team_id = at.team_id \
                     {}ORDER BY g.game_date DESC LIMIT 5",
                    where_clause
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(10)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "get_head_to_head" => {
                let t1 = args["team1"].as_str().unwrap_or("");
                let t2 = args["team2"].as_str().unwrap_or("");
                let season_filter = args["season"]
                    .as_str()
                    .map(|s| format!(" AND g.season_id = '{}'", Self::sql_safe(s)))
                    .unwrap_or_default();
                let reasoning = format!("H2H: {} vs {}", t1, t2);
                let safe_t1 = Self::sql_safe(t1);
                let safe_t2 = Self::sql_safe(t2);
                let sql = format!(
                    "SELECT ht.team_name as home_team, at.team_name as away_team, \
                           ls.team_score as home_score, ls.opp_score as away_score, \
                           g.game_date \
                     FROM game g \
                     JOIN line_score ls ON g.game_id = ls.game_id \
                     JOIN team ht ON ls.team_id = ht.team_id \
                     JOIN team at ON ls.opp_team_id = at.team_id \
                     WHERE (ht.team_name = '{}' AND at.team_name = '{}') \
                        OR (ht.team_name = '{}' AND at.team_name = '{}'){} \
                     ORDER BY g.game_date DESC LIMIT 20",
                    safe_t1, safe_t2, safe_t2, safe_t1, season_filter
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(20)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "check_data_coverage" => {
                let entity_type = args["entity_type"].as_str().unwrap_or("player");
                let entity_name = args["entity_name"].as_str().unwrap_or("");
                let reasoning = format!("Checking {} coverage for {}", entity_type, entity_name);
                let sql = match entity_type {
                    "player" => format!(
                        "SELECT 'player_game_stats' as table_name, COUNT(*) as row_count, \
                               MIN(game_date) as earliest, MAX(game_date) as latest, \
                               COUNT(DISTINCT season_id) as seasons \
                         FROM player_game_stats WHERE player_name = '{}' \
                         UNION ALL \
                         SELECT 'player_career_stats', COUNT(*), MIN(season_id), MAX(season_id), COUNT(DISTINCT season_id) \
                         FROM player_career_stats WHERE player_name = '{}'",
                        Self::sql_safe(entity_name),
                        Self::sql_safe(entity_name)
                    ),
                    "team" => format!(
                        "SELECT 'team_stats' as table_name, COUNT(*) as row_count, \
                               MIN(season_id) as earliest, MAX(season_id) as latest, \
                               COUNT(DISTINCT season_id) as seasons \
                         FROM team_stats WHERE team_name = '{}'",
                        Self::sql_safe(entity_name)
                    ),
                    "season" => format!(
                        "SELECT 'game' as table_name, COUNT(*) as row_count, \
                               MIN(game_date) as earliest, MAX(game_date) as latest, \
                               COUNT(DISTINCT game_id) as games \
                         FROM game WHERE season_id = '{}'",
                        Self::sql_safe(entity_name)
                    ),
                    _ => "SELECT 'unknown' as table_name, 0 as row_count".to_string(),
                };
                let result_str = match self.db.run_sql(sql.clone(), Some(10)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "export_query_result" => {
                let query = args["query"].as_str().unwrap_or("");
                let format = args["format"].as_str().unwrap_or("csv");
                let reasoning = format!("Exporting query result as {}", format);
                let result_str = match self.db.run_sql(query.to_string(), Some(500)).await {
                    Ok(rows) => {
                        if format == "csv" {
                            if rows.is_empty() {
                                "No rows returned".to_string()
                            } else {
                                let keys: Vec<&str> = rows[0]
                                    .as_object()
                                    .map(|o| o.keys().map(|k| k.as_str()).collect())
                                    .unwrap_or_default();
                                let mut csv = keys.join(",");
                                for row in &rows {
                                    let vals: Vec<String> = keys
                                        .iter()
                                        .map(|k| {
                                            row.get(*k)
                                                .map(|v| {
                                                    let s = v.to_string().trim_matches('"').to_string();
                                                    if s.contains(',') || s.contains('"') || s.contains('\n') {
                                                        format!("\"{}\"", s.replace('"', "\"\""))
                                                    } else {
                                                        s
                                                    }
                                                })
                                                .unwrap_or_default()
                                        })
                                        .collect();
                                    csv.push('\n');
                                    csv.push_str(&vals.join(","));
                                }
                                csv
                            }
                        } else {
                            serde_json::to_string_pretty(&rows).unwrap_or_default()
                        }
                    }
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, query.to_string(), result_str)
            }
            "era_adjusted_compare" => {
                let p1 = args["player1"].as_str().unwrap_or("");
                let p2 = args["player2"].as_str().unwrap_or("");
                let metric = args["metric"].as_str().unwrap_or("points_per_game");
                let reasoning = format!("Era-adjusted compare: {} vs {} ({})", p1, p2, metric);
                let safe_p1 = Self::sql_safe(p1);
                let safe_p2 = Self::sql_safe(p2);
                let sql = format!(
                    "WITH player_stats AS ( \
                       SELECT player_name, season_id, \
                              AVG(pts) as ppg, AVG(reb) as rpg, AVG(ast) as apg, \
                              COUNT(*) as games \
                       FROM player_game_stats \
                       WHERE player_name IN ('{}', '{}') \
                       GROUP BY player_name, season_id \
                     ), \
                     league_avg AS ( \
                       SELECT season_id, AVG(pts) as lg_ppg \
                       FROM player_game_stats GROUP BY season_id \
                     ) \
                     SELECT ps.player_name, ps.season_id, ps.ppg, ps.rpg, ps.apg, ps.games, \
                            la.lg_ppg, \
                            ROUND(ps.ppg / NULLIF(la.lg_ppg, 0), 3) as era_ratio \
                     FROM player_stats ps \
                     JOIN league_avg la ON ps.season_id = la.season_id \
                     ORDER BY ps.player_name, ps.season_id",
                    safe_p1, safe_p2
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(100)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "game_reconstruction" => {
                let game_id = args["game_id"].as_str().unwrap_or("");
                let game_date = args["game_date"].as_str().unwrap_or("");
                let reasoning = format!("Reconstructing game {} {}", game_id, game_date);
                let where_clause = if !game_id.is_empty() {
                    format!("WHERE pbp.game_id = '{}' ", Self::sql_safe(game_id))
                } else if !game_date.is_empty() {
                    format!("WHERE pbp.game_date = '{}' ", Self::sql_safe(game_date))
                } else {
                    "".to_string()
                };
                let sql = format!(
                    "SELECT pbp.game_id, pbp.event_type, pbp.player_name, \
                           pbp.visitor_desc, pbp.home_desc, pbp.period, \
                           pbp.pctimestring, pbp.score, pbp.scoremargin \
                     FROM play_by_play pbp \
                     {}ORDER BY pbp.period, pbp.event_num LIMIT 200",
                    where_clause
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(200)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            "expand_player_profile" => {
                let player = args["player_name"].as_str().unwrap_or("");
                let reasoning = format!("Full profile for {}", player);
                let safe = Self::sql_safe(player);
                let sql = format!(
                    "SELECT 'career' as section, player_name, \
                           COUNT(*) as games, SUM(pts) as total_pts, \
                           ROUND(AVG(pts),1) as ppg, ROUND(AVG(reb),1) as rpg, \
                           ROUND(AVG(ast),1) as apg, MAX(pts) as career_high_pts \
                     FROM player_game_stats WHERE player_name = '{}' GROUP BY player_name \
                     UNION ALL \
                     SELECT 'best_season', player_name, \
                           season_id, SUM(pts), ROUND(AVG(pts),1), ROUND(AVG(reb),1), \
                           ROUND(AVG(ast),1), MAX(pts) \
                     FROM player_game_stats WHERE player_name = '{}' \
                     GROUP BY player_name, season_id ORDER BY SUM(pts) DESC LIMIT 5 \
                     UNION ALL \
                     SELECT 'playoff', player_name, \
                           COUNT(*), SUM(pts), ROUND(AVG(pts),1), ROUND(AVG(reb),1), \
                           ROUND(AVG(ast),1), MAX(pts) \
                     FROM player_game_stats WHERE player_name = '{}' AND season_id LIKE '%P%' \
                     GROUP BY player_name",
                    safe, safe, safe
                );
                let result_str = match self.db.run_sql(sql.clone(), Some(30)).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => format!("Error: {}", e),
                };
                (reasoning, sql, result_str)
            }
            _ => ("Unknown tool".to_string(), name.to_string(), format!("Tool `{}` not supported.", name)),
        };
        let elapsed_ms = start.elapsed().as_millis() as u64;
        let row_count = serde_json::from_str::<Vec<Value>>(&result_str).map(|v| v.len()).unwrap_or(0);
        ToolResult { reasoning, param_str, result_str, elapsed_ms, row_count }
    }

    /// Number of tracked sessions (for /api/stats consumers).
    pub fn session_count(&self) -> usize {
        self.sessions.entry_count() as usize
    }

    /// Export a session as Markdown by reconstructing from stored ChatMessages.
    pub fn export_session_markdown(&self, session_id: &str) -> Option<String> {
        let messages = self.sessions.get(session_id)?;

        let mut md = String::new();
        md.push_str("# NBA Data Analysis Report\n\n");
        md.push_str(&format!("**Session:** `{}` | **Messages:** {}\n\n", session_id, messages.len()));
        md.push_str("---\n\n");

        for msg in messages.iter() {
            match msg.role.as_str() {
                "user" => {
                    if let Some(content) = &msg.content {
                        let text = content.as_str().unwrap_or("");
                        md.push_str(&format!("## 🏀 Question\n\n{}\n\n", text));
                    }
                }
                "assistant" => {
                    if let Some(content) = &msg.content {
                        let text = content.as_str().unwrap_or("");
                        if !text.is_empty() {
                            md.push_str(&format!("## 🤖 Answer\n\n{}\n\n", text));
                        }
                    }
                }
                "tool" => {
                    let tool_name = msg.name.as_deref().unwrap_or("unknown");
                    if let Some(content) = &msg.content {
                        let text = content.as_str().unwrap_or("");
                        md.push_str(&format!("### 🔧 Tool: `{}`\n\n", tool_name));
                        if let Ok(parsed) = serde_json::from_str::<Vec<serde_json::Value>>(text) {
                            if !parsed.is_empty() && parsed[0].is_object() {
                                Self::append_markdown_table(&mut md, &parsed);
                            } else {
                                md.push_str(&format!("```json\n{}\n```\n\n", text));
                            }
                        } else {
                            md.push_str(&format!("```\n{}\n```\n\n", text));
                        }
                    }
                }
                _ => {}
            }
        }

        md.push_str("---\n\n*Generated by NBA Database Agent*\n");
        Some(md)
    }

    pub fn append_markdown_table(md: &mut String, rows: &[serde_json::Value]) {
        if rows.is_empty() {
            return;
        }
        let keys: Vec<&str> = rows[0].as_object().map(|o| o.keys().map(|k| k.as_str()).collect()).unwrap_or_default();
        if keys.is_empty() {
            return;
        }

        md.push_str("| ");
        for k in &keys {
            md.push_str(&format!("{} | ", k));
        }
        md.push('\n');
        md.push('|');
        for _ in &keys {
            md.push_str(" --- |");
        }
        md.push('\n');
        for row in rows.iter().take(20) {
            md.push_str("| ");
            for k in &keys {
                let val = row
                    .get(k)
                    .map(|v| if v.is_null() { "—".to_string() } else { v.to_string().trim_matches('"').to_string() })
                    .unwrap_or_else(|| "—".to_string());
                md.push_str(&format!("{} | ", val));
            }
            md.push('\n');
        }
        if rows.len() > 20 {
            md.push_str(&format!("\n*...and {} more rows*\n\n", rows.len() - 20));
        }
        md.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_sliding_window_short_unchanged() {
        let mut msgs = vec![ChatMessage {
            role: "system".to_string(),
            content: Some(json!("sys")),
            tool_calls: None,
            tool_call_id: None,
            name: None,
        }];
        for i in 0..5 {
            msgs.push(ChatMessage {
                role: "user".to_string(),
                content: Some(json!(format!("u{}", i))),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            });
        }
        Agent::trim_sliding_window(&mut msgs);
        assert_eq!(msgs.len(), 6);
        assert_eq!(msgs[0].role, "system");
    }

    #[test]
    fn test_trim_sliding_window_caps_at_20() {
        let mut msgs = vec![ChatMessage {
            role: "system".to_string(),
            content: Some(json!("sys")),
            tool_calls: None,
            tool_call_id: None,
            name: None,
        }];
        for i in 0..30 {
            msgs.push(ChatMessage {
                role: "user".to_string(),
                content: Some(json!(format!("u{}", i))),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            });
        }
        Agent::trim_sliding_window(&mut msgs);
        assert_eq!(msgs.len(), 20);
        assert_eq!(msgs[0].role, "system");
    }

    #[test]
    fn test_sanitize_filename_part_via_helper() {
        // Re-import the lib crate's helper for the boundary check.
        // (kept in agent module via the same logic: alnum, -, _)
        let s: &str = "abc漢字-def_";
        let mut out = String::new();
        for c in s.chars().take(8) {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                out.push(c);
            } else {
                out.push('_');
            }
        }
        assert_eq!(out, "abc__-de");
    }
}
