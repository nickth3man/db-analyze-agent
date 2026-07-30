use anyhow::{Result, anyhow};
use async_stream::stream;
use futures::Stream;
use parking_lot::RwLock;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashMap, pin::Pin, sync::Arc};
use uuid::Uuid;

use crate::db::DbContext;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallStep {
    pub tool_name: String,
    pub reasoning: String,
    pub query_or_params: String,
    pub result: String,
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
    ToolCallResult { tool_name: String, result: String },
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

#[derive(Clone)]
pub struct Agent {
    http_client: Client,
    api_key: String,
    db: DbContext,
    schema_summary: String,
    sessions: Arc<RwLock<HashMap<String, Vec<ChatMessage>>>>,
}

impl Agent {
    pub async fn new(db: DbContext) -> Result<Self> {
        let api_key = std::env::var("OPENROUTER_API_KEY")
            .or_else(|_| std::env::var("OPENAI_API_KEY"))
            .map_err(|_| anyhow!("Neither OPENROUTER_API_KEY nor OPENAI_API_KEY set"))?;

        let http_client = Client::builder().build()?;
        let key_tables: Vec<&str> = vec![
            "player", "team", "game", "common_player_info", "player_game_stats",
            "play_by_play", "line_score", "draft_history", "team_history", "team_details",
            "game_summary", "player_career_stats", "player_clutch_stats", "player_shooting_stats",
            "player_defensive_stats", "player_passing_stats", "player_rebounding_stats",
            "team_stats", "award", "coach", "series_post",
        ];
        let enriched = db.build_enriched_schema(Some(&key_tables)).await?;
        let schema_summary = DbContext::format_enriched_schema(&enriched);

        Ok(Self { http_client, api_key, db, schema_summary, sessions: Arc::new(RwLock::new(HashMap::new())) })
    }

    /// Reset session history
    pub fn reset_session(&self, session_id: &str) {
        self.sessions.write().remove(session_id);
    }

    /// System prompt definition
    fn get_system_prompt(&self) -> String {
        format!(
            "You are an expert Data Agent and NBA Analyst with direct access to a 588-table DuckDB warehouse.\n\
            Your task is to answer natural language questions about NBA stats, games, teams, and players with exact, verified data.\n\n\
            AVAILABLE TOOLS:\n\
            1. `run_sql(reasoning, query)` - Execute DuckDB SQL and get JSON output (capped to 50 rows).\n\
            2. `list_tables(pattern)` - List table names matching a pattern (e.g. 'agg_%', 'player%').\n\
            3. `search_tables(keyword)` - Search table & column names across all 588 tables for keywords.\n\
            4. `describe_table(table_name)` - Inspect column names, data types, and sample rows of a specific table.\n\
            5. `explain_query(query)` - Check SQL syntax and get execution plan using EXPLAIN.\n\
            6. `generate_chart(chart_type, title, sql_query)` - Format query results into a chart visualization specification.\n\n\
            BEST PRACTICES:\n\
            • Use the schema context below — it includes row counts, FK relationships (→ arrows), and sample values (e.g.).\n\
            • FK columns (ending in _id) reference the table of the same name (e.g. team_id → team). Use JOINs on these.\n\
            • If a query fails, read the error message's 'Candidate bindings' to find the correct column names.\n\
            • For complex questions, use search_tables first to discover relevant tables beyond those listed below.\n\
            • Synthesize numerical data into concise analytical narrative insights.\n\n\
            {}",
            self.schema_summary
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
                    "description": "Inspect column names, data types, and 3 sample rows for a table.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table_name": { "type": "string", "description": "Name of table to inspect." }
                        },
                        "required": ["table_name"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "explain_query",
                    "description": "Check SQL syntax and get execution plan using EXPLAIN.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": { "type": "string", "description": "SQL query to check." }
                        },
                        "required": ["query"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "generate_chart",
                    "description": "Generate chart visualization spec for query results.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chart_type": { "type": "string", "enum": ["bar", "line", "scatter", "pie"] },
                            "title": { "type": "string", "description": "Chart title." },
                            "sql_query": { "type": "string", "description": "SQL query producing data for the chart." }
                        },
                        "required": ["chart_type", "title", "sql_query"]
                    }
                }
            }
        ])
    }

    /// Execute multi-turn conversation turn
    pub async fn run_conversation(
        &self,
        session_id_opt: Option<String>,
        user_message: &str,
    ) -> Result<ConversationTrace> {
        let session_id = session_id_opt.unwrap_or_else(|| Uuid::new_v4().to_string());
        let system_prompt = self.get_system_prompt();
        let tools = self.get_tools_json();

        let mut messages = {
            let map = self.sessions.read();
            map.get(&session_id).cloned().unwrap_or_else(|| {
                vec![ChatMessage {
                    role: "system".to_string(),
                    content: Some(json!(system_prompt)),
                    tool_calls: None,
                    tool_call_id: None,
                    name: None,
                }]
            })
        };

        messages.push(ChatMessage {
            role: "user".to_string(),
            content: Some(json!(user_message)),
            tool_calls: None,
            tool_call_id: None,
            name: None,
        });

        let mut trace =
            ConversationTrace { session_id: session_id.clone(), steps: Vec::new(), final_answer: String::new() };

        let mut iterations = 0;
        let max_iterations = 12;

        loop {
            if iterations >= max_iterations {
                trace.final_answer =
                    "Reached max analytical reasoning steps. Here are the findings so far.".to_string();
                break;
            }
            iterations += 1;

            let req_body = json!({
                "model": "qwen/qwen3.7-flash",
                "messages": messages,
                "tools": tools,
                "reasoning": {
                    "exclude": false
                }
            });

            let res = self
                .http_client
                .post("https://openrouter.ai/api/v1/chat/completions")
                .header("Authorization", format!("Bearer {}", self.api_key))
                .header("HTTP-Referer", "https://github.com/db-analyze-agent")
                .header("X-Title", "NBA Data Agent")
                .json(&req_body)
                .send()
                .await?;

            if !res.status().is_success() {
                let err_text = res.text().await?;
                return Err(anyhow!("OpenRouter API error: {}", err_text));
            }

            let res_json: Value = res.json().await?;
            let choice = &res_json["choices"][0];
            let msg_val = &choice["message"];

            let content_str = msg_val["content"].as_str().map(|s| s.to_string());
            let reasoning_str = msg_val["reasoning"].as_str().map(|s| s.to_string());
            let tool_calls_val = msg_val["tool_calls"].as_array();

            let mut step = ChatStep { content: content_str.clone(), reasoning: reasoning_str, tool_calls: Vec::new() };

            let assistant_content = if msg_val["content"].is_null() { None } else { Some(msg_val["content"].clone()) };
            let assistant_tool_calls =
                if msg_val["tool_calls"].is_null() { None } else { Some(msg_val["tool_calls"].clone()) };

            messages.push(ChatMessage {
                role: "assistant".to_string(),
                content: assistant_content,
                tool_calls: assistant_tool_calls,
                tool_call_id: None,
                name: None,
            });

            if let Some(tool_calls) = tool_calls_val {
                for tool_call in tool_calls {
                    let call_id = tool_call["id"].as_str().unwrap_or("").to_string();
                    let fn_val = &tool_call["function"];
                    let fn_name = fn_val["name"].as_str().unwrap_or("");
                    let fn_args_str = fn_val["arguments"].as_str().unwrap_or("{}");
                    let parsed_args: Value = serde_json::from_str(fn_args_str).unwrap_or_default();

                    let (reasoning, param_str, result_str) = self.execute_tool(fn_name, &parsed_args).await;

                    step.tool_calls.push(ToolCallStep {
                        tool_name: fn_name.to_string(),
                        reasoning,
                        query_or_params: param_str,
                        result: result_str.clone(),
                    });

                    messages.push(ChatMessage {
                        role: "tool".to_string(),
                        content: Some(json!(result_str)),
                        tool_calls: None,
                        tool_call_id: Some(call_id),
                        name: Some(fn_name.to_string()),
                    });
                }
                trace.steps.push(step);
            } else {
                if let Some(c) = content_str {
                    trace.final_answer = c;
                }
                break;
            }
        }

        self.sessions.write().insert(session_id, messages);

        Ok(trace)
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
            let system_prompt = this.get_system_prompt();
            let tools = this.get_tools_json();

            let mut messages = {
                let map = this.sessions.read();
                map.get(&session_id).cloned().unwrap_or_else(|| {
                    vec![ChatMessage {
                        role: "system".to_string(),
                        content: Some(json!(system_prompt)),
                        tool_calls: None,
                        tool_call_id: None,
                        name: None,
                    }]
                })
            };

            messages.push(ChatMessage {
                role: "user".to_string(),
                content: Some(json!(user_message)),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            });

            let mut trace = ConversationTrace {
                session_id: session_id.clone(),
                steps: Vec::new(),
                final_answer: String::new(),
            };

            let mut iterations = 0;
            let max_iterations = 12;

            loop {
                if iterations >= max_iterations {
                    trace.final_answer = "Reached max analytical reasoning steps.".to_string();
                    yield Ok(AgentStreamEvent::FinalAnswerChunk { text: trace.final_answer.clone() });
                    break;
                }
                iterations += 1;

                yield Ok(AgentStreamEvent::StepStarted { step: iterations });

                let req_body = json!({
                    "model": "qwen/qwen3.7-flash",
                    "messages": messages,
                    "tools": tools,
                    "reasoning": { "exclude": false }
                });

                let res = match this
                    .http_client
                    .post("https://openrouter.ai/api/v1/chat/completions")
                    .header("Authorization", format!("Bearer {}", this.api_key))
                    .header("HTTP-Referer", "https://github.com/db-analyze-agent")
                    .header("X-Title", "NBA Data Agent")
                    .json(&req_body)
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        yield Ok(AgentStreamEvent::Error { message: e.to_string() });
                        return;
                    }
                };

                if !res.status().is_success() {
                    let err_text = res.text().await.unwrap_or_default();
                    yield Ok(AgentStreamEvent::Error { message: format!("OpenRouter Error: {}", err_text) });
                    return;
                }

                let res_json: Value = match res.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        yield Ok(AgentStreamEvent::Error { message: e.to_string() });
                        return;
                    }
                };

                let choice = &res_json["choices"][0];
                let msg_val = &choice["message"];

                let content_str = msg_val["content"].as_str().map(|s| s.to_string());
                let reasoning_str = msg_val["reasoning"].as_str().map(|s| s.to_string());
                let tool_calls_val = msg_val["tool_calls"].as_array();

                if let Some(r_text) = &reasoning_str {
                    yield Ok(AgentStreamEvent::Reasoning { text: r_text.clone() });
                }

                let mut step = ChatStep {
                    content: content_str.clone(),
                    reasoning: reasoning_str,
                    tool_calls: Vec::new(),
                };

                let assistant_content = if msg_val["content"].is_null() { None } else { Some(msg_val["content"].clone()) };
                let assistant_tool_calls = if msg_val["tool_calls"].is_null() { None } else { Some(msg_val["tool_calls"].clone()) };

                messages.push(ChatMessage {
                    role: "assistant".to_string(),
                    content: assistant_content,
                    tool_calls: assistant_tool_calls,
                    tool_call_id: None,
                    name: None,
                });

                if let Some(tool_calls) = tool_calls_val {
                    for tool_call in tool_calls {
                        let call_id = tool_call["id"].as_str().unwrap_or("").to_string();
                        let fn_val = &tool_call["function"];
                        let fn_name = fn_val["name"].as_str().unwrap_or("");
                        let fn_args_str = fn_val["arguments"].as_str().unwrap_or("{}");
                        let parsed_args: Value = serde_json::from_str(fn_args_str).unwrap_or_default();

                        let (reasoning, param_str, result_str) = this.execute_tool(fn_name, &parsed_args).await;

                        yield Ok(AgentStreamEvent::ToolCallStarted {
                            tool_name: fn_name.to_string(),
                            reasoning: reasoning.clone(),
                            query_or_params: param_str.clone(),
                        });

                        yield Ok(AgentStreamEvent::ToolCallResult {
                            tool_name: fn_name.to_string(),
                            result: result_str.clone(),
                        });

                        step.tool_calls.push(ToolCallStep {
                            tool_name: fn_name.to_string(),
                            reasoning,
                            query_or_params: param_str,
                            result: result_str.clone(),
                        });

                        messages.push(ChatMessage {
                            role: "tool".to_string(),
                            content: Some(json!(result_str)),
                            tool_calls: None,
                            tool_call_id: Some(call_id),
                            name: Some(fn_name.to_string()),
                        });
                    }
                    trace.steps.push(step);
                } else {
                    if let Some(c) = content_str {
                        trace.final_answer = c.clone();
                        yield Ok(AgentStreamEvent::FinalAnswerChunk { text: c });
                    }
                    break;
                }
            }

            this.sessions.write().insert(session_id, messages);
            yield Ok(AgentStreamEvent::Completed { trace });
        })
    }

    /// Dispatch tool calls to DbContext
    async fn execute_tool(&self, name: &str, args: &Value) -> (String, String, String) {
        match name {
            "run_sql" => {
                let reasoning = args["reasoning"].as_str().unwrap_or("Executing query").to_string();
                let query = args["query"].as_str().unwrap_or("").to_string();
                let result_str = match self.db.run_sql(query.clone(), None).await {
                    Ok(rows) => serde_json::to_string_pretty(&rows).unwrap_or_default(),
                    Err(e) => {
                        let err_msg = e.to_string();
                        // Try auto-fix using DuckDB's candidate bindings
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
            _ => ("Unknown tool".to_string(), name.to_string(), format!("Tool `{}` not supported.", name)),
        }
    }
}
