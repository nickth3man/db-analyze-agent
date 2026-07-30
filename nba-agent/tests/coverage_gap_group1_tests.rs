// ---------------------------------------------------------------------------
// Coverage gap tests - Group A: Pure functions and serialization
// ---------------------------------------------------------------------------

use nba_agent::agent::{AgentStreamEvent, ChatMessage, ChatStep, ConversationTrace, ToolCallStep};
use nba_agent::db::{DbContext, DbHistoryEntry, InsightCard, InsightsResponse};
use serde_json::json;

// DbHistoryEntry serialization
#[test]
fn test_db_history_entry_success() {
    let entry = DbHistoryEntry {
        timestamp: 1712000000,
        sql: "SELECT * FROM game".into(),
        row_count: 42,
        elapsed_ms: 12,
        success: true,
    };
    let json = serde_json::to_string(&entry).unwrap();
    assert!(json.contains("SELECT * FROM game"));
    assert!(json.contains("42"));
    assert!(json.contains("true"));
}

#[test]
fn test_db_history_entry_failure() {
    let entry = DbHistoryEntry {
        timestamp: 1712000000,
        sql: "BAD SQL".into(),
        row_count: 0,
        elapsed_ms: 5,
        success: false,
    };
    let json = serde_json::to_string(&entry).unwrap();
    assert!(json.contains("false"));
}

// format_insights_for_prompt
#[test]
fn test_format_insights_for_prompt_successful_cards() {
    let insights = InsightsResponse {
        cards: vec![
            InsightCard {
                id: "1".into(),
                title: "Games".into(),
                value: "50000".into(),
                subtitle: "total rows".into(),
                category: "volume".into(),
                error: None,
            },
        ],
        generated_at: "2024-01-01".into(),
        total_queries: 1,
        successful: 1,
        total_tables: 10,
    };
    let out = DbContext::format_insights_for_prompt(&insights);
    assert!(out.contains("Games: 50000 (total rows)"));
}

#[test]
fn test_format_insights_for_prompt_all_fail() {
    let insights = InsightsResponse {
        cards: vec![
            InsightCard {
                id: "1".into(),
                title: "Games".into(),
                value: "—".into(),
                subtitle: "error".into(),
                category: "volume".into(),
                error: Some("failed".into()),
            },
        ],
        generated_at: "2024-01-01".into(),
        total_queries: 1,
        successful: 0,
        total_tables: 10,
    };
    let out = DbContext::format_insights_for_prompt(&insights);
    assert!(!out.contains("Games"));
}

#[test]
fn test_format_insights_for_prompt_empty() {
    let insights = InsightsResponse {
        cards: vec![],
        generated_at: "2024-01-01".into(),
        total_queries: 0,
        successful: 0,
        total_tables: 0,
    };
    let out = DbContext::format_insights_for_prompt(&insights);
    assert_eq!(out, "Database Insights (pre-computed):\n");
}

// ToolCallStep
#[test]
fn test_tool_call_step_serial() {
    let step = ToolCallStep {
        tool_name: "run_sql".into(),
        reasoning: "need data".into(),
        query_or_params: "SELECT 1".into(),
        result: "1 row".into(),
        elapsed_ms: 42,
        row_count: 1,
    };
    let json = serde_json::to_string(&step).unwrap();
    assert!(json.contains("run_sql"));
    assert!(json.contains("SELECT 1"));
}

// ChatStep
#[test]
fn test_chat_step_content() {
    let step = ChatStep {
        content: Some("hello".into()),
        reasoning: None,
        tool_calls: vec![],
    };
    let json = serde_json::to_string(&step).unwrap();
    assert!(json.contains("hello"));
}

#[test]
fn test_chat_step_with_tool_calls() {
    let step = ChatStep {
        content: None,
        reasoning: Some("thinking".into()),
        tool_calls: vec![ToolCallStep {
            tool_name: "run_sql".into(),
            reasoning: "need data".into(),
            query_or_params: "SELECT 1".into(),
            result: "1".into(),
            elapsed_ms: 10,
            row_count: 1,
        }],
    };
    let json = serde_json::to_string(&step).unwrap();
    assert!(json.contains("run_sql"));
    assert!(json.contains("thinking"));
}

#[test]
fn test_chat_step_none_content() {
    let step = ChatStep {
        content: None,
        reasoning: None,
        tool_calls: vec![],
    };
    let json = serde_json::to_string(&step).unwrap();
    assert!(json.contains("\"content\":null"));
}

// ConversationTrace
#[test]
fn test_conversation_trace_serial() {
    let trace = ConversationTrace {
        session_id: "s1".into(),
        steps: vec![ChatStep {
            content: Some("hi".into()),
            reasoning: None,
            tool_calls: vec![],
        }],
        final_answer: "done".into(),
    };
    let json = serde_json::to_string(&trace).unwrap();
    assert!(json.contains("s1"));
    assert!(json.contains("done"));
}

#[test]
fn test_conversation_trace_empty_steps() {
    let trace = ConversationTrace {
        session_id: "s2".into(),
        steps: vec![],
        final_answer: "empty".into(),
    };
    let json = serde_json::to_string(&trace).unwrap();
    let parsed: ConversationTrace = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.steps.len(), 0);
}

// AgentStreamEvent
#[test]
fn test_event_step_started() {
    let event = AgentStreamEvent::StepStarted { step: 1 };
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("StepStarted"));
}

#[test]
fn test_event_reasoning() {
    let event = AgentStreamEvent::Reasoning { text: "thinking...".into() };
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("thinking..."));
}

#[test]
fn test_event_tool_call_started() {
    let event = AgentStreamEvent::ToolCallStarted {
        tool_name: "run_sql".into(),
        reasoning: "need data".into(),
        query_or_params: "SELECT 1".into(),
    };
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("run_sql"));
}

#[test]
fn test_event_tool_call_result() {
    let event = AgentStreamEvent::ToolCallResult {
        tool_name: "run_sql".into(),
        result: "42".into(),
        elapsed_ms: 10,
        row_count: 1,
    };
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("42"));
}

#[test]
fn test_event_final_answer_chunk() {
    let event = AgentStreamEvent::FinalAnswerChunk { text: "chunk".into() };
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("chunk"));
}

#[test]
fn test_event_completed() {
    let trace = ConversationTrace {
        session_id: "s".into(),
        steps: vec![],
        final_answer: "done".into(),
    };
    let event = AgentStreamEvent::Completed { trace };
    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("Completed"));
}

// ChatMessage
#[test]
fn test_chat_message_user() {
    let msg = ChatMessage {
        role: "user".into(),
        content: Some(json!("hello")),
        tool_calls: None,
        tool_call_id: None,
        name: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("user"));
    assert!(json.contains("hello"));
}

#[test]
fn test_chat_message_tool() {
    let msg = ChatMessage {
        role: "tool".into(),
        content: Some(json!("result")),
        tool_calls: None,
        tool_call_id: Some("call_1".into()),
        name: Some("run_sql".into()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("tool"));
    assert!(json.contains("call_1"));
}

#[test]
fn test_chat_message_skips_none() {
    let msg = ChatMessage {
        role: "user".into(),
        content: Some(json!("hi")),
        tool_calls: None,
        tool_call_id: None,
        name: None,
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(!json.contains("tool_call_id"));
    assert!(!json.contains("tool_calls"));
    assert!(!json.contains("\"name\""));
}

// Agent::append_markdown_table
use nba_agent::agent::Agent;

#[test]
fn test_markdown_table_basic() {
    let rows: Vec<serde_json::Value> = vec![
        json!({"Name": "LeBron", "PTS": 30}),
        json!({"Name": "KD", "PTS": 28}),
    ];
    let mut md = String::new();
    Agent::append_markdown_table(&mut md, &rows);
    assert!(md.contains("LeBron"));
    assert!(md.contains("KD"));
    assert!(md.contains("Name"));
}

#[test]
fn test_markdown_table_empty() {
    let rows: Vec<serde_json::Value> = vec![];
    let mut md = String::new();
    Agent::append_markdown_table(&mut md, &rows);
    assert!(md.is_empty());
}

#[test]
fn test_markdown_table_null_values() {
    let rows: Vec<serde_json::Value> = vec![
        json!({"Name": null, "PTS": 30}),
    ];
    let mut md = String::new();
    Agent::append_markdown_table(&mut md, &rows);
    assert!(md.contains("—"));
    assert!(md.contains("30"));
}

#[test]
fn test_markdown_table_capped() {
    let mut rows: Vec<serde_json::Value> = Vec::new();
    for i in 0..25 {
        rows.push(json!({"N": i}));
    }
    let mut md = String::new();
    Agent::append_markdown_table(&mut md, &rows);
    assert!(md.contains("5 more rows"));
}
