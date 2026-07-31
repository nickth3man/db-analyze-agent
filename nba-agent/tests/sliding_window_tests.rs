// ---------------------------------------------------------------------------
// Sliding window memory tests
// ---------------------------------------------------------------------------

use nba_agent::agent::Agent;
use nba_agent::agent::ChatMessage;

fn make_msg(role: &str, content: &str) -> ChatMessage {
    ChatMessage {
        role: role.to_string(),
        content: Some(serde_json::json!(content)),
        tool_calls: None,
        tool_call_id: None,
        name: None,
    }
}

fn make_system() -> ChatMessage {
    make_msg("system", "You are a helpful assistant.")
}

fn make_user(n: usize) -> ChatMessage {
    make_msg("user", &format!("Question {}", n))
}

fn make_assistant(n: usize) -> ChatMessage {
    make_msg("assistant", &format!("Answer {}", n))
}

#[test]
fn test_trim_noop_when_under_window() {
    let mut msgs = vec![make_system(), make_user(1), make_assistant(1)];
    let original_len = msgs.len();
    Agent::trim_sliding_window(&mut msgs);
    assert_eq!(msgs.len(), original_len, "Should not trim when under window");
    assert_eq!(msgs[0].role, "system", "System message stays at index 0");
}

#[test]
fn test_trim_noop_at_window_boundary() {
    // 1 system + 19 messages = 20 total = at window boundary
    let mut msgs = vec![make_system()];
    for i in 1..=19 {
        msgs.push(make_user(i));
    }
    assert_eq!(msgs.len(), 20);
    Agent::trim_sliding_window(&mut msgs);
    assert_eq!(msgs.len(), 20, "Should not trim exactly at window size");
    assert_eq!(msgs[0].role, "system");
}

#[test]
fn test_trim_drops_oldest_when_over_window() {
    // 1 system + 25 messages = 26 total, should trim to 20
    let mut msgs = vec![make_system()];
    for i in 1..=25 {
        msgs.push(make_user(i));
    }
    assert_eq!(msgs.len(), 26);
    Agent::trim_sliding_window(&mut msgs);
    assert_eq!(msgs.len(), 20, "Should trim to exactly MAX_WINDOW");
    assert_eq!(msgs[0].role, "system", "System message still at index 0");
    // Oldest messages (users 1-6) should be gone
    let first_content = msgs[1].content.as_ref().unwrap().as_str().unwrap();
    assert!(!first_content.contains("Question 1"), "Oldest messages should be dropped");
    // Most recent message should be user 25
    let last_content = msgs.last().unwrap().content.as_ref().unwrap().as_str().unwrap();
    assert!(last_content.contains("Question 25"), "Most recent message should survive");
}

#[test]
fn test_trim_keeps_system_prompt_always() {
    let mut msgs = vec![make_system()];
    for i in 1..=30 {
        msgs.push(make_user(i));
    }
    Agent::trim_sliding_window(&mut msgs);
    assert_eq!(msgs[0].role, "system", "System must stay at index 0");
    // Verify system content is preserved
    let sys_content = msgs[0].content.as_ref().unwrap().as_str().unwrap();
    assert!(sys_content.contains("helpful assistant"), "System content must be intact");
}

#[test]
fn test_trim_large_session() {
    // 1 system + 100 messages = 101 total
    let mut msgs = vec![make_system()];
    for i in 1..=100 {
        msgs.push(if i % 2 == 0 { make_assistant(i) } else { make_user(i) });
    }
    Agent::trim_sliding_window(&mut msgs);
    assert_eq!(msgs.len(), 20, "Should trim to window size");
    assert_eq!(msgs[0].role, "system");
    // Last message should be from the end (user 99 or assistant 100)
    assert!(
        msgs[19].content.as_ref().unwrap().as_str().unwrap().contains("100")
            || msgs[19].content.as_ref().unwrap().as_str().unwrap().contains("99")
    );
}

#[test]
fn test_trim_multiple_calls_idempotent() {
    let mut msgs = vec![make_system()];
    for i in 1..=30 {
        msgs.push(make_user(i));
    }
    Agent::trim_sliding_window(&mut msgs);
    let len_after_first = msgs.len();
    Agent::trim_sliding_window(&mut msgs);
    assert_eq!(msgs.len(), len_after_first, "Second trim should be no-op");
}
