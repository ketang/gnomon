use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::import::NormalizedToolUsePartMetadata;
use crate::perf::{PerfLogger, PerfScope};

#[derive(Debug, Clone)]
pub struct BuildActionsParams {
    pub conversation_id: i64,
    pub perf_logger: Option<PerfLogger>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildActionsResult {
    pub action_count: usize,
    pub path_ref_count: usize,
}

pub fn build_actions(
    conn: &mut Connection,
    params: &BuildActionsParams,
) -> Result<BuildActionsResult> {
    let mut scope = PerfScope::new(params.perf_logger.clone(), "import.build_actions");
    scope.field("conversation_id", params.conversation_id);
    let result = build_actions_inner(conn, params);
    match &result {
        Ok(outcome) => {
            scope.field("action_count", outcome.action_count);
            scope.field("path_ref_count", outcome.path_ref_count);
            scope.finish_ok();
        }
        Err(err) => scope.finish_error(err),
    }
    result
}

fn build_actions_core(
    conn: &Connection,
    params: &BuildActionsParams,
) -> Result<BuildActionsResult> {
    let Some(context) = load_conversation_context(conn, params.conversation_id)? else {
        return Ok(BuildActionsResult {
            action_count: 0,
            path_ref_count: 0,
        });
    };

    purge_existing_classification(conn, params.conversation_id)?;
    let messages = load_messages(conn, params.conversation_id)?;
    let tool_use_lookup = build_tool_use_lookup(&messages);

    let mut actions_written = 0usize;
    let mut path_refs_written = 0usize;

    let mut turns: BTreeMap<i64, Vec<&LoadedMessage>> = BTreeMap::new();
    for message in &messages {
        if let Some(turn_sequence_no) = message.turn_sequence_no {
            turns.entry(turn_sequence_no).or_default().push(message);
        }
    }

    for messages_in_turn in turns.values_mut() {
        messages_in_turn.sort_by_key(|message| message.ordinal_in_turn.unwrap_or(i64::MAX));
    }

    for messages_in_turn in turns.values() {
        let mut next_action_sequence_no = 0i64;
        let mut current_group: Option<ActionGroupDraft> = None;

        for message in messages_in_turn {
            let classification = classify_message(message, &tool_use_lookup);
            path_refs_written += persist_path_refs(
                conn,
                context.project_id,
                &context.project_root,
                message.id,
                &classification.path_refs,
            )?;

            if let Some(group) = current_group.as_mut() {
                if group.descriptor == classification.descriptor {
                    group.push_message(message, &classification);
                    continue;
                }

                persist_action(
                    conn,
                    context.import_chunk_id,
                    group.turn_id,
                    next_action_sequence_no,
                    current_group.take().expect("action group present"),
                )?;
                actions_written += 1;
                next_action_sequence_no += 1;
            }

            if let Some(turn_id) = message.turn_id {
                current_group = Some(ActionGroupDraft::new(turn_id, message, &classification));
            }
        }

        if let Some(group) = current_group.take() {
            persist_action(
                conn,
                context.import_chunk_id,
                group.turn_id,
                next_action_sequence_no,
                group,
            )?;
            actions_written += 1;
        }
    }

    conn.execute(
        "
        UPDATE import_chunk
        SET imported_action_count = (
            SELECT COUNT(*)
            FROM action
            WHERE import_chunk_id = ?1
        )
        WHERE id = ?1
        ",
        [context.import_chunk_id],
    )
    .context("unable to update import chunk action count")?;

    Ok(BuildActionsResult {
        action_count: actions_written,
        path_ref_count: path_refs_written,
    })
}

fn build_actions_inner(
    conn: &mut Connection,
    params: &BuildActionsParams,
) -> Result<BuildActionsResult> {
    let tx = conn
        .transaction()
        .context("unable to start action classification transaction")?;
    let result = build_actions_core(&tx, params)?;
    tx.commit()
        .context("unable to commit action classification transaction")?;
    Ok(result)
}

/// Classify actions for a conversation within an externally-managed transaction.
pub fn build_actions_in_tx(
    conn: &Connection,
    params: &BuildActionsParams,
) -> Result<BuildActionsResult> {
    let mut scope = PerfScope::new(params.perf_logger.clone(), "import.build_actions");
    scope.field("conversation_id", params.conversation_id);
    let result = build_actions_core(conn, params);
    match &result {
        Ok(outcome) => {
            scope.field("action_count", outcome.action_count);
            scope.field("path_ref_count", outcome.path_ref_count);
            scope.finish_ok();
        }
        Err(err) => scope.finish_error(err),
    }
    result
}

#[derive(Debug)]
struct ConversationContext {
    project_id: i64,
    project_root: PathBuf,
    import_chunk_id: i64,
}

fn load_conversation_context(
    conn: &Connection,
    conversation_id: i64,
) -> Result<Option<ConversationContext>> {
    conn.query_row(
        "
        SELECT c.project_id, p.root_path, m.import_chunk_id
        FROM conversation c
        JOIN project p ON p.id = c.project_id
        JOIN message m ON m.conversation_id = c.id
        WHERE c.id = ?1
        ORDER BY m.sequence_no
        LIMIT 1
        ",
        [conversation_id],
        |row| {
            Ok(ConversationContext {
                project_id: row.get(0)?,
                project_root: PathBuf::from(row.get::<_, String>(1)?),
                import_chunk_id: row.get(2)?,
            })
        },
    )
    .optional()
    .context("unable to load conversation context for action classification")
}

fn purge_existing_classification(conn: &Connection, conversation_id: i64) -> Result<()> {
    conn.execute(
        "
        DELETE FROM message_path_ref
        WHERE message_id IN (
            SELECT id
            FROM message
            WHERE conversation_id = ?1
        )
        ",
        [conversation_id],
    )
    .context("unable to clear existing message path refs for conversation")?;

    conn.execute(
        "
        DELETE FROM action
        WHERE turn_id IN (
            SELECT id
            FROM turn
            WHERE conversation_id = ?1
        )
        ",
        [conversation_id],
    )
    .context("unable to clear existing actions for conversation")?;

    Ok(())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Usage {
    input_tokens: Option<i64>,
    cache_creation_input_tokens: Option<i64>,
    cache_read_input_tokens: Option<i64>,
    output_tokens: Option<i64>,
}

#[derive(Debug, Clone)]
struct LoadedPart {
    id: i64,
    part_kind: String,
    tool_name: Option<String>,
    tool_call_id: Option<String>,
    metadata_json: Option<String>,
}

#[derive(Debug, Clone)]
struct LoadedMessage {
    id: i64,
    turn_id: Option<i64>,
    turn_sequence_no: Option<i64>,
    ordinal_in_turn: Option<i64>,
    message_kind: String,
    created_at_utc: Option<String>,
    completed_at_utc: Option<String>,
    usage: Usage,
    parts: Vec<LoadedPart>,
}

fn load_messages(conn: &Connection, conversation_id: i64) -> Result<Vec<LoadedMessage>> {
    let mut stmt = conn.prepare(
        "
        SELECT
            m.id,
            tm.turn_id,
            t.sequence_no,
            tm.ordinal_in_turn,
            m.message_kind,
            m.created_at_utc,
            m.completed_at_utc,
            m.input_tokens,
            m.cache_creation_input_tokens,
            m.cache_read_input_tokens,
            m.output_tokens,
            mp.id,
            mp.part_kind,
            mp.tool_name,
            mp.tool_call_id,
            mp.metadata_json
        FROM message m
        LEFT JOIN turn_message tm ON tm.message_id = m.id
        LEFT JOIN turn t ON t.id = tm.turn_id
        LEFT JOIN message_part mp ON mp.message_id = m.id
        WHERE m.conversation_id = ?1
        ORDER BY m.sequence_no, mp.ordinal
        ",
    )?;

    let rows = stmt.query_map([conversation_id], |row| {
        Ok(LoadedMessageRow {
            id: row.get(0)?,
            turn_id: row.get(1)?,
            turn_sequence_no: row.get(2)?,
            ordinal_in_turn: row.get(3)?,
            message_kind: row.get(4)?,
            created_at_utc: row.get(5)?,
            completed_at_utc: row.get(6)?,
            usage: Usage {
                input_tokens: row.get(7)?,
                cache_creation_input_tokens: row.get(8)?,
                cache_read_input_tokens: row.get(9)?,
                output_tokens: row.get(10)?,
            },
            part_id: row.get(11)?,
            part_kind: row.get(12)?,
            part_tool_name: row.get(13)?,
            part_tool_call_id: row.get(14)?,
            part_metadata_json: row.get(15)?,
        })
    })?;

    let mut messages = Vec::new();
    let mut index_by_message_id = HashMap::new();

    for row in rows {
        let row = row?;
        let message_index = if let Some(&existing_index) = index_by_message_id.get(&row.id) {
            existing_index
        } else {
            let index = messages.len();
            messages.push(LoadedMessage {
                id: row.id,
                turn_id: row.turn_id,
                turn_sequence_no: row.turn_sequence_no,
                ordinal_in_turn: row.ordinal_in_turn,
                message_kind: row.message_kind,
                created_at_utc: row.created_at_utc,
                completed_at_utc: row.completed_at_utc,
                usage: row.usage,
                parts: Vec::new(),
            });
            index_by_message_id.insert(row.id, index);
            index
        };

        if let Some(part_id) = row.part_id {
            messages[message_index].parts.push(LoadedPart {
                id: part_id,
                part_kind: row.part_kind.unwrap_or_else(|| "unknown".to_string()),
                tool_name: row.part_tool_name,
                tool_call_id: row.part_tool_call_id,
                metadata_json: row.part_metadata_json,
            });
        }
    }

    Ok(messages)
}

#[derive(Debug)]
struct LoadedMessageRow {
    id: i64,
    turn_id: Option<i64>,
    turn_sequence_no: Option<i64>,
    ordinal_in_turn: Option<i64>,
    message_kind: String,
    created_at_utc: Option<String>,
    completed_at_utc: Option<String>,
    usage: Usage,
    part_id: Option<i64>,
    part_kind: Option<String>,
    part_tool_name: Option<String>,
    part_tool_call_id: Option<String>,
    part_metadata_json: Option<String>,
}

#[derive(Debug, Clone)]
struct ToolInvocation {
    tool_name: String,
    input: Option<Value>,
}

fn build_tool_use_lookup(messages: &[LoadedMessage]) -> HashMap<String, ToolInvocation> {
    let mut lookup = HashMap::new();

    for message in messages {
        for part in &message.parts {
            if part.part_kind != "tool_use" {
                continue;
            }
            let Some(tool_call_id) = part.tool_call_id.clone() else {
                continue;
            };
            let Some(tool_name) = part.tool_name.clone() else {
                continue;
            };
            let input = part
                .metadata_json
                .as_deref()
                .and_then(tool_use_input_from_metadata_json);

            lookup.insert(tool_call_id, ToolInvocation { tool_name, input });
        }
    }

    lookup
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ActionDescriptor {
    classification_state: &'static str,
    category: Option<String>,
    normalized_action: Option<String>,
    command_family: Option<String>,
    base_command: Option<String>,
}

impl ActionDescriptor {
    fn classified(
        category: impl Into<String>,
        normalized_action: impl Into<String>,
        command_family: Option<impl Into<String>>,
        base_command: Option<impl Into<String>>,
    ) -> Self {
        Self {
            classification_state: "classified",
            category: Some(category.into()),
            normalized_action: Some(normalized_action.into()),
            command_family: command_family.map(Into::into),
            base_command: base_command.map(Into::into),
        }
    }

    fn mixed() -> Self {
        Self {
            classification_state: "mixed",
            category: None,
            normalized_action: None,
            command_family: None,
            base_command: None,
        }
    }

    fn unclassified() -> Self {
        Self {
            classification_state: "unclassified",
            category: None,
            normalized_action: None,
            command_family: None,
            base_command: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PathRefSpec {
    message_part_id: i64,
    full_path: PathBuf,
    ref_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActionSkillAttributionDraft {
    skill_name: String,
    confidence: &'static str,
}

#[derive(Debug)]
struct MessageClassification {
    descriptor: ActionDescriptor,
    path_refs: Vec<PathRefSpec>,
    skill_references: Vec<String>,
}

fn classify_message(
    message: &LoadedMessage,
    tool_use_lookup: &HashMap<String, ToolInvocation>,
) -> MessageClassification {
    let mut descriptors = HashSet::new();
    let mut path_refs = HashSet::new();
    let mut skill_references = HashSet::new();

    for part in &message.parts {
        match part.part_kind.as_str() {
            "tool_use" => {
                if let Some(tool_invocation) = tool_invocation_from_part(part) {
                    descriptors.insert(classify_tool_invocation(&tool_invocation));
                    if let Some(path_ref) = explicit_path_ref_from_tool_use(part, &tool_invocation)
                    {
                        path_refs.insert(path_ref);
                    }
                    skill_references.extend(explicit_skill_references_from_tool(&tool_invocation));
                }
            }
            "tool_result" => {
                let Some(tool_call_id) = part.tool_call_id.as_deref() else {
                    continue;
                };
                if let Some(tool_invocation) = tool_use_lookup.get(tool_call_id) {
                    descriptors.insert(classify_tool_invocation(tool_invocation));
                }
            }
            _ => {}
        }
    }

    let descriptor = if descriptors.len() == 1 {
        descriptors.into_iter().next().expect("single descriptor")
    } else if descriptors.len() > 1 {
        ActionDescriptor::mixed()
    } else {
        fallback_descriptor(message)
    };

    let mut path_refs: Vec<PathRefSpec> = path_refs.into_iter().collect();
    path_refs.sort_by(|left, right| left.full_path.cmp(&right.full_path));
    let mut skill_references: Vec<String> = skill_references.into_iter().collect();
    skill_references.sort();

    MessageClassification {
        descriptor,
        path_refs,
        skill_references,
    }
}

fn tool_invocation_from_part(part: &LoadedPart) -> Option<ToolInvocation> {
    let tool_name = part.tool_name.clone()?;
    let input = part
        .metadata_json
        .as_deref()
        .and_then(tool_use_input_from_metadata_json);

    Some(ToolInvocation { tool_name, input })
}

fn classify_tool_invocation(tool: &ToolInvocation) -> ActionDescriptor {
    match tool.tool_name.as_str() {
        "Read" => ActionDescriptor::classified(
            "project discovery",
            "file read",
            Some("explicit file tool"),
            Some("Read"),
        ),
        "Write" => {
            if explicit_file_path(tool)
                .as_deref()
                .is_some_and(is_documentation_path)
            {
                ActionDescriptor::classified(
                    "documentation writing",
                    "document write",
                    Some("explicit file tool"),
                    Some("Write"),
                )
            } else {
                ActionDescriptor::classified(
                    "editing",
                    "file write",
                    Some("explicit file tool"),
                    Some("Write"),
                )
            }
        }
        "Edit" => {
            if explicit_file_path(tool)
                .as_deref()
                .is_some_and(is_documentation_path)
            {
                ActionDescriptor::classified(
                    "documentation writing",
                    "document edit",
                    Some("explicit file tool"),
                    Some("Edit"),
                )
            } else {
                ActionDescriptor::classified(
                    "editing",
                    "file edit",
                    Some("explicit file tool"),
                    Some("Edit"),
                )
            }
        }
        "MultiEdit" => {
            if explicit_file_path(tool)
                .as_deref()
                .is_some_and(is_documentation_path)
            {
                ActionDescriptor::classified(
                    "documentation writing",
                    "document edit",
                    Some("explicit file tool"),
                    Some("MultiEdit"),
                )
            } else {
                ActionDescriptor::classified(
                    "editing",
                    "file edit",
                    Some("explicit file tool"),
                    Some("MultiEdit"),
                )
            }
        }
        "Glob" => ActionDescriptor::classified(
            "local search/navigation",
            "file glob",
            Some("glob search"),
            Some("Glob"),
        ),
        "Grep" => ActionDescriptor::classified(
            "local search/navigation",
            "content search",
            Some("grep search"),
            Some("Grep"),
        ),
        "Bash" => classify_shell_command(tool),
        "ToolSearch" => classify_tool_search(tool),
        "WebSearch" => ActionDescriptor::classified(
            "external/web research",
            "web search",
            Some("web research"),
            Some("WebSearch"),
        ),
        "WebFetch" => ActionDescriptor::classified(
            "external/web research",
            "web fetch",
            Some("web research"),
            Some("WebFetch"),
        ),
        "TeamCreate" => ActionDescriptor::classified(
            "team communication/coordination",
            "create team",
            Some("team orchestration"),
            Some("TeamCreate"),
        ),
        "SendMessage" => ActionDescriptor::classified(
            "team communication/coordination",
            "send message",
            Some("team orchestration"),
            Some("SendMessage"),
        ),
        "TaskUpdate" | "TaskOutput" => ActionDescriptor::classified(
            "team communication/coordination",
            "task coordination",
            Some("team orchestration"),
            Some(tool.tool_name.clone()),
        ),
        "AskUserQuestion" => ActionDescriptor::classified(
            "planning/reasoning",
            "ask user",
            Some("workflow control"),
            Some("AskUserQuestion"),
        ),
        "EnterPlanMode" | "ExitPlanMode" => ActionDescriptor::classified(
            "planning/reasoning",
            "workflow control",
            Some("workflow control"),
            Some(tool.tool_name.clone()),
        ),
        "EnterWorktree" | "ExitWorktree" => ActionDescriptor::classified(
            "planning/reasoning",
            "worktree control",
            Some("workflow control"),
            Some(tool.tool_name.clone()),
        ),
        _ => ActionDescriptor::unclassified(),
    }
}

fn classify_tool_search(tool: &ToolInvocation) -> ActionDescriptor {
    let query = tool
        .input
        .as_ref()
        .and_then(|input| input.get("query"))
        .and_then(Value::as_str)
        .unwrap_or_default();

    if query.contains("SendMessage")
        || query.contains("TeamCreate")
        || query.contains("TaskUpdate")
        || query.contains("TaskOutput")
    {
        ActionDescriptor::classified(
            "team communication/coordination",
            "tool discovery",
            Some("tool search"),
            Some("ToolSearch"),
        )
    } else {
        ActionDescriptor::classified(
            "planning/reasoning",
            "tool discovery",
            Some("tool search"),
            Some("ToolSearch"),
        )
    }
}

fn classify_shell_command(tool: &ToolInvocation) -> ActionDescriptor {
    let Some(command) = tool
        .input
        .as_ref()
        .and_then(|input| input.get("command"))
        .and_then(Value::as_str)
    else {
        return ActionDescriptor::unclassified();
    };

    if contains_shell_separator(command) {
        return ActionDescriptor::mixed();
    }

    let words = shell_words(command);
    let Some(base_command) = words.first().cloned() else {
        return ActionDescriptor::unclassified();
    };
    let subcommand = words.get(1).map(String::as_str);

    match base_command.as_str() {
        "cargo" => match subcommand {
            Some("test") => ActionDescriptor::classified(
                "test/build/run",
                "test run",
                Some("cargo test"),
                Some("cargo"),
            ),
            Some("build") | Some("check") | Some("run") => ActionDescriptor::classified(
                "test/build/run",
                "build run",
                Some(format!("cargo {}", subcommand.expect("subcommand present"))),
                Some("cargo"),
            ),
            _ => ActionDescriptor::unclassified(),
        },
        "pnpm" | "npm" | "yarn" | "bun" => match subcommand {
            Some("test") => ActionDescriptor::classified(
                "test/build/run",
                "test run",
                Some(format!("{base_command} test")),
                Some(base_command),
            ),
            Some("build") => ActionDescriptor::classified(
                "test/build/run",
                "build run",
                Some(format!("{base_command} build")),
                Some(base_command),
            ),
            _ => ActionDescriptor::unclassified(),
        },
        "pytest" => ActionDescriptor::classified(
            "test/build/run",
            "test run",
            Some("pytest"),
            Some("pytest"),
        ),
        "go" => match subcommand {
            Some("test") => ActionDescriptor::classified(
                "test/build/run",
                "test run",
                Some("go test"),
                Some("go"),
            ),
            Some("build") | Some("run") => ActionDescriptor::classified(
                "test/build/run",
                "build run",
                Some(format!("go {}", subcommand.expect("subcommand present"))),
                Some("go"),
            ),
            _ => ActionDescriptor::unclassified(),
        },
        "git" => match subcommand {
            Some("status") | Some("diff") | Some("show") | Some("log") | Some("branch")
            | Some("rev-parse") => ActionDescriptor::classified(
                "data traffic",
                "git inspection",
                Some(format!("git {}", subcommand.expect("subcommand present"))),
                Some("git"),
            ),
            Some("add") | Some("commit") | Some("restore") | Some("checkout") | Some("merge")
            | Some("rebase") | Some("pull") | Some("push") => ActionDescriptor::classified(
                "data traffic",
                "git mutation",
                Some(format!("git {}", subcommand.expect("subcommand present"))),
                Some("git"),
            ),
            _ => ActionDescriptor::classified(
                "data traffic",
                "git command",
                Some("git"),
                Some("git"),
            ),
        },
        "rg" | "grep" => ActionDescriptor::classified(
            "local search/navigation",
            "content search",
            Some(format!("{base_command} search")),
            Some(base_command),
        ),
        "find" => ActionDescriptor::classified(
            "local search/navigation",
            "filesystem find",
            Some("find search"),
            Some("find"),
        ),
        "ls" | "tree" => ActionDescriptor::classified(
            "local search/navigation",
            "directory inspection",
            Some("shell inspection"),
            Some(base_command),
        ),
        "cat" => ActionDescriptor::classified(
            "project discovery",
            "shell file read",
            Some("shell inspection"),
            Some("cat"),
        ),
        "curl" | "wget" => ActionDescriptor::classified(
            "external/web research",
            "web fetch",
            Some("shell fetch"),
            Some(base_command),
        ),
        _ => ActionDescriptor::unclassified(),
    }
}

fn fallback_descriptor(message: &LoadedMessage) -> ActionDescriptor {
    match message.message_kind.as_str() {
        "user_prompt" => {
            ActionDescriptor::classified("user input", "prompt", None::<String>, None::<String>)
        }
        "relay_user_prompt" => ActionDescriptor::classified(
            "team communication/coordination",
            "relay prompt",
            Some("agent relay"),
            None::<String>,
        ),
        "relay_assistant_message" => ActionDescriptor::classified(
            "team communication/coordination",
            "relay response",
            Some("agent relay"),
            None::<String>,
        ),
        "agent_run_summary" => ActionDescriptor::classified(
            "team communication/coordination",
            "agent summary",
            Some("agent relay"),
            None::<String>,
        ),
        "assistant_message" => ActionDescriptor::classified(
            "planning/reasoning",
            "assistant reasoning",
            Some("assistant text"),
            None::<String>,
        ),
        _ => ActionDescriptor::unclassified(),
    }
}

fn explicit_path_ref_from_tool_use(
    part: &LoadedPart,
    tool: &ToolInvocation,
) -> Option<PathRefSpec> {
    let ref_kind = match tool.tool_name.as_str() {
        "Read" => "read",
        "Write" => "write",
        "Edit" => "edit",
        "MultiEdit" => "multiedit",
        _ => return None,
    };

    let full_path = explicit_file_path(tool)?;
    Some(PathRefSpec {
        message_part_id: part.id,
        full_path,
        ref_kind: ref_kind.to_string(),
    })
}

fn explicit_file_path(tool: &ToolInvocation) -> Option<PathBuf> {
    tool.input
        .as_ref()
        .and_then(|input| input.get("file_path"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
}

fn explicit_skill_references_from_tool(tool: &ToolInvocation) -> Vec<String> {
    let mut skills = HashSet::new();

    if let Some(path) = explicit_file_path(tool)
        && let Some(skill_name) = skill_name_from_explicit_path(&path)
    {
        skills.insert(skill_name);
    }

    if tool.tool_name == "Bash" {
        skills.extend(explicit_skill_references_from_shell_command(tool));
    }

    let mut skills: Vec<String> = skills.into_iter().collect();
    skills.sort();
    skills
}

fn explicit_skill_references_from_shell_command(tool: &ToolInvocation) -> Vec<String> {
    let Some(command) = tool
        .input
        .as_ref()
        .and_then(|input| input.get("command"))
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    if contains_shell_separator(command) {
        return Vec::new();
    }

    let mut skills = HashSet::new();
    for token in shell_words(command) {
        if token.starts_with('-') || !token.contains('/') {
            continue;
        }
        if let Some(skill_name) = skill_name_from_explicit_path(Path::new(&token)) {
            skills.insert(skill_name);
        }
    }

    let mut skills: Vec<String> = skills.into_iter().collect();
    skills.sort();
    skills
}

fn skill_name_from_explicit_path(path: &Path) -> Option<String> {
    if path.file_name().and_then(|name| name.to_str()) == Some("SKILL.md") {
        return path
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty() && !name.starts_with('.'))
            .map(ToOwned::to_owned);
    }

    let components = path
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let skills_index = components
        .iter()
        .position(|component| component == "skills")?;

    components
        .iter()
        .skip(skills_index + 1)
        .find(|component| !component.is_empty() && !component.starts_with('.'))
        .cloned()
}

fn contains_shell_separator(command: &str) -> bool {
    ["&&", "||", ";", "|", "\n"]
        .iter()
        .any(|separator| command.contains(separator))
}

fn shell_words(command: &str) -> Vec<String> {
    let mut words = Vec::new();

    for token in command.split_whitespace() {
        let token = token.trim_matches(|character| character == '"' || character == '\'');
        if token.is_empty() {
            continue;
        }
        if words.is_empty()
            && token.contains('=')
            && !token.starts_with("./")
            && !token.starts_with('/')
        {
            continue;
        }
        words.push(token.to_string());
    }

    words
}

fn is_documentation_path(path: &Path) -> bool {
    let extension = path.extension().and_then(|extension| extension.to_str());
    if matches!(extension, Some("md" | "mdx" | "rst" | "txt" | "adoc")) {
        return true;
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if file_name.starts_with("README") || file_name.starts_with("CHANGELOG") {
        return true;
    }

    path.components().any(|component| {
        let component = component.as_os_str().to_string_lossy();
        component == "docs" || component == "doc"
    })
}

fn tool_use_input_from_metadata_json(raw_json: &str) -> Option<Value> {
    NormalizedToolUsePartMetadata::parse(raw_json).map(|metadata| metadata.input)
}

fn persist_path_refs(
    conn: &Connection,
    project_id: i64,
    project_root: &Path,
    message_id: i64,
    path_refs: &[PathRefSpec],
) -> Result<usize> {
    let mut inserted = 0usize;

    for (ordinal, path_ref) in path_refs.iter().enumerate() {
        if !path_ref.full_path.starts_with(project_root) {
            continue;
        }

        let Some(path_node_id) =
            ensure_path_node_chain(conn, project_id, project_root, &path_ref.full_path)?
        else {
            continue;
        };

        conn.execute(
            "
            INSERT INTO message_path_ref (
                message_id,
                message_part_id,
                path_node_id,
                ref_kind,
                ordinal
            )
            VALUES (?1, ?2, ?3, ?4, ?5)
            ",
            params![
                message_id,
                path_ref.message_part_id,
                path_node_id,
                path_ref.ref_kind,
                ordinal as i64,
            ],
        )
        .context("unable to insert message path reference")?;
        inserted += 1;
    }

    Ok(inserted)
}

fn ensure_path_node_chain(
    conn: &Connection,
    project_id: i64,
    project_root: &Path,
    full_path: &Path,
) -> Result<Option<i64>> {
    if full_path == project_root {
        return Ok(None);
    }

    let root_full_path = project_root.to_string_lossy().to_string();
    let root_name = project_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(root_full_path.as_str());
    let root_id = ensure_path_node(
        conn,
        project_id,
        None,
        root_name,
        &root_full_path,
        "root",
        0,
    )?;

    let relative_path = full_path
        .strip_prefix(project_root)
        .ok()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    let mut current_full_path = project_root.to_path_buf();
    let mut parent_id = root_id;
    let components: Vec<_> = relative_path.components().collect();

    if components.is_empty() {
        return Ok(None);
    }

    for (depth, component) in components.iter().enumerate() {
        current_full_path.push(component.as_os_str());
        let is_last = depth + 1 == components.len();
        let node_kind = if is_last { "file" } else { "dir" };
        let name = component.as_os_str().to_string_lossy().to_string();
        parent_id = ensure_path_node(
            conn,
            project_id,
            Some(parent_id),
            &name,
            &current_full_path.to_string_lossy(),
            node_kind,
            (depth + 1) as i64,
        )?;
    }

    Ok(Some(parent_id))
}

fn ensure_path_node(
    conn: &Connection,
    project_id: i64,
    parent_id: Option<i64>,
    name: &str,
    full_path: &str,
    node_kind: &str,
    depth: i64,
) -> Result<i64> {
    if let Some(existing_id) = conn
        .query_row(
            "
            SELECT id
            FROM path_node
            WHERE project_id = ?1 AND full_path = ?2
            ",
            params![project_id, full_path],
            |row| row.get(0),
        )
        .optional()
        .context("unable to look up existing path node")?
    {
        return Ok(existing_id);
    }

    conn.query_row(
        "
        INSERT INTO path_node (
            project_id,
            parent_id,
            name,
            full_path,
            node_kind,
            depth
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        RETURNING id
        ",
        params![project_id, parent_id, name, full_path, node_kind, depth],
        |row| row.get(0),
    )
    .context("unable to insert path node")
}

#[derive(Debug)]
struct ActionGroupDraft {
    turn_id: i64,
    descriptor: ActionDescriptor,
    started_at_utc: Option<String>,
    ended_at_utc: Option<String>,
    usage: Usage,
    message_ids: Vec<i64>,
    skill_references: HashSet<String>,
}

impl ActionGroupDraft {
    fn new(turn_id: i64, message: &LoadedMessage, classification: &MessageClassification) -> Self {
        Self {
            turn_id,
            descriptor: classification.descriptor.clone(),
            started_at_utc: message
                .created_at_utc
                .clone()
                .or_else(|| message.completed_at_utc.clone()),
            ended_at_utc: message
                .completed_at_utc
                .clone()
                .or_else(|| message.created_at_utc.clone()),
            usage: message.usage.clone(),
            message_ids: vec![message.id],
            skill_references: classification.skill_references.iter().cloned().collect(),
        }
    }

    fn push_message(&mut self, message: &LoadedMessage, classification: &MessageClassification) {
        if let Some(candidate) = message
            .created_at_utc
            .clone()
            .or_else(|| message.completed_at_utc.clone())
        {
            if self
                .started_at_utc
                .as_ref()
                .is_none_or(|current| candidate < *current)
            {
                self.started_at_utc = Some(candidate.clone());
            }
            if self
                .ended_at_utc
                .as_ref()
                .is_none_or(|current| candidate > *current)
            {
                self.ended_at_utc = Some(candidate);
            }
        }

        if let Some(candidate) = message.completed_at_utc.clone()
            && self
                .ended_at_utc
                .as_ref()
                .is_none_or(|current| candidate > *current)
        {
            self.ended_at_utc = Some(candidate);
        }

        add_usage(&mut self.usage.input_tokens, message.usage.input_tokens);
        add_usage(
            &mut self.usage.cache_creation_input_tokens,
            message.usage.cache_creation_input_tokens,
        );
        add_usage(
            &mut self.usage.cache_read_input_tokens,
            message.usage.cache_read_input_tokens,
        );
        add_usage(&mut self.usage.output_tokens, message.usage.output_tokens);
        self.message_ids.push(message.id);
        self.skill_references
            .extend(classification.skill_references.iter().cloned());
    }

    fn skill_attribution(&self) -> Option<ActionSkillAttributionDraft> {
        let skill_names = self
            .skill_references
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let [skill_name] = skill_names.as_slice() else {
            return None;
        };

        Some(ActionSkillAttributionDraft {
            skill_name: (*skill_name).to_string(),
            confidence: "high",
        })
    }
}

fn persist_action(
    conn: &Connection,
    import_chunk_id: i64,
    turn_id: i64,
    sequence_no: i64,
    group: ActionGroupDraft,
) -> Result<()> {
    let action_id: i64 = conn.query_row(
        "
        INSERT INTO action (
            turn_id,
            import_chunk_id,
            sequence_no,
            category,
            normalized_action,
            command_family,
            base_command,
            classification_state,
            classifier,
            started_at_utc,
            ended_at_utc,
            input_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            output_tokens,
            message_count
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'deterministic_v1', ?9, ?10, ?11, ?12, ?13, ?14, ?15)
        RETURNING id
        ",
        params![
            turn_id,
            import_chunk_id,
            sequence_no,
            group.descriptor.category,
            group.descriptor.normalized_action,
            group.descriptor.command_family,
            group.descriptor.base_command,
            group.descriptor.classification_state,
            group.started_at_utc,
            group.ended_at_utc,
            group.usage.input_tokens,
            group.usage.cache_creation_input_tokens,
            group.usage.cache_read_input_tokens,
            group.usage.output_tokens,
            group.message_ids.len() as i64,
        ],
        |row| row.get(0),
    )?;

    for (ordinal, message_id) in group.message_ids.iter().enumerate() {
        conn.execute(
            "
            INSERT INTO action_message (action_id, message_id, ordinal_in_action)
            VALUES (?1, ?2, ?3)
            ",
            params![action_id, message_id, ordinal as i64],
        )?;
    }

    if let Some(attribution) = group.skill_attribution() {
        conn.execute(
            "
            INSERT INTO action_skill_attribution (action_id, skill_name, confidence)
            VALUES (?1, ?2, ?3)
            ",
            params![action_id, attribution.skill_name, attribution.confidence],
        )?;
    }

    Ok(())
}

fn add_usage(total: &mut Option<i64>, candidate: Option<i64>) {
    if let Some(candidate) = candidate {
        *total = Some(total.unwrap_or(0) + candidate);
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use rusqlite::{Connection, params};
    use tempfile::tempdir;

    use crate::db::Database;
    use crate::import::{
        NormalizeJsonlFileOutcome, NormalizeJsonlFileParams, normalize_jsonl_file,
    };

    use super::{BuildActionsParams, build_actions};

    const CLASSIFICATION_FIXTURE: &str = concat!(
        "{\"type\":\"user\",\"uuid\":\"prompt-1\",\"timestamp\":\"2026-03-26T12:00:00Z\",\"sessionId\":\"session-3\",\"cwd\":\"/tmp/project\",\"message\":{\"role\":\"user\",\"content\":\"Investigate the parser failure.\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"read-1\",\"timestamp\":\"2026-03-26T12:00:01Z\",\"sessionId\":\"session-3\",\"message\":{\"id\":\"msg-read\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-read\",\"name\":\"Read\",\"input\":{\"file_path\":\"/tmp/project/src/parser.rs\"}}],\"usage\":{\"input_tokens\":3,\"cache_creation_input_tokens\":4,\"cache_read_input_tokens\":0,\"output_tokens\":1},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}\n",
        "{\"type\":\"user\",\"uuid\":\"read-result-1\",\"timestamp\":\"2026-03-26T12:00:02Z\",\"sessionId\":\"session-3\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-read\",\"content\":\"fn parse() {}\",\"is_error\":false}]},\"toolUseResult\":{\"stdout\":\"fn parse() {}\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"edit-1\",\"timestamp\":\"2026-03-26T12:00:03Z\",\"sessionId\":\"session-3\",\"message\":{\"id\":\"msg-edit\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-edit\",\"name\":\"Edit\",\"input\":{\"file_path\":\"/tmp/project/src/parser.rs\",\"old_string\":\"fn parse() {}\",\"new_string\":\"fn parse(input: &str) {}\"}}],\"usage\":{\"input_tokens\":5,\"cache_creation_input_tokens\":6,\"cache_read_input_tokens\":0,\"output_tokens\":2},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}\n",
        "{\"type\":\"user\",\"uuid\":\"edit-result-1\",\"timestamp\":\"2026-03-26T12:00:04Z\",\"sessionId\":\"session-3\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-edit\",\"content\":\"updated\",\"is_error\":false}]},\"toolUseResult\":{\"type\":\"edit\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"test-1\",\"timestamp\":\"2026-03-26T12:00:05Z\",\"sessionId\":\"session-3\",\"message\":{\"id\":\"msg-test\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-test\",\"name\":\"Bash\",\"input\":{\"command\":\"cargo test\"}}],\"usage\":{\"input_tokens\":7,\"cache_creation_input_tokens\":8,\"cache_read_input_tokens\":9,\"output_tokens\":3},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}\n",
        "{\"type\":\"user\",\"uuid\":\"test-result-1\",\"timestamp\":\"2026-03-26T12:00:06Z\",\"sessionId\":\"session-3\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-test\",\"content\":\"ok\",\"is_error\":false}]},\"toolUseResult\":{\"stdout\":\"ok\"}}\n"
    );

    const MIXED_FIXTURE: &str = concat!(
        "{\"type\":\"user\",\"uuid\":\"prompt-2\",\"timestamp\":\"2026-03-26T13:00:00Z\",\"sessionId\":\"session-4\",\"cwd\":\"/tmp/project\",\"message\":{\"role\":\"user\",\"content\":\"Fix docs and rerun checks.\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"mixed-1\",\"timestamp\":\"2026-03-26T13:00:01Z\",\"sessionId\":\"session-4\",\"message\":{\"id\":\"msg-mixed\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-mixed\",\"name\":\"Bash\",\"input\":{\"command\":\"git status && cargo test\"}}],\"usage\":{\"input_tokens\":2,\"cache_creation_input_tokens\":3,\"cache_read_input_tokens\":0,\"output_tokens\":1},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}\n",
        "{\"type\":\"user\",\"uuid\":\"mixed-result-1\",\"timestamp\":\"2026-03-26T13:00:02Z\",\"sessionId\":\"session-4\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-mixed\",\"content\":\"mixed output\",\"is_error\":false}]},\"toolUseResult\":{\"stdout\":\"mixed output\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"doc-1\",\"timestamp\":\"2026-03-26T13:00:03Z\",\"sessionId\":\"session-4\",\"message\":{\"id\":\"msg-doc\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-doc\",\"name\":\"Write\",\"input\":{\"file_path\":\"/tmp/project/docs/README.md\",\"content\":\"updated docs\"}}],\"usage\":{\"input_tokens\":4,\"cache_creation_input_tokens\":5,\"cache_read_input_tokens\":0,\"output_tokens\":2},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}\n",
        "{\"type\":\"user\",\"uuid\":\"doc-result-1\",\"timestamp\":\"2026-03-26T13:00:04Z\",\"sessionId\":\"session-4\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-doc\",\"content\":\"written\",\"is_error\":false}]},\"toolUseResult\":{\"stdout\":\"written\"}}\n"
    );

    const SKILL_ATTRIBUTION_FIXTURE: &str = concat!(
        "{\"type\":\"user\",\"uuid\":\"prompt-3\",\"timestamp\":\"2026-03-26T14:00:00Z\",\"sessionId\":\"session-5\",\"cwd\":\"/tmp/project\",\"message\":{\"role\":\"user\",\"content\":\"Check the planner skill and rerun tests.\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"skill-read-1\",\"timestamp\":\"2026-03-26T14:00:01Z\",\"sessionId\":\"session-5\",\"message\":{\"id\":\"msg-skill-read\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-skill-read\",\"name\":\"Read\",\"input\":{\"file_path\":\"/home/ketan/.codex/skills/planner/SKILL.md\"}}],\"usage\":{\"input_tokens\":6,\"cache_creation_input_tokens\":1,\"cache_read_input_tokens\":0,\"output_tokens\":2},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}\n",
        "{\"type\":\"user\",\"uuid\":\"skill-read-result-1\",\"timestamp\":\"2026-03-26T14:00:02Z\",\"sessionId\":\"session-5\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-skill-read\",\"content\":\"---\\nname: planner\\n---\",\"is_error\":false}]},\"toolUseResult\":{\"stdout\":\"---\\nname: planner\\n---\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"test-2\",\"timestamp\":\"2026-03-26T14:00:03Z\",\"sessionId\":\"session-5\",\"message\":{\"id\":\"msg-test-2\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-test-2\",\"name\":\"Bash\",\"input\":{\"command\":\"cargo test\"}}],\"usage\":{\"input_tokens\":4,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0,\"output_tokens\":1},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}\n",
        "{\"type\":\"user\",\"uuid\":\"test-result-2\",\"timestamp\":\"2026-03-26T14:00:04Z\",\"sessionId\":\"session-5\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-test-2\",\"content\":\"ok\",\"is_error\":false}]},\"toolUseResult\":{\"stdout\":\"ok\"}}\n"
    );

    #[test]
    fn builds_grouped_actions_from_turn_messages() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("session.jsonl");
        std::fs::write(&fixture_path, CLASSIFICATION_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "session.jsonl")?;
        let normalized = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
                perf_logger: None,
            },
        )?;
        let NormalizeJsonlFileOutcome::Imported(normalized) = normalized else {
            panic!("classification fixture should import");
        };
        let result = build_actions(
            db.connection_mut(),
            &BuildActionsParams {
                conversation_id: normalized
                    .conversation_id
                    .expect("transcript normalization should produce a conversation id"),
                perf_logger: None,
            },
        )?;

        assert_eq!(result.action_count, 4);
        assert_eq!(result.path_ref_count, 2);

        let conn = db.connection();
        let actions: Vec<StoredAction> = query_actions(conn)?;
        assert_eq!(
            actions,
            vec![
                (
                    "user input".to_string(),
                    "prompt".to_string(),
                    None,
                    None,
                    1
                ),
                (
                    "project discovery".to_string(),
                    "file read".to_string(),
                    Some("explicit file tool".to_string()),
                    Some("Read".to_string()),
                    2
                ),
                (
                    "editing".to_string(),
                    "file edit".to_string(),
                    Some("explicit file tool".to_string()),
                    Some("Edit".to_string()),
                    2
                ),
                (
                    "test/build/run".to_string(),
                    "test run".to_string(),
                    Some("cargo test".to_string()),
                    Some("cargo".to_string()),
                    2
                ),
            ]
        );

        let file_refs: i64 = conn.query_row(
            "
            SELECT COUNT(*)
            FROM message_path_ref
            ",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(file_refs, 2);

        Ok(())
    }

    #[test]
    fn keeps_mixed_shell_messages_mixed_and_limits_path_refs_to_file_tools() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("mixed.jsonl");
        std::fs::write(&fixture_path, MIXED_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "mixed.jsonl")?;
        let normalized = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
                perf_logger: None,
            },
        )?;
        let NormalizeJsonlFileOutcome::Imported(normalized) = normalized else {
            panic!("mixed fixture should import");
        };
        let result = build_actions(
            db.connection_mut(),
            &BuildActionsParams {
                conversation_id: normalized
                    .conversation_id
                    .expect("transcript normalization should produce a conversation id"),
                perf_logger: None,
            },
        )?;

        assert_eq!(result.action_count, 3);
        assert_eq!(result.path_ref_count, 1);

        let conn = db.connection();

        let mixed_state: (String, Option<String>) = conn.query_row(
            "
            SELECT classification_state, category
            FROM action
            WHERE sequence_no = 1
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(mixed_state.0, "mixed");
        assert_eq!(mixed_state.1, None);

        let doc_action: (String, String) = conn.query_row(
            "
            SELECT category, normalized_action
            FROM action
            WHERE sequence_no = 2
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        assert_eq!(doc_action.0, "documentation writing");
        assert_eq!(doc_action.1, "document write");

        let doc_path: String = conn.query_row(
            "
            SELECT pn.full_path
            FROM message_path_ref mpr
            JOIN path_node pn ON pn.id = mpr.path_node_id
            WHERE pn.node_kind = 'file'
            ",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(doc_path, "/tmp/project/docs/README.md");

        Ok(())
    }

    #[test]
    fn attributes_explicit_skill_paths_without_tagging_generic_actions() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("skill-attribution.jsonl");
        std::fs::write(&fixture_path, SKILL_ATTRIBUTION_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "skill-attribution.jsonl")?;
        let normalized = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
                perf_logger: None,
            },
        )?;
        let NormalizeJsonlFileOutcome::Imported(normalized) = normalized else {
            panic!("skill attribution fixture should import");
        };
        let result = build_actions(
            db.connection_mut(),
            &BuildActionsParams {
                conversation_id: normalized
                    .conversation_id
                    .expect("transcript normalization should produce a conversation id"),
                perf_logger: None,
            },
        )?;

        assert_eq!(result.action_count, 3);
        assert_eq!(result.path_ref_count, 0);

        let conn = db.connection();
        let rows: Vec<(i64, String, String)> = {
            let mut stmt = conn.prepare(
                "
                SELECT a.sequence_no, asa.skill_name, asa.confidence
                FROM action_skill_attribution asa
                JOIN action a ON a.id = asa.action_id
                ORDER BY a.sequence_no
                ",
            )?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        assert_eq!(rows, vec![(1, "planner".to_string(), "high".to_string())]);

        let generic_action_attribution_count: i64 = conn.query_row(
            "
            SELECT COUNT(*)
            FROM action_skill_attribution asa
            JOIN action a ON a.id = asa.action_id
            WHERE a.normalized_action = 'test run'
            ",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(generic_action_attribution_count, 0);

        Ok(())
    }

    type StoredAction = (String, String, Option<String>, Option<String>, i64);

    fn query_actions(conn: &Connection) -> Result<Vec<StoredAction>> {
        let mut stmt = conn.prepare(
            "
            SELECT
                category,
                normalized_action,
                command_family,
                base_command,
                message_count
            FROM action
            ORDER BY sequence_no
            ",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })?;

        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    struct SeededIds {
        project_id: i64,
        source_file_id: i64,
        import_chunk_id: i64,
    }

    fn seed_import_context(conn: &mut Connection, relative_path: &str) -> Result<SeededIds> {
        let project_id = conn.query_row(
            "
            INSERT INTO project (identity_kind, canonical_key, display_name, root_path)
            VALUES ('path', 'project-key', 'project', '/tmp/project')
            RETURNING id
            ",
            [],
            |row| row.get(0),
        )?;

        let source_file_id = conn.query_row(
            "
            INSERT INTO source_file (project_id, relative_path, size_bytes)
            VALUES (?1, ?2, 0)
            RETURNING id
            ",
            params![project_id, relative_path],
            |row| row.get(0),
        )?;

        let import_chunk_id = conn.query_row(
            "
            INSERT INTO import_chunk (project_id, chunk_day_local, state)
            VALUES (?1, '2026-03-26', 'running')
            RETURNING id
            ",
            [project_id],
            |row| row.get(0),
        )?;

        Ok(SeededIds {
            project_id,
            source_file_id,
            import_chunk_id,
        })
    }
}
