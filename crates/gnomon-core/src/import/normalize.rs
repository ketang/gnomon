use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::Value;

const PRIMARY_STREAM_SEQUENCE_NO: i64 = 0;

#[derive(Debug, Clone)]
pub struct NormalizeJsonlFileParams {
    pub project_id: i64,
    pub source_file_id: i64,
    pub import_chunk_id: i64,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizeJsonlFileResult {
    pub conversation_id: i64,
    pub stream_id: i64,
    pub record_count: usize,
    pub message_count: usize,
    pub turn_count: usize,
}

pub fn normalize_jsonl_file(
    conn: &mut Connection,
    params: &NormalizeJsonlFileParams,
) -> Result<NormalizeJsonlFileResult> {
    let mut tx = conn
        .transaction()
        .context("unable to start normalization transaction")?;
    purge_existing_import(&mut tx, params)?;

    let file = File::open(&params.path)
        .with_context(|| format!("unable to open jsonl source {}", params.path.display()))?;
    let reader = BufReader::new(file);

    let mut state = ImportState::new(params.clone());

    for (zero_based_line_no, line_result) in reader.lines().enumerate() {
        let line_no = zero_based_line_no + 1;
        let line = line_result.with_context(|| {
            format!(
                "unable to read line {line_no} from {}",
                params.path.display()
            )
        })?;

        let record: Value = serde_json::from_str(&line).with_context(|| {
            format!(
                "unable to parse json on line {line_no} from {}",
                params.path.display()
            )
        })?;

        if state.conversation.is_none() {
            if extract_session_id(&record).is_some() {
                state.initialize_context(&mut tx, &record)?;
                state.flush_buffered_records(&mut tx)?;
            } else {
                state.buffered_records.push(BufferedRecord {
                    source_line_no: line_no as i64,
                    value: record,
                });
                continue;
            }
        }

        state.process_record(&mut tx, record, line_no as i64)?;
    }

    if state.conversation.is_none() {
        bail!(
            "no sessionId found in {}; unable to normalize file",
            params.path.display()
        );
    }

    if !state.buffered_records.is_empty() {
        state.flush_buffered_records(&mut tx)?;
    }

    let turn_count = state.build_turns(&mut tx)?;
    state.finish_import(&mut tx, turn_count)?;
    tx.commit().context("unable to commit normalized import")?;

    Ok(NormalizeJsonlFileResult {
        conversation_id: state
            .conversation
            .as_ref()
            .map(|conversation| conversation.id)
            .ok_or_else(|| anyhow!("conversation missing after import"))?,
        stream_id: state
            .stream
            .as_ref()
            .map(|stream| stream.id)
            .ok_or_else(|| anyhow!("stream missing after import"))?,
        record_count: state.record_count,
        message_count: state.message_states.len(),
        turn_count,
    })
}

fn purge_existing_import(
    tx: &mut Transaction<'_>,
    params: &NormalizeJsonlFileParams,
) -> Result<()> {
    let existing_conversation_id: Option<i64> = tx
        .query_row(
            "SELECT id FROM conversation WHERE source_file_id = ?1",
            [params.source_file_id],
            |row| row.get(0),
        )
        .optional()
        .context("unable to look up existing conversation for source file")?;

    if let Some(conversation_id) = existing_conversation_id {
        tx.execute("DELETE FROM conversation WHERE id = ?1", [conversation_id])
            .context("unable to purge existing normalized conversation state")?;
    }

    tx.execute(
        "
        DELETE FROM import_warning
        WHERE import_chunk_id = ?1 AND source_file_id = ?2
        ",
        params![params.import_chunk_id, params.source_file_id],
    )
    .context("unable to clear prior import warnings for source file")?;

    Ok(())
}

#[derive(Debug, Clone)]
struct ConversationState {
    id: i64,
    started_at_utc: Option<String>,
    ended_at_utc: Option<String>,
}

#[derive(Debug, Clone)]
struct StreamState {
    id: i64,
    opened_at_utc: Option<String>,
    closed_at_utc: Option<String>,
}

#[derive(Debug, Clone)]
struct BufferedRecord {
    source_line_no: i64,
    value: Value,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Usage {
    input_tokens: Option<i64>,
    cache_creation_input_tokens: Option<i64>,
    cache_read_input_tokens: Option<i64>,
    output_tokens: Option<i64>,
}

impl Usage {
    fn from_value(value: Option<&Value>) -> Self {
        let Some(value) = value else {
            return Self::default();
        };

        Self {
            input_tokens: usage_field(value, "input_tokens"),
            cache_creation_input_tokens: usage_field(value, "cache_creation_input_tokens"),
            cache_read_input_tokens: usage_field(value, "cache_read_input_tokens"),
            output_tokens: usage_field(value, "output_tokens"),
        }
    }

    fn has_any(&self) -> bool {
        self.input_tokens.is_some()
            || self.cache_creation_input_tokens.is_some()
            || self.cache_read_input_tokens.is_some()
            || self.output_tokens.is_some()
    }
}

#[derive(Debug, Clone)]
struct ExtractedMessage {
    external_id: String,
    source_line_no: i64,
    role: String,
    message_kind: &'static str,
    recorded_at_utc: Option<String>,
    model_name: Option<String>,
    stop_reason: Option<String>,
    usage_source: Option<&'static str>,
    usage: Usage,
    parts: Vec<ExtractedMessagePart>,
}

#[derive(Debug, Clone)]
struct ExtractedMessagePart {
    part_kind: String,
    mime_type: Option<String>,
    text_value: Option<String>,
    tool_name: Option<String>,
    tool_call_id: Option<String>,
    metadata_json: Option<String>,
    is_error: bool,
    dedupe_key: String,
}

#[derive(Debug, Clone)]
struct MessageState {
    id: i64,
    recorded_at_utc: Option<String>,
    usage: Usage,
    next_part_ordinal: i64,
    seen_part_keys: HashSet<String>,
}

#[derive(Debug)]
struct ImportState {
    params: NormalizeJsonlFileParams,
    conversation: Option<ConversationState>,
    stream: Option<StreamState>,
    buffered_records: Vec<BufferedRecord>,
    record_count: usize,
    next_record_sequence_no: i64,
    next_message_sequence_no: i64,
    message_states: HashMap<String, MessageState>,
}

impl ImportState {
    fn new(params: NormalizeJsonlFileParams) -> Self {
        Self {
            params,
            conversation: None,
            stream: None,
            buffered_records: Vec::new(),
            record_count: 0,
            next_record_sequence_no: 0,
            next_message_sequence_no: 0,
            message_states: HashMap::new(),
        }
    }

    fn initialize_context(&mut self, tx: &mut Transaction<'_>, record: &Value) -> Result<()> {
        let session_id = extract_session_id(record)
            .ok_or_else(|| anyhow!("cannot initialize import context without sessionId"))?;
        let conversation_external_id =
            conversation_external_id(session_id, self.params.source_file_id);
        let timestamp = extract_record_timestamp(record).map(ToOwned::to_owned);

        let conversation_id = tx
            .query_row(
                "
                INSERT INTO conversation (
                    project_id,
                    source_file_id,
                    external_id,
                    started_at_utc,
                    ended_at_utc
                )
                VALUES (?1, ?2, ?3, ?4, ?4)
                RETURNING id
                ",
                params![
                    self.params.project_id,
                    self.params.source_file_id,
                    conversation_external_id,
                    timestamp
                ],
                |row| row.get(0),
            )
            .context("unable to insert conversation for normalized file")?;

        let stream_kind = if record
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            "sidechain"
        } else {
            "primary"
        };
        let stream_external_id = record.get("agentId").and_then(Value::as_str);

        let stream_id = tx
            .query_row(
                "
                INSERT INTO stream (
                    conversation_id,
                    import_chunk_id,
                    external_id,
                    stream_kind,
                    sequence_no,
                    opened_at_utc,
                    closed_at_utc
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
                RETURNING id
                ",
                params![
                    conversation_id,
                    self.params.import_chunk_id,
                    stream_external_id,
                    stream_kind,
                    PRIMARY_STREAM_SEQUENCE_NO,
                    timestamp
                ],
                |row| row.get(0),
            )
            .context("unable to insert primary stream for normalized file")?;

        self.conversation = Some(ConversationState {
            id: conversation_id,
            started_at_utc: timestamp.clone(),
            ended_at_utc: timestamp.clone(),
        });
        self.stream = Some(StreamState {
            id: stream_id,
            opened_at_utc: timestamp.clone(),
            closed_at_utc: timestamp,
        });

        Ok(())
    }

    fn flush_buffered_records(&mut self, tx: &mut Transaction<'_>) -> Result<()> {
        let buffered_records = std::mem::take(&mut self.buffered_records);
        for record in buffered_records {
            self.process_record(tx, record.value, record.source_line_no)?;
        }
        Ok(())
    }

    fn process_record(
        &mut self,
        tx: &mut Transaction<'_>,
        record: Value,
        source_line_no: i64,
    ) -> Result<()> {
        let conversation = self
            .conversation
            .as_mut()
            .ok_or_else(|| anyhow!("conversation state missing while processing record"))?;
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| anyhow!("stream state missing while processing record"))?;

        let recorded_at_utc = extract_record_timestamp(&record).map(ToOwned::to_owned);
        update_bounds(
            &mut conversation.started_at_utc,
            &mut conversation.ended_at_utc,
            &recorded_at_utc,
        );
        update_bounds(
            &mut stream.opened_at_utc,
            &mut stream.closed_at_utc,
            &recorded_at_utc,
        );

        let record_kind = classify_record_kind(&record);
        tx.execute(
            "
            INSERT INTO record (
                import_chunk_id,
                source_file_id,
                conversation_id,
                stream_id,
                source_line_no,
                sequence_no,
                record_kind,
                recorded_at_utc
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ",
            params![
                self.params.import_chunk_id,
                self.params.source_file_id,
                conversation.id,
                stream.id,
                source_line_no,
                self.next_record_sequence_no,
                record_kind,
                recorded_at_utc,
            ],
        )
        .with_context(|| format!("unable to insert normalized record for line {source_line_no}"))?;
        self.next_record_sequence_no += 1;
        self.record_count += 1;

        if let Some(message) = extract_message(&record, source_line_no) {
            self.upsert_message(tx, message)?;
        }

        Ok(())
    }

    fn upsert_message(
        &mut self,
        tx: &mut Transaction<'_>,
        extracted: ExtractedMessage,
    ) -> Result<()> {
        let conversation_id = self
            .conversation
            .as_ref()
            .map(|conversation| conversation.id)
            .ok_or_else(|| anyhow!("conversation state missing while inserting message"))?;
        let stream_id = self
            .stream
            .as_ref()
            .map(|stream| stream.id)
            .ok_or_else(|| anyhow!("stream state missing while inserting message"))?;

        if let Some(state) = self.message_states.get_mut(&extracted.external_id) {
            tx.execute(
                "
                UPDATE message
                SET
                    completed_at_utc = COALESCE(?2, completed_at_utc),
                    model_name = COALESCE(?3, model_name),
                    stop_reason = COALESCE(?4, stop_reason),
                    usage_source = COALESCE(?5, usage_source),
                    input_tokens = ?6,
                    cache_creation_input_tokens = ?7,
                    cache_read_input_tokens = ?8,
                    output_tokens = ?9
                WHERE id = ?1
                ",
                params![
                    state.id,
                    extracted.recorded_at_utc,
                    extracted.model_name,
                    extracted.stop_reason,
                    extracted.usage_source,
                    extracted.usage.input_tokens,
                    extracted.usage.cache_creation_input_tokens,
                    extracted.usage.cache_read_input_tokens,
                    extracted.usage.output_tokens,
                ],
            )
            .with_context(|| {
                format!(
                    "unable to update normalized message on line {} from {}",
                    extracted.source_line_no,
                    self.params.path.display()
                )
            })?;

            state.recorded_at_utc = extracted
                .recorded_at_utc
                .clone()
                .or_else(|| state.recorded_at_utc.clone());
            if extracted.usage.has_any() {
                state.usage = extracted.usage.clone();
            }

            for part in extracted.parts {
                if state.seen_part_keys.insert(part.dedupe_key.clone()) {
                    insert_message_part(
                        tx,
                        state.id,
                        state.next_part_ordinal,
                        &part,
                        extracted.source_line_no,
                        &self.params.path,
                    )?;
                    state.next_part_ordinal += 1;
                }
            }

            return Ok(());
        }

        let message_id = tx
            .query_row(
                "
                INSERT INTO message (
                    stream_id,
                    conversation_id,
                    import_chunk_id,
                    external_id,
                    role,
                    message_kind,
                    sequence_no,
                    created_at_utc,
                    completed_at_utc,
                    input_tokens,
                    cache_creation_input_tokens,
                    cache_read_input_tokens,
                    output_tokens,
                    model_name,
                    stop_reason,
                    usage_source
                )
                VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15
                )
                RETURNING id
                ",
                params![
                    stream_id,
                    conversation_id,
                    self.params.import_chunk_id,
                    extracted.external_id,
                    extracted.role,
                    extracted.message_kind,
                    self.next_message_sequence_no,
                    extracted.recorded_at_utc,
                    extracted.usage.input_tokens,
                    extracted.usage.cache_creation_input_tokens,
                    extracted.usage.cache_read_input_tokens,
                    extracted.usage.output_tokens,
                    extracted.model_name,
                    extracted.stop_reason,
                    extracted.usage_source,
                ],
                |row| row.get(0),
            )
            .with_context(|| {
                format!(
                    "unable to insert normalized message on line {} from {}",
                    extracted.source_line_no,
                    self.params.path.display()
                )
            })?;

        let mut state = MessageState {
            id: message_id,
            recorded_at_utc: extracted.recorded_at_utc.clone(),
            usage: extracted.usage.clone(),
            next_part_ordinal: 0,
            seen_part_keys: HashSet::new(),
        };
        self.next_message_sequence_no += 1;

        for part in extracted.parts {
            if state.seen_part_keys.insert(part.dedupe_key.clone()) {
                insert_message_part(
                    tx,
                    state.id,
                    state.next_part_ordinal,
                    &part,
                    extracted.source_line_no,
                    &self.params.path,
                )?;
                state.next_part_ordinal += 1;
            }
        }

        self.message_states.insert(extracted.external_id, state);
        Ok(())
    }

    fn build_turns(&self, tx: &mut Transaction<'_>) -> Result<usize> {
        let conversation_id = self
            .conversation
            .as_ref()
            .map(|conversation| conversation.id)
            .ok_or_else(|| anyhow!("conversation state missing while building turns"))?;

        let mut stmt = tx.prepare(
            "
            SELECT
                id,
                stream_id,
                message_kind,
                created_at_utc,
                completed_at_utc,
                input_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                output_tokens
            FROM message
            WHERE conversation_id = ?1
            ORDER BY sequence_no, id
            ",
        )?;

        let rows = stmt.query_map([conversation_id], |row| {
            Ok(TurnCandidateMessage {
                id: row.get(0)?,
                stream_id: row.get(1)?,
                message_kind: row.get(2)?,
                created_at_utc: row.get(3)?,
                completed_at_utc: row.get(4)?,
                usage: Usage {
                    input_tokens: row.get(5)?,
                    cache_creation_input_tokens: row.get(6)?,
                    cache_read_input_tokens: row.get(7)?,
                    output_tokens: row.get(8)?,
                },
            })
        })?;
        let messages: Vec<TurnCandidateMessage> = rows.collect::<rusqlite::Result<_>>()?;
        drop(stmt);

        let mut turn_count = 0usize;
        let mut current_turn: Option<TurnDraft> = None;

        for message in messages {
            if message.message_kind == "user_prompt" {
                if let Some(turn) = current_turn.take() {
                    persist_turn(tx, conversation_id, self.params.import_chunk_id, turn)?;
                    turn_count += 1;
                }
                current_turn = Some(TurnDraft::new(message));
                continue;
            }

            if let Some(turn) = current_turn.as_mut() {
                turn.push(message);
            }
        }

        if let Some(turn) = current_turn.take() {
            persist_turn(tx, conversation_id, self.params.import_chunk_id, turn)?;
            turn_count += 1;
        }

        Ok(turn_count)
    }

    fn finish_import(&self, tx: &mut Transaction<'_>, turn_count: usize) -> Result<()> {
        let conversation = self
            .conversation
            .as_ref()
            .ok_or_else(|| anyhow!("conversation state missing while finalizing import"))?;
        let stream = self
            .stream
            .as_ref()
            .ok_or_else(|| anyhow!("stream state missing while finalizing import"))?;

        tx.execute(
            "
            UPDATE conversation
            SET started_at_utc = ?2, ended_at_utc = ?3
            WHERE id = ?1
            ",
            params![
                conversation.id,
                conversation.started_at_utc,
                conversation.ended_at_utc
            ],
        )
        .context("unable to update conversation bounds after normalization")?;

        tx.execute(
            "
            UPDATE stream
            SET opened_at_utc = ?2, closed_at_utc = ?3
            WHERE id = ?1
            ",
            params![stream.id, stream.opened_at_utc, stream.closed_at_utc],
        )
        .context("unable to update stream bounds after normalization")?;

        tx.execute(
            "
            UPDATE import_chunk
            SET
                imported_record_count = imported_record_count + ?2,
                imported_message_count = imported_message_count + ?3,
                imported_conversation_count = imported_conversation_count + 1,
                imported_turn_count = imported_turn_count + ?4
            WHERE id = ?1
            ",
            params![
                self.params.import_chunk_id,
                self.record_count as i64,
                self.message_states.len() as i64,
                turn_count as i64
            ],
        )
        .context("unable to update import chunk counters after normalization")?;

        Ok(())
    }
}

#[derive(Debug)]
struct TurnCandidateMessage {
    id: i64,
    stream_id: i64,
    message_kind: String,
    created_at_utc: Option<String>,
    completed_at_utc: Option<String>,
    usage: Usage,
}

#[derive(Debug)]
struct TurnDraft {
    stream_id: i64,
    root_message_id: i64,
    started_at_utc: Option<String>,
    ended_at_utc: Option<String>,
    usage: Usage,
    message_ids: Vec<i64>,
}

impl TurnDraft {
    fn new(message: TurnCandidateMessage) -> Self {
        let timestamp = message
            .created_at_utc
            .clone()
            .or_else(|| message.completed_at_utc.clone());

        Self {
            stream_id: message.stream_id,
            root_message_id: message.id,
            started_at_utc: timestamp.clone(),
            ended_at_utc: message.completed_at_utc.clone().or(timestamp),
            usage: message.usage,
            message_ids: vec![message.id],
        }
    }

    fn push(&mut self, message: TurnCandidateMessage) {
        let start = message
            .created_at_utc
            .clone()
            .or_else(|| message.completed_at_utc.clone());
        let end = message.completed_at_utc.clone().or(start.clone());

        update_bounds(&mut self.started_at_utc, &mut self.ended_at_utc, &start);
        update_end(&mut self.ended_at_utc, &end);

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
    }
}

fn persist_turn(
    tx: &mut Transaction<'_>,
    conversation_id: i64,
    import_chunk_id: i64,
    turn: TurnDraft,
) -> Result<()> {
    let next_turn_sequence_no: i64 = tx.query_row(
        "
        SELECT COALESCE(MAX(sequence_no), -1) + 1
        FROM turn
        WHERE conversation_id = ?1
        ",
        [conversation_id],
        |row| row.get(0),
    )?;

    let turn_id: i64 = tx.query_row(
        "
        INSERT INTO turn (
            stream_id,
            conversation_id,
            import_chunk_id,
            root_message_id,
            sequence_no,
            started_at_utc,
            ended_at_utc,
            input_tokens,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            output_tokens
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        RETURNING id
        ",
        params![
            turn.stream_id,
            conversation_id,
            import_chunk_id,
            turn.root_message_id,
            next_turn_sequence_no,
            turn.started_at_utc,
            turn.ended_at_utc,
            turn.usage.input_tokens,
            turn.usage.cache_creation_input_tokens,
            turn.usage.cache_read_input_tokens,
            turn.usage.output_tokens,
        ],
        |row| row.get(0),
    )?;

    for (ordinal_in_turn, message_id) in turn.message_ids.iter().enumerate() {
        tx.execute(
            "
            INSERT INTO turn_message (turn_id, message_id, ordinal_in_turn)
            VALUES (?1, ?2, ?3)
            ",
            params![turn_id, message_id, ordinal_in_turn as i64],
        )?;
    }

    Ok(())
}

fn insert_message_part(
    tx: &mut Transaction<'_>,
    message_id: i64,
    ordinal: i64,
    part: &ExtractedMessagePart,
    source_line_no: i64,
    source_path: &Path,
) -> Result<()> {
    tx.execute(
        "
        INSERT INTO message_part (
            message_id,
            ordinal,
            part_kind,
            mime_type,
            text_value,
            tool_name,
            tool_call_id,
            metadata_json,
            is_error
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ",
        params![
            message_id,
            ordinal,
            part.part_kind,
            part.mime_type,
            part.text_value,
            part.tool_name,
            part.tool_call_id,
            part.metadata_json,
            part.is_error,
        ],
    )
    .with_context(|| {
        format!(
            "unable to insert normalized message part on line {} from {}",
            source_line_no,
            source_path.display()
        )
    })?;

    Ok(())
}

fn extract_message(record: &Value, source_line_no: i64) -> Option<ExtractedMessage> {
    match record.get("type").and_then(Value::as_str) {
        Some("assistant") => extract_top_level_assistant(record, source_line_no),
        Some("user") => extract_top_level_user(record, source_line_no),
        Some("progress")
            if record.pointer("/data/type").and_then(Value::as_str) == Some("agent_progress") =>
        {
            extract_relay_message(record, source_line_no)
        }
        _ => None,
    }
}

fn extract_top_level_assistant(record: &Value, source_line_no: i64) -> Option<ExtractedMessage> {
    let wrapper = record.get("message")?;
    let external_id = message_external_id(wrapper, record, source_line_no)?;
    let parts = extract_message_parts(wrapper.get("content"), source_line_no).ok()?;

    Some(ExtractedMessage {
        external_id,
        source_line_no,
        role: message_role(wrapper)?,
        message_kind: "assistant_message",
        recorded_at_utc: extract_record_timestamp(record).map(ToOwned::to_owned),
        model_name: optional_string(wrapper.get("model")),
        stop_reason: optional_string(wrapper.get("stop_reason")),
        usage_source: if wrapper.get("usage").is_some() {
            Some("message_usage")
        } else {
            None
        },
        usage: Usage::from_value(wrapper.get("usage")),
        parts,
    })
}

fn extract_top_level_user(record: &Value, source_line_no: i64) -> Option<ExtractedMessage> {
    let wrapper = record.get("message")?;
    let external_id = message_external_id(wrapper, record, source_line_no)?;
    let role = message_role(wrapper)?;
    let is_agent_run_summary = record.pointer("/toolUseResult/usage").is_some();
    let message_kind = if is_agent_run_summary {
        "agent_run_summary"
    } else if content_is_tool_result_only(wrapper.get("content")) {
        "user_tool_result"
    } else {
        "user_prompt"
    };
    let parts = extract_message_parts(wrapper.get("content"), source_line_no).ok()?;

    Some(ExtractedMessage {
        external_id,
        source_line_no,
        role,
        message_kind,
        recorded_at_utc: extract_record_timestamp(record).map(ToOwned::to_owned),
        model_name: None,
        stop_reason: None,
        usage_source: if is_agent_run_summary {
            Some("tool_use_result_usage")
        } else {
            None
        },
        usage: if is_agent_run_summary {
            Usage::from_value(record.pointer("/toolUseResult/usage"))
        } else {
            Usage::default()
        },
        parts,
    })
}

fn extract_relay_message(record: &Value, source_line_no: i64) -> Option<ExtractedMessage> {
    let relay_wrapper = record.pointer("/data/message")?;
    let message_wrapper = relay_wrapper.get("message")?;
    let role = message_role(message_wrapper)?;
    let external_id = message_external_id(message_wrapper, relay_wrapper, source_line_no)?;
    let message_kind = match role.as_str() {
        "assistant" => "relay_assistant_message",
        "user" if content_is_tool_result_only(message_wrapper.get("content")) => {
            "relay_user_tool_result"
        }
        "user" => "relay_user_prompt",
        _ => return None,
    };

    let usage_source = if role == "assistant" && message_wrapper.get("usage").is_some() {
        Some("relay_usage")
    } else {
        None
    };
    let usage = if role == "assistant" {
        Usage::from_value(message_wrapper.get("usage"))
    } else {
        Usage::default()
    };
    let parts = extract_message_parts(message_wrapper.get("content"), source_line_no).ok()?;

    Some(ExtractedMessage {
        external_id,
        source_line_no,
        role,
        message_kind,
        recorded_at_utc: relay_wrapper
            .get("timestamp")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| extract_record_timestamp(record).map(ToOwned::to_owned)),
        model_name: optional_string(message_wrapper.get("model")),
        stop_reason: optional_string(message_wrapper.get("stop_reason")),
        usage_source,
        usage,
        parts,
    })
}

fn extract_message_parts(
    content: Option<&Value>,
    source_line_no: i64,
) -> Result<Vec<ExtractedMessagePart>> {
    let Some(content) = content else {
        return Ok(Vec::new());
    };

    match content {
        Value::String(text) => Ok(vec![ExtractedMessagePart {
            part_kind: "text".to_string(),
            mime_type: None,
            text_value: Some(text.clone()),
            tool_name: None,
            tool_call_id: None,
            metadata_json: None,
            is_error: false,
            dedupe_key: format!("text:{text}"),
        }]),
        Value::Array(parts) => {
            let mut extracted_parts = Vec::with_capacity(parts.len());
            for part in parts {
                let part_type = part
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                let dedupe_key = serde_json::to_string(part).with_context(|| {
                    format!("unable to serialize message part on source line {source_line_no}")
                })?;

                let text_value = match part_type.as_str() {
                    "text" | "thinking" => optional_string(part.get("text")),
                    "tool_result" => extract_tool_result_text(part),
                    _ => None,
                };

                let tool_name = optional_string(part.get("name"));
                let tool_call_id = optional_string(part.get("id"))
                    .or_else(|| optional_string(part.get("tool_use_id")));
                let mime_type = optional_string(part.get("mime_type"));
                let metadata_json = if part_type == "text" || part_type == "thinking" {
                    None
                } else {
                    Some(dedupe_key.clone())
                };
                let is_error = part
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);

                extracted_parts.push(ExtractedMessagePart {
                    part_kind: part_type,
                    mime_type,
                    text_value,
                    tool_name,
                    tool_call_id,
                    metadata_json,
                    is_error,
                    dedupe_key,
                });
            }

            Ok(extracted_parts)
        }
        _ => Ok(Vec::new()),
    }
}

fn extract_tool_result_text(part: &Value) -> Option<String> {
    match part.get("content") {
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Array(values)) => {
            let texts: Vec<String> = values
                .iter()
                .filter_map(|value| {
                    value
                        .get("text")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .collect();
            if texts.is_empty() {
                None
            } else {
                Some(texts.join("\n"))
            }
        }
        _ => None,
    }
}

fn classify_record_kind(record: &Value) -> &'static str {
    match record.get("type").and_then(Value::as_str) {
        Some("file-history-snapshot") => "file_history_snapshot",
        Some("progress") => match record.pointer("/data/type").and_then(Value::as_str) {
            Some("hook_progress") => "hook_progress",
            Some("agent_progress") => "agent_progress_relay",
            _ => "other",
        },
        Some("assistant") => "assistant_message",
        Some("user") => {
            if record.pointer("/toolUseResult/usage").is_some() {
                "agent_run_summary"
            } else if record
                .get("message")
                .and_then(|message| message.get("role"))
                .and_then(Value::as_str)
                == Some("user")
                && content_is_tool_result_only(record.pointer("/message/content"))
            {
                "user_tool_result"
            } else {
                "user_prompt"
            }
        }
        _ => "other",
    }
}

fn content_is_tool_result_only(content: Option<&Value>) -> bool {
    let Some(Value::Array(values)) = content else {
        return false;
    };

    !values.is_empty()
        && values
            .iter()
            .all(|value| value.get("type").and_then(Value::as_str) == Some("tool_result"))
}

fn message_external_id(
    message_wrapper: &Value,
    context: &Value,
    source_line_no: i64,
) -> Option<String> {
    optional_string(message_wrapper.get("id"))
        .or_else(|| optional_string(context.get("uuid")))
        .or_else(|| optional_string(context.get("messageId")))
        .or_else(|| optional_string(context.get("toolUseID")).map(|id| format!("tool-use:{id}")))
        .or_else(|| Some(format!("line:{source_line_no}")))
}

fn message_role(message_wrapper: &Value) -> Option<String> {
    optional_string(message_wrapper.get("role"))
}

fn extract_session_id(record: &Value) -> Option<&str> {
    record.get("sessionId").and_then(Value::as_str)
}

fn conversation_external_id(session_id: &str, source_file_id: i64) -> String {
    format!("source-file:{source_file_id}:session:{session_id}")
}

fn extract_record_timestamp(record: &Value) -> Option<&str> {
    record.get("timestamp").and_then(Value::as_str).or_else(|| {
        record
            .pointer("/snapshot/timestamp")
            .and_then(Value::as_str)
    })
}

fn optional_string(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(ToOwned::to_owned)
}

fn usage_field(value: &Value, field_name: &str) -> Option<i64> {
    value.get(field_name).and_then(Value::as_i64)
}

fn update_bounds(start: &mut Option<String>, end: &mut Option<String>, candidate: &Option<String>) {
    if let Some(candidate) = candidate {
        if start.as_ref().is_none_or(|current| candidate < current) {
            *start = Some(candidate.clone());
        }
        if end.as_ref().is_none_or(|current| candidate > current) {
            *end = Some(candidate.clone());
        }
    }
}

fn update_end(end: &mut Option<String>, candidate: &Option<String>) {
    if let Some(candidate) = candidate
        && end.as_ref().is_none_or(|current| candidate > current)
    {
        *end = Some(candidate.clone());
    }
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

    use super::{NormalizeJsonlFileParams, normalize_jsonl_file};

    const MAIN_SESSION_FIXTURE: &str = concat!(
        "{\"type\":\"file-history-snapshot\",\"messageId\":\"snap-1\",\"snapshot\":{\"messageId\":\"snap-1\",\"trackedFileBackups\":{},\"timestamp\":\"2026-03-26T10:00:00Z\"},\"isSnapshotUpdate\":false}\n",
        "{\"type\":\"user\",\"uuid\":\"user-1\",\"timestamp\":\"2026-03-26T10:00:01Z\",\"sessionId\":\"session-1\",\"cwd\":\"/tmp/project\",\"message\":{\"role\":\"user\",\"content\":\"Investigate the failing test.\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"assistant-1a\",\"timestamp\":\"2026-03-26T10:00:02Z\",\"sessionId\":\"session-1\",\"message\":{\"id\":\"msg-a\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"I will inspect the files first.\"}],\"usage\":{\"input_tokens\":3,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":0,\"output_tokens\":2},\"model\":\"claude-opus\",\"stop_reason\":null}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"assistant-1b\",\"timestamp\":\"2026-03-26T10:00:03Z\",\"sessionId\":\"session-1\",\"message\":{\"id\":\"msg-a\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-read\",\"name\":\"Read\",\"input\":{\"file_path\":\"/tmp/project/src/lib.rs\"}}],\"usage\":{\"input_tokens\":3,\"cache_creation_input_tokens\":10,\"cache_read_input_tokens\":0,\"output_tokens\":7},\"model\":\"claude-opus\",\"stop_reason\":\"tool_use\"}}\n",
        "{\"type\":\"user\",\"uuid\":\"tool-result-1\",\"timestamp\":\"2026-03-26T10:00:04Z\",\"sessionId\":\"session-1\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-read\",\"content\":\"fn broken() {}\",\"is_error\":false}]},\"toolUseResult\":{\"stdout\":\"fn broken() {}\",\"stderr\":\"\",\"interrupted\":false}}\n",
        "{\"type\":\"progress\",\"uuid\":\"relay-1\",\"timestamp\":\"2026-03-26T10:00:05Z\",\"sessionId\":\"session-1\",\"data\":{\"type\":\"agent_progress\",\"agentId\":\"agent-2\",\"message\":{\"type\":\"assistant\",\"uuid\":\"relay-assistant-1\",\"timestamp\":\"2026-03-26T10:00:05Z\",\"message\":{\"id\":\"relay-msg-1\",\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu-bash\",\"name\":\"Bash\",\"input\":{\"command\":\"cargo test\"}}],\"usage\":{\"input_tokens\":5,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":8,\"output_tokens\":1},\"model\":\"claude-haiku\",\"stop_reason\":\"tool_use\"}}}}\n",
        "{\"type\":\"progress\",\"uuid\":\"relay-2\",\"timestamp\":\"2026-03-26T10:00:06Z\",\"sessionId\":\"session-1\",\"data\":{\"type\":\"agent_progress\",\"agentId\":\"agent-2\",\"message\":{\"type\":\"user\",\"uuid\":\"relay-user-1\",\"timestamp\":\"2026-03-26T10:00:06Z\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-bash\",\"content\":\"ok\",\"is_error\":false}]}}}}\n",
        "{\"type\":\"user\",\"uuid\":\"summary-1\",\"timestamp\":\"2026-03-26T10:00:07Z\",\"sessionId\":\"session-1\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-team\",\"content\":\"agent finished\",\"is_error\":false}]},\"toolUseResult\":{\"status\":\"completed\",\"agentId\":\"agent-2\",\"usage\":{\"input_tokens\":1,\"cache_creation_input_tokens\":2,\"cache_read_input_tokens\":3,\"output_tokens\":4}}}\n"
    );

    const SIDECHAIN_FIXTURE: &str = concat!(
        "{\"type\":\"user\",\"uuid\":\"side-user-1\",\"timestamp\":\"2026-03-26T11:00:00Z\",\"sessionId\":\"session-2\",\"isSidechain\":true,\"agentId\":\"agent-side\",\"cwd\":\"/tmp/project\",\"message\":{\"role\":\"user\",\"content\":\"Fix the bug in parser.rs\"}}\n",
        "{\"type\":\"assistant\",\"uuid\":\"side-assistant-1\",\"timestamp\":\"2026-03-26T11:00:01Z\",\"sessionId\":\"session-2\",\"isSidechain\":true,\"agentId\":\"agent-side\",\"message\":{\"id\":\"msg-side\",\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"Inspecting parser.rs\"}],\"usage\":{\"input_tokens\":4,\"cache_creation_input_tokens\":6,\"cache_read_input_tokens\":7,\"output_tokens\":1},\"model\":\"claude-haiku\",\"stop_reason\":null}}\n"
    );

    #[test]
    fn normalizes_main_session_and_deduplicates_assistant_usage() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("session.jsonl");
        std::fs::write(&fixture_path, MAIN_SESSION_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "session.jsonl")?;
        let result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
            },
        )?;

        assert_eq!(result.record_count, 8);
        assert_eq!(result.message_count, 6);
        assert_eq!(result.turn_count, 1);

        let conn = db.connection();

        let assistant_usage: (Option<i64>, Option<i64>, Option<i64>, Option<i64>, String) = conn.query_row(
            "
            SELECT input_tokens, cache_creation_input_tokens, cache_read_input_tokens, output_tokens, message_kind
            FROM message
            WHERE external_id = 'msg-a'
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )?;
        assert_eq!(assistant_usage.0, Some(3));
        assert_eq!(assistant_usage.1, Some(10));
        assert_eq!(assistant_usage.2, Some(0));
        assert_eq!(assistant_usage.3, Some(7));
        assert_eq!(assistant_usage.4, "assistant_message");

        let assistant_part_count: i64 = conn.query_row(
            "
            SELECT COUNT(*)
            FROM message_part
            WHERE message_id = (SELECT id FROM message WHERE external_id = 'msg-a')
            ",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(assistant_part_count, 2);

        let relay_kind: String = conn.query_row(
            "SELECT message_kind FROM message WHERE external_id = 'relay-msg-1'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(relay_kind, "relay_assistant_message");

        let summary_usage: (Option<i64>, Option<i64>, Option<i64>, Option<i64>, String) = conn.query_row(
            "
            SELECT input_tokens, cache_creation_input_tokens, cache_read_input_tokens, output_tokens, usage_source
            FROM message
            WHERE external_id = 'summary-1'
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )?;
        assert_eq!(summary_usage.0, Some(1));
        assert_eq!(summary_usage.1, Some(2));
        assert_eq!(summary_usage.2, Some(3));
        assert_eq!(summary_usage.3, Some(4));
        assert_eq!(summary_usage.4, "tool_use_result_usage");

        let turn_totals: (Option<i64>, Option<i64>, Option<i64>, Option<i64>) = conn.query_row(
            "
            SELECT input_tokens, cache_creation_input_tokens, cache_read_input_tokens, output_tokens
            FROM turn
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(turn_totals.0, Some(9));
        assert_eq!(turn_totals.1, Some(12));
        assert_eq!(turn_totals.2, Some(11));
        assert_eq!(turn_totals.3, Some(12));

        Ok(())
    }

    #[test]
    fn preserves_missing_usage_as_null_and_marks_sidechain_streams() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let fixture_path = temp.path().join("side.jsonl");
        std::fs::write(&fixture_path, SIDECHAIN_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let ids = seed_import_context(db.connection_mut(), "side.jsonl")?;
        let result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: ids.project_id,
                source_file_id: ids.source_file_id,
                import_chunk_id: ids.import_chunk_id,
                path: fixture_path,
            },
        )?;

        assert_eq!(result.record_count, 2);
        assert_eq!(result.message_count, 2);
        assert_eq!(result.turn_count, 1);

        let conn = db.connection();

        let prompt_usage: (Option<i64>, Option<i64>, Option<i64>, Option<i64>) = conn.query_row(
            "
            SELECT input_tokens, cache_creation_input_tokens, cache_read_input_tokens, output_tokens
            FROM message
            WHERE external_id = 'side-user-1'
            ",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        assert_eq!(prompt_usage, (None, None, None, None));

        let stream: (String, Option<String>) =
            conn.query_row("SELECT stream_kind, external_id FROM stream", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
        assert_eq!(stream.0, "sidechain");
        assert_eq!(stream.1.as_deref(), Some("agent-side"));

        Ok(())
    }

    #[test]
    fn insert_message_errors_include_source_file_and_line() -> Result<()> {
        let temp = tempdir()?;
        let fixture_path = temp.path().join("session.jsonl");
        std::fs::write(&fixture_path, MAIN_SESSION_FIXTURE)?;

        let mut conn = Connection::open(temp.path().join("legacy.sqlite3"))?;
        conn.execute_batch(
            "
            CREATE TABLE conversation (
                id INTEGER PRIMARY KEY,
                project_id INTEGER NOT NULL,
                source_file_id INTEGER NOT NULL,
                external_id TEXT,
                title TEXT,
                started_at_utc TEXT,
                ended_at_utc TEXT,
                imported_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE stream (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                import_chunk_id INTEGER NOT NULL,
                external_id TEXT,
                stream_kind TEXT NOT NULL,
                sequence_no INTEGER NOT NULL,
                opened_at_utc TEXT,
                closed_at_utc TEXT
            );

            CREATE TABLE record (
                id INTEGER PRIMARY KEY,
                import_chunk_id INTEGER NOT NULL,
                source_file_id INTEGER NOT NULL,
                conversation_id INTEGER NOT NULL,
                stream_id INTEGER,
                source_line_no INTEGER NOT NULL,
                sequence_no INTEGER NOT NULL,
                record_kind TEXT NOT NULL,
                recorded_at_utc TEXT
            );

            CREATE TABLE message (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER NOT NULL,
                import_chunk_id INTEGER NOT NULL,
                external_id TEXT,
                role TEXT NOT NULL,
                message_kind TEXT NOT NULL,
                sequence_no INTEGER NOT NULL,
                created_at_utc TEXT,
                completed_at_utc TEXT,
                input_tokens INTEGER,
                cache_creation_input_tokens INTEGER,
                cache_read_input_tokens INTEGER,
                output_tokens INTEGER,
                model_name TEXT,
                stop_reason TEXT,
                usage_source TEXT
            );

            CREATE TABLE import_warning (
                id INTEGER PRIMARY KEY,
                import_chunk_id INTEGER NOT NULL,
                source_file_id INTEGER,
                conversation_id INTEGER,
                code TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'warning',
                message TEXT NOT NULL,
                created_at_utc TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            ",
        )?;

        let error = normalize_jsonl_file(
            &mut conn,
            &NormalizeJsonlFileParams {
                project_id: 1,
                source_file_id: 1,
                import_chunk_id: 1,
                path: fixture_path.clone(),
            },
        )
        .expect_err("legacy message schema should fail during insert");

        let rendered = format!("{error:#}");
        assert!(rendered.contains(&fixture_path.display().to_string()));
        assert!(rendered.contains("line 2"));
        assert!(rendered.contains("unable to insert normalized message"));
        assert!(rendered.contains("stream_id"));

        Ok(())
    }

    #[test]
    fn normalization_allows_duplicate_session_ids_across_source_files() -> Result<()> {
        let temp = tempdir()?;
        let db_path = temp.path().join("usage.sqlite3");
        let first_fixture_path = temp.path().join("first.jsonl");
        let second_fixture_path = temp.path().join("second.jsonl");
        std::fs::write(&first_fixture_path, MAIN_SESSION_FIXTURE)?;
        std::fs::write(&second_fixture_path, MAIN_SESSION_FIXTURE)?;

        let mut db = Database::open(&db_path)?;
        let first_ids = seed_import_context(db.connection_mut(), "first.jsonl")?;
        let second_ids = seed_second_source_file(
            db.connection_mut(),
            first_ids.project_id,
            "second.jsonl",
            first_ids.import_chunk_id,
        )?;

        let first_result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: first_ids.project_id,
                source_file_id: first_ids.source_file_id,
                import_chunk_id: first_ids.import_chunk_id,
                path: first_fixture_path,
            },
        )?;
        let second_result = normalize_jsonl_file(
            db.connection_mut(),
            &NormalizeJsonlFileParams {
                project_id: second_ids.project_id,
                source_file_id: second_ids.source_file_id,
                import_chunk_id: second_ids.import_chunk_id,
                path: second_fixture_path,
            },
        )?;

        let conversations: Vec<(i64, i64, String)> = {
            let mut stmt = db.connection().prepare(
                "
                SELECT id, source_file_id, external_id
                FROM conversation
                ORDER BY source_file_id
                ",
            )?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        assert_eq!(first_result.conversation_id, conversations[0].0);
        assert_eq!(second_result.conversation_id, conversations[1].0);
        assert_eq!(conversations.len(), 2);
        assert_eq!(
            conversations[0].2,
            format!("source-file:{}:session:session-1", first_ids.source_file_id)
        );
        assert_eq!(
            conversations[1].2,
            format!(
                "source-file:{}:session:session-1",
                second_ids.source_file_id
            )
        );

        Ok(())
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

    fn seed_second_source_file(
        conn: &mut Connection,
        project_id: i64,
        relative_path: &str,
        import_chunk_id: i64,
    ) -> Result<SeededIds> {
        let source_file_id = conn.query_row(
            "
            INSERT INTO source_file (project_id, relative_path, size_bytes)
            VALUES (?1, ?2, 0)
            RETURNING id
            ",
            params![project_id, relative_path],
            |row| row.get(0),
        )?;

        Ok(SeededIds {
            project_id,
            source_file_id,
            import_chunk_id,
        })
    }
}
