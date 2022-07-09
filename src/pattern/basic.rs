use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufStream},
    net::TcpStream,
    time::Instant,
};

use crate::generator::generate_valid_string;

use super::{ParsePattern, ParsePatternCommand, PatternExecError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasicPattern(pub(crate) Vec<BasicCommand>, pub(crate) Vec<String>);

impl BasicPattern {
    pub(crate) fn new(
        p: &ParsePattern,
        key_len: usize,
        value_len: usize,
        state: &mut BasicState,
    ) -> Self {
        let mut current_set: Option<BasicCommand> = None;
        let content: Vec<BasicCommand> =
            p.0.iter()
                .map(|e| match e {
                    ParsePatternCommand::SET => {
                        let ret = generate_set(key_len, value_len);
                        current_set = Some(ret.clone());
                        ret
                    }
                    ParsePatternCommand::GET => match &current_set {
                        Some(s) => derive_get(s),
                        None => generate_get(key_len),
                    },
                    ParsePatternCommand::DEL => match &current_set {
                        Some(s) => derive_del(s),
                        None => generate_del(key_len),
                    },
                })
                .collect();

        let predictions = content.iter().map(|e| e.predict(state)).collect();

        Self(content, predictions)
    }

    pub(crate) async fn execute(
        &self,
        conn: &mut BufStream<TcpStream>,
    ) -> std::io::Result<(Vec<Result<Duration, PatternExecError>>, Duration)> {
        let mut ret = Vec::with_capacity(self.0.len());
        let start = tokio::time::Instant::now();
        for (idx, b) in self.0.iter().enumerate() {
            let res = b.execute(conn, self.1.get(idx).unwrap().to_string()).await;
            if let Err(PatternExecError::IoError(io)) = res {
                return Err(io);
            }
            ret.push(res);
        }
        let duration = start.elapsed();
        Ok((ret, duration))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum BasicCommand {
    Get { key: String },
    Set { key: String, value: String },
    Del { key: String },
}

impl BasicCommand {
    #[inline(always)]
    async fn execute(
        &self,
        conn: &mut BufStream<TcpStream>,
        expected_response: String,
    ) -> Result<Duration, PatternExecError> {
        match self {
            BasicCommand::Get { ref key } => execute_get(conn, key, expected_response).await,
            BasicCommand::Set { ref key, ref value } => {
                execute_set(conn, key.to_string(), value.to_string(), expected_response).await
            }
            BasicCommand::Del { ref key } => execute_del(conn, key, expected_response).await,
        }
    }

    fn predict(&self, state: &mut BasicState) -> String {
        match self {
            BasicCommand::Get { ref key } => predict_get(state, key),
            BasicCommand::Set { ref key, ref value } => predict_set(state, key, value),
            BasicCommand::Del { ref key } => predict_del(state, key),
        }
    }
}

impl ToString for BasicCommand {
    fn to_string(&self) -> String {
        match self {
            BasicCommand::Get { ref key } => format!("GET {}", key),
            BasicCommand::Set { ref key, ref value } => format!("SET {} {}", key, value),
            BasicCommand::Del { ref key } => format!("DEL {}", key),
        }
    }
}

#[inline(always)]
async fn execute_get(
    conn: &mut BufStream<TcpStream>,
    key: &str,
    expected_response: String,
) -> Result<Duration, PatternExecError> {
    let command_string: String = format!("GET {}\n", key);

    let start = Instant::now();

    conn.write_all(command_string.as_bytes()).await?;
    conn.flush().await?;

    let mut actual_response_buf = String::with_capacity(expected_response.len());
    conn.read_line(&mut actual_response_buf).await?;

    let duration = start.elapsed();

    PatternExecError::validate_response(expected_response, actual_response_buf)?;

    Ok(duration)
}

#[inline(always)]
async fn execute_set(
    conn: &mut BufStream<TcpStream>,
    key: String,
    value: String,
    expected_response: String,
) -> Result<Duration, PatternExecError> {
    let command_string: String = format!("SET {} {}\n", key, value);

    let start = Instant::now();

    conn.write_all(command_string.as_bytes()).await?;
    conn.flush().await?;

    let mut actual_response_buf = String::with_capacity(expected_response.len());
    conn.read_line(&mut actual_response_buf).await?;

    let duration = start.elapsed();

    PatternExecError::validate_response(expected_response, actual_response_buf)?;

    Ok(duration)
}

#[inline(always)]
async fn execute_del(
    conn: &mut BufStream<TcpStream>,
    key: &str,
    expected_response: String,
) -> Result<Duration, PatternExecError> {
    let command_string: String = format!("DEL {}\n", key);

    let start = Instant::now();

    conn.write_all(command_string.as_bytes()).await?;
    conn.flush().await?;

    let mut actual_response_buf = String::with_capacity(expected_response.len());
    conn.read_line(&mut actual_response_buf).await?;

    let duration = start.elapsed();

    PatternExecError::validate_response(expected_response, actual_response_buf)?;

    Ok(duration)
}

#[inline]
pub(crate) fn generate_get(key_len: usize) -> BasicCommand {
    let key = generate_valid_string(key_len);
    BasicCommand::Get { key }
}

#[inline]
pub(crate) fn generate_del(key_len: usize) -> BasicCommand {
    let key = generate_valid_string(key_len);
    BasicCommand::Del { key }
}

#[inline]
pub(crate) fn generate_set(key_len: usize, value_len: usize) -> BasicCommand {
    let key = generate_valid_string(key_len);
    let value = generate_valid_string(value_len);
    BasicCommand::Set { key, value }
}

#[inline(always)]
pub(crate) fn derive_get(set_command: &BasicCommand) -> BasicCommand {
    match set_command {
        BasicCommand::Set { key, .. } => BasicCommand::Get { key: key.clone() },
        _ => panic!("tried to derive a get command from a non set command"),
    }
}

#[inline(always)]
pub(crate) fn derive_del(set_command: &BasicCommand) -> BasicCommand {
    match set_command {
        BasicCommand::Set { key, .. } => BasicCommand::Del { key: key.clone() },
        _ => panic!("tried to derive a del command from a non set command"),
    }
}

#[inline(always)]
fn predict_get(state: &BasicState, key: &str) -> String {
    state
        .get(key)
        .map(String::to_string)
        .map(|mut e| {
            e.push('\n');
            e
        })
        .unwrap_or_else(|| "not found\n".into())
}

#[inline(always)]
fn predict_set(state: &mut BasicState, key: &str, val: &str) -> String {
    state
        .insert(key.to_string(), val.to_string())
        .map(|mut e| {
            e.push('\n');
            e
        })
        .unwrap_or_else(|| "not found\n".into())
}

#[inline(always)]
fn predict_del(state: &mut BasicState, key: &str) -> String {
    state
        .remove(key)
        .map(|mut e| {
            e.push('\n');
            e
        })
        .unwrap_or_else(|| "not found\n".into())
}

pub(crate) type BasicState = HashMap<String, String>;
