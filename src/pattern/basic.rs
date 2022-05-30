use std::time::Duration;

use chashmap::ReadGuard;
use serde::{Serialize, Deserialize};
use tokio::{net::TcpStream, io::{AsyncWriteExt, BufStream, AsyncBufReadExt}, time::Instant};

use crate::{generator::generate_valid_string, supplier::PatternResponse, state::State};

use super::{ParsePattern, ParsePatternCommand, PatternExecError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct BasicPattern(pub(crate) Vec<BasicCommand>);

impl BasicPattern {
    pub(crate) fn new(p: &ParsePattern, key_len: usize, value_len: usize) -> Self {
        let mut current_set: Option<BasicCommand> = None;
        let content = p.0
            .iter()
            .map(|e| {
                match e {
                    ParsePatternCommand::SET => {
                        let ret = generate_set(key_len, value_len);
                        current_set = Some(ret.clone());
                        ret
                    },
                    ParsePatternCommand::GET => {
                        match &current_set {
                            Some(s) => derive_get(s),
                            None => generate_get(key_len),
                        }
                    },
                    ParsePatternCommand::DEL => {
                        match &current_set {
                            Some(s) => derive_del(s),
                            None => generate_del(key_len)
                        }
                    }
                }
            }).collect();
        Self(content)
    }

    pub(crate) async fn execute(&self, conn: &mut BufStream<TcpStream>, state: &State) -> std::io::Result<(Vec<Result<Duration, PatternExecError>>, Duration)> {
        let mut ret = Vec::with_capacity(self.0.len());
        let start = tokio::time::Instant::now();
        for b in self.0.iter() {
            let res = b.execute(conn, state).await;
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
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    Del {
        key: String,
    },
}

impl BasicCommand {
    #[inline(always)]
    async fn execute(&self, conn: &mut BufStream<TcpStream>, state: &State) -> Result<Duration, PatternExecError> {
        match self {
            BasicCommand::Get { ref key } => execute_get(conn, key, state).await,
            BasicCommand::Set { ref key, ref value } => execute_set(conn, key.to_string(), value.to_string(), state).await,
            BasicCommand::Del { ref key } => execute_del(conn, key, state).await,
        }
    }

}

impl ToString for BasicCommand {
    fn to_string(&self) -> String {
        match self {
            BasicCommand::Get { ref key } => format!("GET {}", key),
            BasicCommand::Set { ref key, ref value } => format!("SET {} {}", key, value),
            BasicCommand::Del { ref key } => format!("DEL {}", key)
        }
    }
}

#[inline(always)]
fn found_not_found(inp: Option<ReadGuard<String, String>>) -> String {
    inp.map(|e| {
        let mut ret = e.to_string();
        ret.push('\n');
        ret
    }).unwrap_or("not found\n".to_string())
}

#[inline(always)]
async fn execute_get(conn: &mut BufStream<TcpStream>, key: &str, state: &State) -> Result<Duration, PatternExecError> {
    let command_string: String = format!("GET {}\n", key);

    let start = Instant::now();

    conn.write_all(command_string.as_bytes()).await?; conn.flush().await?;
    conn.flush().await?;

    let expected_response = found_not_found(state.map.get(key));

    let mut actual_response_buf = String::with_capacity(expected_response.len());
    conn.read_line(&mut actual_response_buf).await?;

    let duration = start.elapsed();

    PatternExecError::validate_response(expected_response, actual_response_buf)?;
    

    Ok(duration)
}

#[inline(always)]
async fn execute_set(conn: &mut BufStream<TcpStream>, key: String, value: String, state: &State) -> Result<Duration, PatternExecError> {
    let command_string: String = format!("SET {} {}\n", key, value);

    let start = Instant::now();

    conn.write_all(command_string.as_bytes()).await?;
    conn.flush().await?;

    let expected_response = state.map.insert(key, value)
        .map(|e| {
            let mut ret = e.to_string();
            ret.push('\n');
            ret
        }).unwrap_or("not found\n".to_string());

    let mut actual_response_buf = String::with_capacity(expected_response.len());
    conn.read_line(&mut actual_response_buf).await?;
    
    let duration = start.elapsed();

    PatternExecError::validate_response(expected_response, actual_response_buf)?;

    Ok(duration)
}

#[inline(always)]
async fn execute_del(conn: &mut BufStream<TcpStream>, key: &str, state: &State) -> Result<Duration, PatternExecError> {
    let command_string: String = format!("DEL {}\n", key);

    let start = Instant::now();

    conn.write_all(command_string.as_bytes()).await?;
    conn.flush().await?;

    let expected_response = state.map.remove(key).map(|e| {
        let mut ret = e.to_string();
        ret.push('\n');
        ret
    }).unwrap_or("not found\n".to_string());


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


#[inline]
pub(crate) fn derive_get(set_command: &BasicCommand) -> BasicCommand {
    match set_command {
        BasicCommand::Set { key, .. } => BasicCommand::Get { key: key.clone() },
        _ => panic!("tried to derive a get command from a non set command"),
    }
}

#[inline]
pub(crate) fn derive_del(set_command: &BasicCommand) -> BasicCommand {
    match set_command {
        BasicCommand::Set { key, .. } => BasicCommand::Del { key: key.clone() },
        _ => panic!("tried to derive a del command from a non set command"),
    }
}

