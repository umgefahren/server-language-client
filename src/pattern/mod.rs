pub(crate) mod basic;

use std::str::FromStr;

use thiserror::Error;

use self::basic::BasicPattern;

pub(crate) type ExecPattern = BasicPattern;

#[derive(Debug, Clone, Copy)]
pub(crate) enum ParsePatternCommand {
    GET,
    SET,
    DEL,
}

impl FromStr for ParsePatternCommand {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GET" => Ok(Self::GET),
            "SET" => Ok(Self::SET),
            "DEL" => Ok(Self::DEL),
            _ => Err("invalid command in pattern"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ParsePattern(pub(crate) Vec<ParsePatternCommand>);

impl FromStr for ParsePattern {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner = s
            .split('-')
            .map(move |e: &str| -> ParsePatternCommand {
                ParsePatternCommand::from_str(e).unwrap()
            })
            .collect();

        Ok(Self(inner))
    }
}

#[derive(Debug, Error)]
pub(crate) enum PatternExecError {
    #[error("io error occured while trying to execute pattern")]
    IoError(#[from] std::io::Error),
    #[error("invalid response (expected {expected:?}, found {found:?})")]
    InvalidResponse { expected: String, found: String },
}

impl PatternExecError {
    #[inline(always)]
    pub(crate) fn invalid_response(expected: String, found: String) -> Self {
        Self::InvalidResponse { expected, found }
    }

    #[inline(always)]
    pub(crate) fn validate_response(expected: String, found: String) -> Result<(), Self> {
        if expected != found {
            return Err(Self::invalid_response(expected, found));
        }

        Ok(())
    }
}
