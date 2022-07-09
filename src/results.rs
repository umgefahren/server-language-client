use std::{sync::Arc, time::Duration};

use crate::pattern::{ExecPattern, PatternExecError};
use tokio::time::Instant;

pub struct ResultEntry {
    pub pattern: Arc<ExecPattern>,
    pub durations: Vec<Result<Duration, PatternExecError>>,
    pub total_duration: Duration,
    pub start_time: Instant,
}

const NO_ERROR_STR: &str = "-";
const NO_DUR_STR: &str = "-";

impl ResultEntry {
    pub(crate) fn to_csv_line(&self, global_start_time: Instant) -> String {
        self.to_string_vec(global_start_time).join(",")
    }

    #[inline]
    fn to_string_vec(&self, global_start_time: Instant) -> Vec<String> {
        let mut ret = Vec::with_capacity(self.pattern.0.len() + (self.durations.len() * 2) + 2);
        self.pattern_to_string_vec(&mut ret);
        self.durations_to_string_vec(&mut ret);
        self.total_duration_to_string_vec(&mut ret);
        self.start_time_to_string_vec(&mut ret, global_start_time);
        ret
    }

    #[inline]
    fn pattern_to_string_vec(&self, parts: &mut Vec<String>) {
        self.pattern
            .0
            .iter()
            .for_each(|e| parts.push(e.to_string()))
    }

    #[inline]
    fn durations_to_string_vec(&self, parts: &mut Vec<String>) {
        for i in self.durations.iter() {
            match i {
                Ok(d) => {
                    let duration_string = d.as_nanos().to_string();
                    parts.push(duration_string);
                    parts.push(NO_ERROR_STR.to_string());
                }
                Err(e) => {
                    let error_string = e.to_string();
                    parts.push(NO_DUR_STR.to_string());
                    parts.push(error_string);
                }
            }
        }
    }

    #[inline]
    fn total_duration_to_string_vec(&self, parts: &mut Vec<String>) {
        let duration_string = self.total_duration.as_nanos().to_string();
        parts.push(duration_string);
    }

    #[inline]
    fn start_time_to_string_vec(&self, parts: &mut Vec<String>, global_start_time: Instant) {
        let start_time_string = self
            .start_time
            .duration_since(global_start_time)
            .as_nanos()
            .to_string();
        parts.push(start_time_string);
    }
}
