use std::{
    path::Path,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use crate::{
    pattern::{ExecPattern, PatternExecError},
    supplier::{PatternResponse, ResponseHandlerBundler, TimeResult},
};
use flume::Receiver;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    time::Instant,
};

pub(crate) struct ResultEntry {
    pattern: Arc<ExecPattern>,
    durations: Vec<Result<Duration, PatternExecError>>,
    total_duration: Duration,
    start_time: Instant,
}

const NO_ERROR_STR: &str = "-";
const NO_DUR_STR: &str = "-";

impl ResultEntry {
    pub(crate) fn new(pattern: Arc<ExecPattern>, time_res: TimeResult) -> Self {
        Self {
            pattern,
            durations: time_res.durations,
            total_duration: time_res.total_duration,
            start_time: time_res.start_time,
        }
    }

    pub(crate) fn to_csv_line(&self, global_start_time: Instant) -> String {
        self.to_string_vec(global_start_time).join(", ")
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

pub(crate) async fn benchmark_resp_handler(
    resp_hand_receiver: Receiver<ResponseHandlerBundler>,
    kill_switch: Arc<AtomicBool>,
    out_path: impl AsRef<Path>,
) {
    let file = File::create(out_path)
        .await
        .expect("error creating the file for output");
    let mut file_buf = BufWriter::new(file);

    let global_start_time = Instant::now();

    let (incoming_sender, mut incoming_receiver) =
        tokio::sync::mpsc::unbounded_channel::<(TimeResult, Arc<ExecPattern>)>();

    let acceptor_kill_switch = kill_switch.clone();

    tokio::spawn(async move {
        loop {
            if acceptor_kill_switch.load(std::sync::atomic::Ordering::Relaxed) {
                return;
            }

            let local_sender = incoming_sender.clone();
            let ResponseHandlerBundler { chan, pattern } =
                match resp_hand_receiver.recv_async().await {
                    Ok(d) => d,
                    Err(_) => {
                        println!("shutting down response handler");
                        return;
                    }
                };
            tokio::spawn(async move {
                // println!("forked");
                let PatternResponse { timing } = match chan.await {
                    Ok(d) => d,
                    _ => {
                        // println!("{:?}", e);
                        return;
                    }
                };
                local_sender
                    .send((timing, pattern))
                    .expect("error sending local awnser");
                // local_sender.send_async((timing, pattern)).await.expect("error sending local awnser");
            });
        }
    });

    let mut counter = 0;

    loop {
        if kill_switch.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let (timing, pattern) = incoming_receiver
            .recv()
            .await
            .expect("Error getting result from incoming");
        let result_entry = ResultEntry::new(pattern, timing);
        let mut line = result_entry.to_csv_line(global_start_time);
        line.push('\n');
        file_buf
            .write_all(line.as_bytes())
            .await
            .expect("error writing line to file");
        counter += 1;
        if counter % 1000 == 0 {
            println!("Counter => {}", counter);
        }
        // file_buf.flush().await.expect("error while flushing");
    }
}
