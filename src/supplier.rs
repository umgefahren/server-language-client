use std::io::{ErrorKind, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::error::Error;

use flume::{Sender, Receiver};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::pattern::{ExecPattern, PatternExecError};

#[derive(Debug)]
pub(crate) struct TimeResult {
    pub(crate) durations: Vec<Result<Duration, PatternExecError>>,
    pub(crate) total_duration: Duration,
    pub(crate) start_time: Instant,
}

#[derive(Debug)]
pub(crate) struct PatternResponse {
    pub(crate) timing: TimeResult,
}

#[derive(Debug)]
pub(crate) struct PatternBundle {
    pub(crate) pattern: Arc<ExecPattern>,
    pub(crate) response_chan: oneshot::Sender<PatternResponse>,
}

fn bundle_pattern(pat: ExecPattern) -> (PatternBundle, ResponseHandlerBundler) {
    let (sender, receiver) = oneshot::channel();
    let arc_pat = Arc::new(pat);
    let ret = PatternBundle {
        pattern: arc_pat.clone(),
        response_chan: sender,
    };
    let resp = ResponseHandlerBundler {
        chan: receiver,
        pattern: arc_pat
    };
    (ret, resp)
}

#[derive(Debug)]
pub(crate) struct ResponseHandlerBundler {
    pub(crate) chan: oneshot::Receiver<PatternResponse>,
    pub(crate) pattern: Arc<ExecPattern>,
}

pub(crate) async fn feed_chans<const TEST_MODE: bool>(pattern: Receiver<ExecPattern>, worker_chans: Vec<Sender<PatternBundle>>, resp_hand_chan: Sender<ResponseHandlerBundler>, kill_switch: Arc<AtomicBool>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let pat = pattern.recv_async().await?;
    let (mut bundle, mut resp_bundle) = bundle_pattern(pat);
    for i in worker_chans.iter().cycle() {


        match i.try_send(bundle) {
            Ok(()) => {
                resp_hand_chan.send_async(resp_bundle).await?;
                let pat = pattern.recv_async().await?;
                (bundle, resp_bundle) = bundle_pattern(pat);
            },
            Err(flume::TrySendError::Full(d)) => {
                bundle = d;
            },
            e => {
                println!("Quitting => {:?}", e);
                return Ok(());
            },
        }


        if TEST_MODE && kill_switch.load(std::sync::atomic::Ordering::Relaxed) && pattern.is_empty() {
            return Ok(());
        } else if (!TEST_MODE) && kill_switch.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
    }
    unreachable!()
}

async fn decode_from_file(file: &mut File, buf: &mut Vec<u8>) -> std::io::Result<ExecPattern> {
    let length = file.read_u64_le().await?;
    buf.resize(length as usize, 0);
    file.read_exact(buf).await?;
    let ret = bincode::deserialize(buf).expect("error decoding");
    Ok(ret)
}

pub(crate) async fn feed_from_file<T: AsRef<Path>>(path: T, sender: Sender<ExecPattern>) -> Result<(), Box<dyn Error>> {
    let mut file = File::open(path).await?;
    let mut buf = vec![0u8; 100];
    loop {
        match decode_from_file(&mut file, &mut buf).await {
            Ok(d) => {
                sender.send_async(d).await?;
            },
            Err(e) => {
                match e.kind() {
                    ErrorKind::UnexpectedEof => {
                        file.seek(SeekFrom::Start(0)).await?;
                    },
                    _ => return Err(Box::new(e)),
                }
            }
        }
    }
}

pub(crate) async fn feed_test(repetitions: impl Into<usize>, pattern: ExecPattern, kill_switch: Arc<AtomicBool>, sender: Sender<ExecPattern>) -> Result<(), Box<dyn Error + Send + Sync>> {

    println!("Initiating test feeder");

    let repetitions: usize = repetitions.into();
    for _ in 0..repetitions {
        let local_pattern = pattern.clone();
        println!("sending local pattern");
        sender.send_async(local_pattern).await?;
    }

    kill_switch.store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}
