use async_compression::tokio::bufread::ZstdDecoder;
use std::error::Error;
use std::io::{ErrorKind, SeekFrom};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use flume::{Receiver, Sender};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
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
        pattern: arc_pat,
    };
    (ret, resp)
}

#[derive(Debug)]
pub(crate) struct ResponseHandlerBundler {
    pub(crate) chan: oneshot::Receiver<PatternResponse>,
    pub(crate) pattern: Arc<ExecPattern>,
}

pub(crate) async fn feed_chans<const TEST_MODE: bool>(
    pattern: Receiver<ExecPattern>,
    worker_chans: Vec<Sender<PatternBundle>>,
    resp_hand_chan: Sender<ResponseHandlerBundler>,
    kill_switch: Arc<AtomicBool>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let pat = pattern.recv_async().await?;
    let (mut bundle, mut resp_bundle) = bundle_pattern(pat);
    for (idx, i) in worker_chans.iter().enumerate().cycle() {
        if idx == 0 {
            // println!("cycled");
        }

        match i.try_send(bundle) {
            Ok(()) => {
                resp_hand_chan.send_async(resp_bundle).await?;
                let pat = pattern.recv_async().await?;
                (bundle, resp_bundle) = bundle_pattern(pat);
            }
            Err(flume::TrySendError::Full(d)) => {
                bundle = d;
            }
            e => {
                println!("Quitting => {:?}", e);
                println!("Idx => {}", idx);
                return Ok(());
            }
        }

        if TEST_MODE && kill_switch.load(std::sync::atomic::Ordering::Relaxed) && pattern.is_empty()
        {
            return Ok(());
        } else if (!TEST_MODE) && kill_switch.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
    }
    unreachable!()
}

async fn decode_from_file(
    file: &mut ZstdDecoder<BufReader<File>>,
    buf: &mut Vec<u8>,
    file_length: u64,
) -> std::io::Result<ExecPattern> {
    // println!("{} {}", file.get_mut().get_mut().stream_position().await?, file_length);
    if file.get_mut().get_mut().stream_position().await? >= file_length - 1000 {
        return Err(std::io::Error::new(ErrorKind::UnexpectedEof, "lol"));
    }
    let length = file.read_u64_le().await?;
    buf.resize(length as usize, 0);
    let len = file.read_exact(buf).await?;
    assert_eq!(len, length as usize);
    let ret = bincode::deserialize(buf).expect("error decoding");
    Ok(ret)
}

pub(crate) async fn feed_from_file<T: AsRef<Path>>(
    path: T,
    sender: Sender<ExecPattern>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let file = File::open(path).await?;

    let file_length = file.metadata().await.unwrap().len();

    let file_buf = BufReader::new(file);
    let mut decoder = ZstdDecoder::new(file_buf);
    let mut buf = vec![0u8; 100];
    let mut iterations = 0;

    let mut bytes_position = 0;
    loop {
        match decode_from_file(&mut decoder, &mut buf, file_length).await {
            Ok(d) => {
                sender.send_async(d).await?;
            }
            Err(e) => {
                println!("Error {:?}", e);
                match e.kind() {
                    ErrorKind::UnexpectedEof => {
                        println!("refreshing file buffer");
                        let mut file_buf = decoder.into_inner();
                        file_buf.flush().await?;
                        let mut file = file_buf.into_inner();
                        file.seek(SeekFrom::Start(0)).await?;
                        file_buf = BufReader::new(file);
                        decoder = ZstdDecoder::new(file_buf);
                    }
                    _ => {
                        println!("Error in file feeder => {:?}", e);
                        return Err(Box::new(e));
                    }
                }
            }
        }
        if bytes_position >= file_length - 1000 {
            println!("refreshing file buffer 2");
            let mut file_buf = decoder.into_inner();
            file_buf.flush().await?;
            let mut file = file_buf.into_inner();
            file.seek(SeekFrom::Start(0)).await?;
            file_buf = BufReader::new(file);
            decoder = ZstdDecoder::new(file_buf);
            bytes_position = 0;
        }
        iterations += 1;
        if iterations % 1000 == 0 {
            println!("Iterations => {}", iterations);
        }
    }
}

pub(crate) async fn feed_test(
    repetitions: impl Into<usize>,
    pattern: ExecPattern,
    kill_switch: Arc<AtomicBool>,
    sender: Sender<ExecPattern>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
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
