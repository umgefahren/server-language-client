use std::cmp::Ordering;

use std::error::Error;
use std::io::{ErrorKind, SeekFrom};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

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
    pub(crate) pattern: Arc<ExecPattern>,
}

impl Eq for PatternResponse {}

impl PartialEq<Self> for PatternResponse {
    fn eq(&self, other: &Self) -> bool {
        PartialEq::eq(&self.timing.start_time, &other.timing.start_time)
    }
}

impl PartialOrd<Self> for PatternResponse {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        PartialOrd::partial_cmp(&self.timing.start_time, &other.timing.start_time)
    }
}

impl Ord for PatternResponse {
    fn cmp(&self, other: &Self) -> Ordering {
        Ord::cmp(&self.timing.start_time, &other.timing.start_time)
    }
}

#[derive(Debug)]
pub(crate) struct PatternBundle {
    pub(crate) pattern: Arc<ExecPattern>,
}

pub(crate) async fn feed_chans<const TEST_MODE: bool>(
    mut pattern: tokio::sync::mpsc::Receiver<ExecPattern>,
    worker_chans: Vec<tokio::sync::mpsc::Sender<PatternBundle>>,
    kill_switch: tokio::sync::watch::Receiver<()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // let pat = pattern.recv_async().await?;
    let pat = tokio::task::unconstrained(pattern.recv()).await.unwrap();
    // let pat = pattern.recv().await.unwrap();
    let mut bundle = PatternBundle {
        pattern: Arc::new(pat),
    };
    for (idx, i) in worker_chans.iter().enumerate().cycle() {
        match i.try_send(bundle) {
            Ok(()) => {
                let pat_opt = tokio::task::unconstrained(pattern.recv()).await;
                let pat = if TEST_MODE && pat_opt.is_none() {
                    return Ok(());
                } else {
                    pat_opt.unwrap()
                };
                bundle = PatternBundle {
                    pattern: Arc::new(pat),
                };
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(d)) => {
                bundle = d;
            }
            e => {
                println!("Quitting => {:?}", e);
                println!("Idx => {}", idx);
                return Ok(());
            }
        }

        if kill_switch.has_changed().unwrap() {
            return Ok(());
        }
    }
    unreachable!()
}

async fn decode_from_file(
    file: &mut async_compression::tokio::bufread::ZstdDecoder<
        tokio::io::BufReader<tokio::fs::File>,
    >,
    buf: &mut Vec<u8>,
) -> std::io::Result<ExecPattern> {
    // println!("{} {}", file.get_mut().get_mut().stream_position().await?, file_length);
    /*
    if file.stream_position().await? >= file_length - 1000 {
        return Err(std::io::Error::new(ErrorKind::UnexpectedEof, "lol"));
    }

     */
    let mut length_buffer = [0u8; 8];

    file.read_exact(&mut length_buffer).await?;
    let length = u64::from_le_bytes(length_buffer);
    buf.resize(length as usize, 0);
    file.read_exact(buf).await?;
    let ret: ExecPattern = bincode::deserialize(buf).expect("error decoding");
    Ok(ret)
}

pub(crate) async fn feed_from_file<T: AsRef<Path>>(
    path: T,
    sender: tokio::sync::mpsc::Sender<ExecPattern>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    /*
    loop {
        let pattern = ExecPattern {
            0: vec![BasicCommand::Set { key: "KEY".into(), value: "VAL".into()}]
        };
        sender.send(pattern).await?;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

     */

    let file = tokio::fs::File::open(path)
        .await
        .expect("error opening file");

    let file_length = file
        .metadata()
        .await
        .expect("error getting metadata from file")
        .len();
    let file_buf = tokio::io::BufReader::new(file);

    let mut decoder = async_compression::tokio::bufread::ZstdDecoder::new(file_buf);
    let mut buf = vec![0u8; 100];

    let mut bytes_position = 0;
    loop {
        match decode_from_file(&mut decoder, &mut buf).await {
            Ok(d) => {
                sender.send(d).await?;
            }
            Err(e) => {
                match e.kind() {
                    ErrorKind::UnexpectedEof => {
                        let file_buf = decoder.into_inner();
                        let mut file = file_buf.into_inner();
                        file.seek(SeekFrom::Start(0)).await?;
                        let file_buf = tokio::io::BufReader::new(file);
                        decoder = async_compression::tokio::bufread::ZstdDecoder::new(file_buf);
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
            let file_buf = decoder.into_inner();

            let mut file = file_buf.into_inner();
            file.seek(SeekFrom::Start(0)).await?;
            decoder = async_compression::tokio::bufread::ZstdDecoder::new(
                tokio::io::BufReader::new(file),
            );
            bytes_position = 0;
        }
    }
}

pub(crate) async fn feed_test(
    repetitions: impl Into<usize>,
    pattern: ExecPattern,
    kill_switch: Arc<AtomicBool>,
    sender: tokio::sync::mpsc::Sender<ExecPattern>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Initiating test feeder");

    let repetitions: usize = repetitions.into();
    for _ in 0..repetitions {
        let local_pattern = pattern.clone();
        println!("sending local pattern");
        sender.send(local_pattern).await.unwrap();
    }

    kill_switch.store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}
