use std::collections::BinaryHeap;
use std::{
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

const WORKER_CHANNEL_SIZE: usize = 100;

use tokio::{sync::Semaphore, task::JoinHandle};

use crate::results::ResultEntry;
use crate::supplier::PatternResponse;
use crate::{
    supplier::{feed_chans, feed_from_file, PatternBundle},
    worker::worker,
};

pub(crate) async fn perform_benchmark(
    duration: Duration,
    inp_file: PathBuf,
    out_file: PathBuf,
    host: SocketAddr,
    fd_limit: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let workers_num = fd_limit_to_worker_num(fd_limit);

    println!("creating {} workers", workers_num);

    let start_time = std::time::Instant::now();

    let (kill_switch_sender, mut kill_switch_receiver) = tokio::sync::watch::channel(());
    kill_switch_receiver.borrow_and_update();

    let (decoder_sender, decoder_receiver) = tokio::sync::mpsc::channel(1000);

    let decoder_handle = tokio::spawn(async move {
        let res = feed_from_file(inp_file, decoder_sender).await;
        println!("from file feader died");
        res
    });

    println!("started decoder");

    let (worker_senders, worker_receivers) = make_worker_chans(workers_num);

    println!("created worker chans");

    let activator = Arc::new(Semaphore::new(0));
    let host_arc = Arc::new(host);

    let workers = make_workers(
        worker_receivers,
        host_arc.clone(),
        activator.clone(),
        kill_switch_receiver.clone(),
    );

    println!("created workers");

    let feeder_handle = tokio::spawn(async move {
        let res = feed_chans::<false>(
            decoder_receiver,
            worker_senders,
            kill_switch_receiver.clone(),
        )
        .await;
        println!("channel feeder quit");
        res
    });

    let killer_handle = tokio::spawn(async move {
        println!("the killer is awake {:?}", duration);
        let now = Instant::now();
        let bar = indicatif::ProgressBar::new(duration.as_secs() - 1);
        while now.elapsed() < duration {
            bar.set_position(now.elapsed().as_secs());
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        bar.finish_and_clear();
        println!("Killing!!");
        kill_switch_sender.send(()).unwrap();
    });

    activator.add_permits(workers_num * 10);

    let mut all_results = BinaryHeap::new();
    for worker in workers {
        let mut m = worker.await.unwrap().unwrap();
        all_results.append(&mut m);
    }

    killer_handle.await.expect("failed to join killer task");

    decoder_handle.abort();
    // decoder_handle.await??;
    // resp_handler_handle.abort();
    feeder_handle.await.unwrap().unwrap();

    println!("finished benchmark");

    let mut out_file = std::fs::File::create(out_file)?;

    let responses = all_results.into_sorted_vec();
    responses
        .into_iter()
        .map(|e| ResultEntry {
            pattern: e.pattern,
            durations: e.timing.durations,
            total_duration: e.timing.total_duration,
            start_time: e.timing.start_time,
        })
        .map(|e| e.to_csv_line(start_time.into()))
        .for_each(|mut e| {
            e.push('\n');
            out_file.write_all(e.as_bytes()).unwrap();
        });

    std::process::exit(0);
}

fn fd_limit_to_worker_num(fd_limit: u64) -> usize {
    let concurrency_available = std::thread::available_parallelism().unwrap().get() * 4;
    let tmp = fd_limit as usize;
    concurrency_available.min(tmp)
}

fn make_worker_chans(
    workers: usize,
) -> (
    Vec<tokio::sync::mpsc::Sender<PatternBundle>>,
    Vec<tokio::sync::mpsc::Receiver<PatternBundle>>,
) {
    let mut senders = Vec::with_capacity(workers);
    let mut receivers = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (sender, receiver) = tokio::sync::mpsc::channel(WORKER_CHANNEL_SIZE);
        senders.push(sender);
        receivers.push(receiver);
    }
    (senders, receivers)
}

fn make_workers(
    worker_receivers: Vec<tokio::sync::mpsc::Receiver<PatternBundle>>,
    host: Arc<SocketAddr>,
    activator: Arc<Semaphore>,
    kill_switch: tokio::sync::watch::Receiver<()>,
) -> Vec<JoinHandle<Result<BinaryHeap<PatternResponse>, Box<dyn std::error::Error + Send + Sync>>>>
{
    let mut ret = Vec::with_capacity(worker_receivers.len());

    for r in worker_receivers {
        let local_host = host.clone();
        let local_activator = activator.clone();
        let local_kill_switch = kill_switch.clone();
        let worker_handle = tokio::spawn(async move {
            let inner_host = local_host.clone();
            let res = worker(r, *inner_host, local_kill_switch, local_activator).await;

            res
        });

        ret.push(worker_handle);
    }

    ret
}
