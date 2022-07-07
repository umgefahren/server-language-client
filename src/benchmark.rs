use std::{
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, Instant},
    sync::Arc,
};
use std::collections::BinaryHeap;

const WORKER_CHANNEL_SIZE: usize = 5;

use tokio::{sync::Semaphore, task::JoinHandle};

use crate::{
    // results::benchmark_resp_handler,
    state::State,
    supplier::{feed_chans, feed_from_file, PatternBundle},
    worker::worker,
};
use crate::supplier::PatternResponse;

pub(crate) async fn perform_benchmark(
    duration: Duration,
    inp_file: PathBuf,
    _out_file: PathBuf,
    host: SocketAddr,
    fd_limit: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let workers_num = fd_limit_to_worker_num(fd_limit);

    println!("creating {} workers", workers_num);


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
            kill_switch_receiver.clone()
        ).await;
        println!("channel feeder quit");
        res
    });

    let killer_handle = tokio::spawn(async move {
        println!("the killer is awake {:?}", duration);
        let now = Instant::now();
        // let bar = indicatif::ProgressBar::new(duration.as_secs());
        while now.elapsed() < duration {
            // println!("T -{}", (duration - now.elapsed()).as_secs());
            // bar.inc(1);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        // bar.finish_and_clear();
        println!("Killing!!");

        // kill_switch.store(true, std::sync::atomic::Ordering::Relaxed);
        kill_switch_sender.send(()).unwrap();
        ()
    });

    activator.add_permits(workers_num * 10);


    // feeder_handle.await??;

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

    std::process::exit(0);
}

fn fd_limit_to_worker_num(fd_limit: u64) -> usize {
    let concurrency_available = std::thread::available_parallelism().unwrap().get();
    let tmp = fd_limit as usize;
    // concurrency_available.min(tmp)
    10
}

fn make_worker_chans(workers: usize) -> (Vec<tokio::sync::mpsc::Sender<PatternBundle>>, Vec<tokio::sync::mpsc::Receiver<PatternBundle>>) {
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
) -> Vec<JoinHandle<Result<BinaryHeap<PatternResponse>, Box<dyn std::error::Error + Send + Sync>>>> {
    let mut ret = Vec::with_capacity(worker_receivers.len());
    let state = Arc::new(State::new());

    for r in worker_receivers {
        let local_host = host.clone();
        let local_activator = activator.clone();
        let local_kill_switch = kill_switch.clone();
        let local_state = state.clone();
        let worker_handle = tokio::spawn(async move {
            let inner_state = local_state.clone();
            let inner_host = local_host.clone();
            let res = worker(
                r,
                *inner_host,
                local_kill_switch,
                local_activator,
                inner_state.as_ref(),
            )
            .await;
            println!("this worker just died. FUUUUUUUUCK");
            println!("{:?}", res);
            res
        });

        // println!("created worker handle");
        ret.push(worker_handle);
    }

    ret
}
