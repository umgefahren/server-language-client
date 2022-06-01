use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

const WORKER_CHANNEL_SIZE: usize = 1;

use flume::{Receiver, Sender};
use tokio::{sync::Semaphore, task::JoinHandle};

use crate::{
    results::benchmark_resp_handler,
    state::State,
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

    let kill_switch = Arc::new(AtomicBool::new(false));

    let (decoder_sender, decoder_receiver) = flume::unbounded();

    let decoder_handle = tokio::spawn(async move {
        let res = feed_from_file(inp_file, decoder_sender).await;
        println!("from file feader died");
        res
    });

    println!("started decoder");

    let (resp_hand_sender, resp_hand_receiver) = flume::bounded(100);
    let resp_handler_kill_switch = kill_switch.clone();
    let resp_handler_handle = tokio::spawn(async move {
        benchmark_resp_handler(resp_hand_receiver, resp_handler_kill_switch, out_file).await;
        println!("benchmark response handler quit");
    });

    println!("started response handler");

    let (worker_senders, worker_receivers) = make_worker_chans(workers_num);

    println!("created worker chans");

    let activator = Arc::new(Semaphore::new(0));
    let host_arc = Arc::new(host);

    let workers = make_workers(
        worker_receivers,
        host_arc.clone(),
        activator.clone(),
        kill_switch.clone(),
    );

    println!("created workers");

    let feeder_kill_switch = kill_switch.clone();

    let feeder_handle = tokio::spawn(async move {
        let res = feed_chans::<false>(
            decoder_receiver,
            worker_senders,
            resp_hand_sender,
            feeder_kill_switch,
        )
        .await;
        println!("channel feeder quit");
        res
    });

    let killer_handle = std::thread::spawn(move || {
        println!("the killer is awake {:?}", duration);
        let now = Instant::now();
        // let bar = indicatif::ProgressBar::new(duration.as_secs());
        while now.elapsed() < duration {
            // println!("T -{}", (duration - now.elapsed()).as_secs());
            // bar.inc(1);
            std::thread::sleep(Duration::from_secs(1));
        }
        // bar.finish_and_clear();
        println!("Killing!!");
        for w in workers {
            w.abort();
        }
        kill_switch.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    activator.add_permits(workers_num * 10);

    killer_handle.join().expect("failed to join killer task");
    resp_handler_handle.await.unwrap();
    decoder_handle.abort();
    // decoder_handle.await??;
    // resp_handler_handle.abort();
    feeder_handle.await?.unwrap();
    // feeder_handle.await??;

    println!("finished benchmark");

    std::process::exit(0);
}

fn fd_limit_to_worker_num(fd_limit: u64) -> usize {
    let tmp = fd_limit / 3;
    tmp as usize
}

fn make_worker_chans(workers: usize) -> (Vec<Sender<PatternBundle>>, Vec<Receiver<PatternBundle>>) {
    let mut senders = Vec::with_capacity(workers);
    let mut receivers = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (sender, receiver) = flume::bounded(WORKER_CHANNEL_SIZE);
        senders.push(sender);
        receivers.push(receiver);
    }
    (senders, receivers)
}

fn make_workers(
    worker_receivers: Vec<Receiver<PatternBundle>>,
    host: Arc<SocketAddr>,
    activator: Arc<Semaphore>,
    kill_switch: Arc<AtomicBool>,
) -> Vec<JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>> {
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
