use std::collections::BinaryHeap;
use std::{
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
};

use comfy_table::Table;
use tokio::sync::Semaphore;

use crate::pattern::basic::BasicState;
use crate::{
    pattern::{ExecPattern, ParsePattern},
    supplier::{feed_chans, feed_test, PatternResponse, TimeResult},
    worker::worker,
};

pub(crate) async fn perform_test(
    repetitions: usize,
    host: SocketAddr,
    pattern: ParsePattern,
    key_size: usize,
    value_size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (_kill_switch_sender, kill_switch_receiver) = tokio::sync::watch::channel(());

    let mut state = BasicState::new();

    let exec_pattern = ExecPattern::new(&pattern, key_size, value_size, &mut state);
    let kill_switch = Arc::new(AtomicBool::new(false));
    let (decoder_sender, decoder_receiver) = tokio::sync::mpsc::channel(1000);
    let (worker_sender, worker_receiver) = tokio::sync::mpsc::channel(1_00000000);

    let worker_chans = vec![worker_sender];
    let activator = Arc::new(Semaphore::new(0));
    let worker_activator = activator.clone();

    let worker_host = Arc::new(host);
    let worker_kill_switch = kill_switch_receiver.clone();

    let worker_handle = tokio::spawn(async move {
        let inner_host = worker_host;
        worker(
            worker_receiver,
            *inner_host,
            worker_kill_switch,
            worker_activator,
        )
        .await
    });

    activator.add_permits(1);

    let feeder_kill_switch = kill_switch_receiver.clone();

    let feeder_handle = tokio::spawn(async move {
        feed_chans::<true>(decoder_receiver, worker_chans, feeder_kill_switch).await
    });

    let decoder_handle = tokio::spawn(async move {
        feed_test(repetitions, exec_pattern, kill_switch, decoder_sender).await
    });

    feeder_handle.await??;
    decoder_handle.await??;
    let m = worker_handle.await??;
    test_resp_printer(m);

    Ok(())
}

fn test_resp_printer(mut results: BinaryHeap<PatternResponse>) {
    while !results.is_empty() {
        let response = results.pop().unwrap();
        let pattern = response.pattern;
        let TimeResult {
            durations,
            total_duration,
            start_time,
        } = response.timing;

        let mut table = Table::new();

        let mut header: Vec<String> = pattern.0.iter().map(ToString::to_string).collect();
        header.push("total duration".to_string());
        header.push("start time".to_string());
        let mut row: Vec<String> = durations
            .iter()
            .map(|e| match e {
                Ok(dur) => format!("Ok: {:?}", dur),
                Err(e) => format!("Err: {:?}", e),
            })
            .collect();
        row.push(format!("{:?}", total_duration));
        row.push(format!("{:?}", start_time));
        table.set_header(header).add_row(row);
        println!("{table}");
    }
}
