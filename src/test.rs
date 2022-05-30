use std::{net::SocketAddr, sync::{Arc, atomic::AtomicBool}};

use comfy_table::Table;
use flume::Receiver;
use tokio::sync::{Notify, Semaphore};

use crate::{pattern::{ParsePattern, ExecPattern}, supplier::{ResponseHandlerBundler, PatternResponse, TimeResult, feed_chans, feed_test}, state::State, worker::worker};



pub(crate) async fn perform_test(repetitions: usize, host: SocketAddr, pattern: ParsePattern, key_size: usize, value_size: usize) -> Result<(), Box<dyn std::error::Error>> {
    let exec_pattern = ExecPattern::new(&pattern, key_size, value_size);
    let kill_switch = Arc::new(AtomicBool::new(false));
    let (decoder_sender, decoder_receiver) = flume::unbounded();
    let (worker_sender, worker_receiver) = flume::unbounded();
    let (resp_hand_sender, resp_hand_receiver) = flume::unbounded();



    let resp_handler_kill_switch = kill_switch.clone();
    let resp_handler_handle = tokio::spawn(async move {
        test_resp_handler(resp_hand_receiver, resp_handler_kill_switch).await
    });

    let worker_chans = vec![worker_sender];
    let worker_kill_switch = kill_switch.clone();
    let activator = Arc::new(Semaphore::new(0));
    let worker_activator = activator.clone();

    let state = Arc::new(State::new());
    let worker_state = state.clone();

    let worker_host = Arc::new(host.clone());

    let worker_handle = tokio::spawn(async move { 
        let inner_host = worker_host;
        let inner_worker_state = worker_state;
        worker(worker_receiver, &*inner_host, worker_kill_switch, worker_activator, &inner_worker_state).await
    });

    activator.add_permits(1);


    let feeder_kill_switch = kill_switch.clone();

    let feeder_handle = tokio::spawn(async move {
        feed_chans::<true>(decoder_receiver, worker_chans, resp_hand_sender, feeder_kill_switch).await
    });


    let decoder_handle = tokio::spawn( async move {
        feed_test(repetitions, exec_pattern, kill_switch, decoder_sender).await
    });


    feeder_handle.await?;
    decoder_handle.await?;
    worker_handle.await?;
    resp_handler_handle.await?;

    Ok(())

}

async fn test_resp_handler(resp_hand_receiver: Receiver<ResponseHandlerBundler>, kill_switch: Arc<AtomicBool>) {

    loop {
        if kill_switch.load(std::sync::atomic::Ordering::Relaxed) && resp_hand_receiver.is_empty() {
            return;
        }

        let ResponseHandlerBundler { chan, pattern } = resp_hand_receiver.recv_async().await.expect("error receiving");
        let PatternResponse { timing } = chan.await.expect("error receiving one shot");
        let TimeResult { durations, total_duration, start_time } = timing;

        let mut table = Table::new();

        let header: Vec<String> = pattern.0.iter().map(ToString::to_string).collect();
        let row: Vec<String> = durations.iter()
            .map(|e| {
                match e {
                    Ok(dur) => format!("Ok: {}ms", dur.as_millis()),
                    Err(e) => format!("Err: {:?}", e),
                }
            })
            .collect();
        table
            .set_header(header)
            .add_row(row);

        println!("{table}");
    }

}
