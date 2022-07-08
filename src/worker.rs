use std::collections::BinaryHeap;

use std::sync::Arc;
use std::time::Duration;

// use flume::{Receiver, TryRecvError};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Receiver;
use tokio::{io::BufStream, net::TcpStream, sync::Semaphore, time::Instant};

use crate::{
    state::State,
    supplier::{PatternBundle, PatternResponse, TimeResult},
};

pub(crate) async fn worker(
    mut supplier: Receiver<PatternBundle>,
    address: std::net::SocketAddr,
    mut kill_switch: tokio::sync::watch::Receiver<()>,
    activator: Arc<Semaphore>,
    state: &State,
) -> Result<BinaryHeap<PatternResponse>, Box<dyn std::error::Error + Send + Sync>> {
    activator.acquire().await?.forget();

    let mut result_heap = BinaryHeap::new();

    loop {
        match kill_switch.has_changed() {
            Ok(true) => {
                println!("Got killed exiting");
                return Ok(result_heap);
            }
            Ok(_) => {}
            Err(e) => {
                println!("Quitting from error in kill switch => {:?}", e);
                return Ok(result_heap);
            }
        }

        let mut bundle_opt: Option<PatternBundle> = supplier.try_recv().ok();

        if bundle_opt.is_none() {
            tokio::select! {
                bundle_result = supplier.recv() => {
                    if bundle_result.is_none() {
                        println!("Empty supplier, exiting worker");
                        return Ok(result_heap);
                    }
                    bundle_opt = Some(bundle_result.unwrap());
                }
                changed_result = kill_switch.changed() => {
                    changed_result.unwrap();
                    return Ok(result_heap);
                }
            }
        }

        let bundle = bundle_opt.unwrap();
        let response = execute_bundle(&address, bundle, state).await.unwrap();
        result_heap.push(response);

        /* if result_heap.len() % 100 == 0 {
            println!("Heap Size in Worker => {}", result_heap.len());
        } */

        // tokio::task::yield_now().await;
    }
}

async fn execute_bundle(
    address: &std::net::SocketAddr,
    bundle: PatternBundle,
    state: &State,
) -> Result<PatternResponse, Box<dyn std::error::Error + Send + Sync>> {
    let pattern = bundle.pattern;

    let connection = TcpStream::connect(address).await?;
    // let local_address = connection.local_addr().unwrap();
    // println!("Port => {}", local_address.port());

    let mut buf = BufStream::new(connection);
    // let mut buf = BufStream::with_capacity(10, 0, connection);

    let start_time = Instant::now();

    // println!("Starting to execute");

    /*
    let (durations, total_duration) = match tokio::time::timeout(Duration::from_millis(1000), pattern.execute(&mut buf, state)).await {
        Ok(Ok(d)) => d,
        e => {


            panic!("{:?}", e);
        },
    };


     */

    let (durations, total_duration) = pattern.execute(&mut buf, state).await?;

    let mut tcp = buf.into_inner();
    tcp.flush().await?;
    tcp.set_linger(Some(Duration::from_millis(1))).unwrap();
    tcp.shutdown().await?;

    drop(tcp);


    let timing = TimeResult {
        durations,
        total_duration,
        start_time,
    };

    let response = PatternResponse { timing, pattern };

    /*

    tokio::spawn(async move {
        sender
            .send(response)
            .await
            .expect("error passing pattern response");
    });
    */

    Ok(response)
}
