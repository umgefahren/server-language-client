use std::{sync::Arc, time::Duration};
use std::collections::BinaryHeap;

// use flume::{Receiver, TryRecvError};
use tokio::sync::mpsc::Receiver;
use tokio::{
    io::BufStream,
    net::{TcpStream, ToSocketAddrs},
    sync::Semaphore,
    time::Instant,
};

use crate::{
    state::State,
    supplier::{PatternBundle, PatternResponse, TimeResult},
};

pub(crate) async fn worker<A: ToSocketAddrs>(
    mut supplier: Receiver<PatternBundle>,
    address: A,
    kill_switch: tokio::sync::watch::Receiver<()>,
    activator: Arc<Semaphore>,
    state: &State,
) -> Result<BinaryHeap<PatternResponse>, Box<dyn std::error::Error + Send + Sync>> {
    // activator.acquire().await?.forget();

    let mut result_heap = BinaryHeap::new();

    loop {
        /*
        match kill_switch.has_changed() {
            Ok(true) => {
                println!("Got killed exiting");
                return Ok(result_heap)
            },
            Ok(_) => {},
            Err(e) => Err(e)?,
        }

         */

        /*
        let mut bundle_opt: Option<PatternBundle> = supplier.try_recv().ok();

        if bundle_opt.is_none() {
            tokio::select! {
                bundle_result = supplier.recv() => {
                    bundle_opt = Some(bundle_result.unwrap());
                }
                changed_result = kill_switch.changed() => {
                    let _ = changed_result.unwrap();
                    return Ok(result_heap);
                }
            }
        }*/

        let bundle = supplier.recv().await.unwrap();
        let response = execute_bundle(&address, bundle, state).await.unwrap();
        result_heap.push(response);

        println!("Heap Size in Worker => {}", result_heap.len());

        // tokio::task::yield_now().await;
    }
}

async fn execute_bundle<A: ToSocketAddrs>(
    address: &A,
    bundle: PatternBundle,
    state: &State,
) -> Result<PatternResponse, Box<dyn std::error::Error + Send + Sync>> {
    let pattern = bundle.pattern;

    let connection = TcpStream::connect(address)
        .await
        .expect("Error while trying to establish connection");
    let mut buf = BufStream::new(connection);
    // let mut buf = BufStream::with_capacity(10, 0, connection);

    let start_time = Instant::now();

    println!("Starting to execute");

    let (durations, total_duration) = match tokio::time::timeout(Duration::from_millis(100), pattern.execute(&mut buf, state)).await {
        Ok(Ok(d)) => d,
        e => {
            panic!("{:?}", e);

        },
    };

    println!("Stopped execution");


    // let (durations, total_duration) = pattern.execute(&mut buf, state).await?;


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
