use std::sync::{Arc, atomic::AtomicBool};

use flume::Receiver;
use tokio::{sync::{Notify, Semaphore}, net::{ToSocketAddrs, TcpStream}, io::BufStream, time::Instant};

use crate::{supplier::{PatternBundle, PatternResponse, TimeResult}, state::State};


pub(crate) async fn worker<A: ToSocketAddrs>(supplier: Receiver<PatternBundle>, address: A, kill_swith: Arc<AtomicBool>, activator: Arc<Semaphore>, state: &State) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    activator.acquire().await?;

    println!("starting to work now");

    loop {
        if kill_swith.load(std::sync::atomic::Ordering::Relaxed) && supplier.is_empty() {
            return Ok(());
        }
        
        let bundle = supplier.recv_async().await?;
        execute_bundle(&address, bundle, state).await?;
    }
}

async fn execute_bundle<A: ToSocketAddrs>(address: &A, bundle: PatternBundle, state: &State) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pattern = bundle.pattern;
    let sender = bundle.response_chan;

    let connection = TcpStream::connect(address).await?;
    let mut buf = BufStream::new(connection);

    let start_time = Instant::now();

    let (durations, total_duration) = pattern.execute(&mut buf, state).await?;

    let timing = TimeResult {
        durations,
        total_duration,
        start_time
    };

    let response = PatternResponse {
        timing
    };

    tokio::spawn(async {
        sender.send(response).expect("failed to send pattern response");
    });

    Ok(())
}
