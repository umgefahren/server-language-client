use std::sync::{atomic::AtomicBool, Arc};

use flume::Receiver;
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
    supplier: Receiver<PatternBundle>,
    address: A,
    kill_swith: Arc<AtomicBool>,
    activator: Arc<Semaphore>,
    state: &State,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    activator.acquire().await?.forget();

    // println!("starting to work now");

    loop {
        if kill_swith.load(std::sync::atomic::Ordering::Relaxed) && supplier.is_empty() {
            return Ok(());
        }

        let bundle = supplier.recv_async().await?;
        execute_bundle(&address, bundle, state).await?;
    }
}

async fn execute_bundle<A: ToSocketAddrs>(
    address: &A,
    bundle: PatternBundle,
    state: &State,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pattern = bundle.pattern;
    let sender = bundle.response_chan;

    let connection = TcpStream::connect(address)
        .await
        .expect("Error while trying to establish connection");
    let mut buf = BufStream::new(connection);
    // let mut buf = BufStream::with_capacity(10, 0, connection);

    let start_time = Instant::now();

    let (durations, total_duration) = pattern.execute(&mut buf, state).await?;

    let timing = TimeResult {
        durations,
        total_duration,
        start_time,
    };

    let response = PatternResponse { timing };

    sender
        .send(response)
        .expect("error passing pattern response");

    Ok(())
}
