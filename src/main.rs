#![allow(clippy::upper_case_acronyms)]

use std::error::Error;

use benchmark::perform_benchmark;
use clap::Parser;
use options::Commands;
use test::perform_test;

use crate::{generator::generate, options::Cli};

pub(crate) mod benchmark;
pub(crate) mod generator;
pub(crate) mod options;
pub(crate) mod pattern;
pub(crate) mod results;
pub(crate) mod supplier;
pub(crate) mod test;
pub(crate) mod worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Generate {
            size,
            data_out,
            pattern,
            key_size,
            value_size,
            compression_level,
        } => {
            println!("generating");
            generate(
                size,
                data_out,
                pattern,
                key_size,
                value_size,
                compression_level,
            )
            .await?;
        }
        Commands::Test {
            repetitions,
            host,
            pattern,
            key_size,
            value_size,
        } => {
            perform_test(repetitions, host, pattern, key_size, value_size).await?;
        }
        Commands::Benchmark {
            duration,
            inp_file,
            out_file,
            host,
        } => {
            perform_benchmark(duration, inp_file, out_file, host, cli.fd_limit).await?;
        }
    }
    Ok(())
}
