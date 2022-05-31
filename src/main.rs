#![allow(unused)]
#![allow(clippy::upper_case_acronyms)]

use std::error::Error;

use clap::Parser;
use options::Commands;
use test::perform_test;

use crate::{options::Cli, generator::generate};

pub(crate) mod options;
pub(crate) mod pattern;
pub(crate) mod state;
pub(crate) mod generator;
pub(crate) mod supplier;
pub(crate) mod test;
pub(crate) mod worker;
pub(crate) mod results;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Generate { size, data_out, pattern, key_size, value_size, compression_level } => {
            println!("generating");
            generate(size, data_out, pattern, key_size, value_size, compression_level).await?;
        },
        Commands::Test { repetitions, host, pattern, key_size, value_size } => {
            perform_test(repetitions, host, pattern, key_size, value_size).await?;
        },
        a => unimplemented!("{:?} is unimplemented", a)
    }
    Ok(())
}
