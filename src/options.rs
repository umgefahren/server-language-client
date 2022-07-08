use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};

#[cfg(unix)]
use libc::{getrlimit, rlimit, setrlimit, RLIMIT_NOFILE};

use crate::pattern::ParsePattern as Pattern;

#[cfg(unix)]
fn get_cur_file_descritpors_unix() -> u64 {
    let mut lim = rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let lim_ptr: *mut rlimit = &mut lim;
    let check_result = unsafe { getrlimit(RLIMIT_NOFILE, lim_ptr) };
    match check_result {
        0 => {
            let hard_limit = lim.rlim_max;
            lim.rlim_cur = hard_limit;
            let lim_ptr: *mut rlimit = &mut lim;
            unsafe { setrlimit(RLIMIT_NOFILE, lim_ptr) };
            hard_limit.try_into().expect("couldn't convert")
        }
        _ => {
            panic!("error reading maximum number of file descriptors");
        }
    }
}

#[cfg(not(unix))]
fn default_file_descriptor() -> u64 {
    eprintln!("the client is intended for use of unix or unix-like systems, usage outside of this may lead to unexpected behaviour, since the client tries to operate as close to OS-limits as possible");

    1_000
}

#[inline(always)]
fn get_file_descriptor_limit() -> u64 {
    #[cfg(unix)]
    return get_cur_file_descritpors_unix();
    #[cfg(not(unix))]
    return default_file_descriptor();
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub(crate) struct Cli {
    /// subcommand
    #[clap(subcommand)]
    pub(crate) command: Commands,
    /// limit on the number of concurrent file descriptors
    /// that can be open concurrently
    #[clap(short = 'l', long, default_value_t = get_file_descriptor_limit())]
    pub(crate) fd_limit: u64,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    /// generate benchmark data
    Generate {
        /// amount of patterns to be generated
        size: usize,
        /// file for where to put the generated data
        #[clap(default_value = "data.bin")]
        data_out: PathBuf,
        /// pattern to pass
        #[clap(parse(try_from_str), default_value = "SET-GET-GET-DEL")]
        pattern: Pattern,
        /// key size in number of characters
        #[clap(default_value_t = 10)]
        key_size: usize,
        /// value size in number of characters
        #[clap(default_value_t = 10)]
        value_size: usize,
        #[clap(min_values(0), max_values(21), default_value_t = 0)]
        compression_level: i32,
    },
    Test {
        /// specify how often the given pattern should be repeated
        #[clap(default_value_t = 1)]
        repetitions: usize,
        /// host on which the server is listening
        #[clap(default_value = "0.0.0.0:8080")]
        host: SocketAddr,
        /// pattern that will be executed
        ///
        /// A pattern is defined as pattern key words, seperated by a `-`.
        /// Valid pattern key words are:
        ///
        /// * SET
        ///
        /// * GET
        ///
        /// * DEL
        #[clap(parse(try_from_str), default_value = "SET-GET-GET-DEL")]
        pattern: Pattern,
        /// the size of the generated keys
        #[clap(default_value_t = 10)]
        key_size: usize,
        /// the size of the generated values
        #[clap(default_value_t = 10)]
        value_size: usize,
    },
    Benchmark {
        #[clap(parse(try_from_str=parse_duration::parse))]
        duration: std::time::Duration,
        #[clap(default_value = "data.bin")]
        inp_file: PathBuf,
        #[clap(default_value = "result.csv")]
        out_file: PathBuf,
        #[clap(default_value = "127.0.0.1:8080")]
        host: SocketAddr,
    },
}
