use std::{path::PathBuf, net::SocketAddr};


use clap::{Parser, Subcommand};

#[cfg(unix)]
use libc::{rlimit, getrlimit, RLIMIT_NOFILE};

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
            lim.rlim_cur.try_into().expect("limit didn't fit")
        },
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
        data_out: PathBuf,
        /// pattern to pass
        #[clap(parse(try_from_str), default_value = "SET-GET-GET-DEL")]
        pattern: Pattern,
        /// key size in number of characters
        #[clap(default_value_t = 100)]
        key_size: usize,
        /// value size in number of characters
        #[clap(default_value_t = 100)]
        value_size: usize,
        #[clap(min_values(0), max_values(21), default_value_t = 0)]
        compression_level: i32,
    },
    Test {
        #[clap(default_value_t = 1)]
        repetitions: usize,
        #[clap(default_value = "127.0.0.1:8080")]
        host: SocketAddr,
        #[clap(parse(try_from_str), default_value = "SET-GET-GET-DEL")]
        pattern: Pattern,
        #[clap(default_value_t = 10)]
        key_size: usize,
        #[clap(default_value_t = 10)]
        value_size: usize,
    },
}

