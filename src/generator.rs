use std::io::{Result as IoResult, Write};
use std::path::PathBuf;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rand::{thread_rng, Rng};
use std::fs::File;
use std::io::BufWriter;
use std::ops::RangeInclusive;
use zstd::Encoder;

use crate::pattern::basic::{BasicPattern, BasicState};
use crate::pattern::ParsePattern;

const LOWER_CASE_CHARS: RangeInclusive<char> = 'a'..='z';
const UPPER_CASE_CHARS: RangeInclusive<char> = 'A'..='Z';

lazy_static::lazy_static! {
    static ref ASCII_CHARS: Vec<char> = {
        let mut ret: Vec<char> = LOWER_CASE_CHARS.chain(UPPER_CASE_CHARS).collect();
        ret.sort_unstable();
        ret
    };
}

#[allow(unused)]
fn is_char_valid(inp: char) -> bool {
    match inp {
        'a'..='z' | 'A'..='Z' => true,
        _ => false,
    }
}

#[inline(always)]
fn generate_valid_ascii_char() -> char {
    let mut rng = thread_rng();
    let chosing_range = 0..ASCII_CHARS.len();
    let chosen_idx = rng.gen_range(chosing_range);
    *ASCII_CHARS.get(chosen_idx).unwrap()
}

pub(crate) fn generate_valid_string(len: usize) -> String {
    let mut ret = String::with_capacity(len);
    for _ in 0..len {
        let character = generate_valid_ascii_char();
        ret.push(character);
    }
    ret
}

#[test]
fn test_generate_valid_ascii_char() {
    let sample_size = ASCII_CHARS.len() * 1_000;
    for _ in 0..sample_size {
        let chosen = generate_valid_ascii_char();
        assert!(is_char_valid(chosen));
    }
}

pub(crate) async fn generate(
    size: usize,
    data_out: PathBuf,
    pattern: ParsePattern,
    key_size: usize,
    value_size: usize,
    compression_level: i32,
) -> IoResult<()> {
    let multi = MultiProgress::new();

    let bytes_style = ProgressStyle::default_spinner()
        .template("SPD: [{binary_bytes_per_sec:>14.magenta}] ELA: [{elapsed:>4.red}] Bytes: [{bytes:.green}] {msg}");
    let bytes_bar = multi.add(ProgressBar::new_spinner());
    bytes_bar.set_style(bytes_style);

    let patterns_style = ProgressStyle::default_bar()
        .template("SPD: [{per_sec:>14.magenta}] ETA: [{eta:>4.red}] {percent:>3.green}% {bar:50.cyan/blue} {pos:>10.yellow}/{len}");
    let patterns_bar = multi.add(ProgressBar::new(size as u64));
    patterns_bar.set_style(patterns_style);

    let multi_progress = tokio::spawn(async move { multi.join() });

    let file = File::create(data_out)?;
    let file_bar = bytes_bar.wrap_write(file);
    let buffered = BufWriter::new(file_bar);
    let compressor = Encoder::new(buffered, compression_level)?;
    let mut buf_comp = BufWriter::new(compressor);

    bytes_bar.println("created file");

    let mut state = BasicState::new();

    for _ in 0..size {
        let gen_pattern = BasicPattern::new(&pattern, key_size, value_size, &mut state);

        let encoded = bincode::serialize(&gen_pattern).unwrap();
        let encoded_pattern_len = encoded.len();
        let m = encoded_pattern_len.to_le_bytes();
        buf_comp.write_all(&m)?;
        buf_comp.flush()?;
        buf_comp.write_all(&encoded)?;
        buf_comp.flush()?;
        patterns_bar.inc(1);
    }

    patterns_bar.finish_with_message("finished generating all patterns");
    bytes_bar.println("flushing compressor buffer");
    buf_comp.flush()?;
    let mut compressor = buf_comp.into_inner()?;
    bytes_bar.println("flushing compressor");
    compressor.flush()?;
    let mut buffered = compressor.finish()?;
    bytes_bar.println("flushing file buffer");
    buffered.flush()?;
    let mut file = buffered.into_inner()?;
    bytes_bar.println("flushing file");
    file.flush()?;
    bytes_bar.finish_with_message("finished writing");

    multi_progress.await??;

    Ok(())
}
