use std::{ffi::OsStr, io::BufReader, path::{Path, PathBuf}};

use anyhow::{anyhow, ensure};
use clap::{Arg, Command, value_parser};

use straptrack::{DEFAULT_PARQUET_CHUNK_SIZE, convert_to_parquet, default_parquet_path, strap_to_parquet};

fn is_stdin_marker(path: &Path) -> bool {
    path.as_os_str() == OsStr::new("-")
}

fn main() -> anyhow::Result<()> {
    let matches = Command::new("strap2parquet")
        .about("Convert a .strap or compressed .strap file to parquet")
        .arg(
            Arg::new("input")
                .help("Input .strap/.log file, compressed STRAP file, or - for stdin")
                .default_value("-")
                .required(false)
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("output")
                .help("Output parquet path (defaults to <input>.parquet)")
                .required_if_eq("input", "-")
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("chunk-size")
                .long("chunk-size")
                .help("Number of rows to buffer per parquet batch")
                .value_parser(value_parser!(usize))
                .default_value("2048"),
        )
        .arg(
            Arg::new("all")
                .long("all")
                .help("Include all lines, even those without @strap prefix")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    let input_path = matches
        .get_one::<PathBuf>("input")
        .expect("required by clap")
        .clone();
    let output_path = matches
        .get_one::<PathBuf>("output")
        .cloned()
        .unwrap_or_else(|| default_parquet_path(&input_path));
    let chunk_size = *matches
        .get_one::<usize>("chunk-size")
        .unwrap_or(&DEFAULT_PARQUET_CHUNK_SIZE);
    let all : bool = *matches.get_one::<bool>("all").unwrap_or(&false);

    ensure!(chunk_size > 0, "--chunk-size must be greater than 0");
    ensure!(
        input_path != output_path,
        "output path must differ from input path"
    );
    if is_stdin_marker(&input_path) {
        strap_to_parquet(
            BufReader::new(std::io::stdin()),
            all,
            output_path.to_string_lossy().as_ref(),
            chunk_size,
        ).map_err(|err| {
            anyhow!(
                "failed to convert stdin to {}: {}",
                output_path.display(),
                err
            )
        })?;
    }
    else {
        convert_to_parquet(&input_path, &output_path, chunk_size).map_err(|err| {
            anyhow!(
                "failed to convert {} to {}: {}",
                input_path.display(),
                output_path.display(),
                err
            )
        })?;
    }


    println!("{}", output_path.display());

    Ok(())
}
