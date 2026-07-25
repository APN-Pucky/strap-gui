use std::path::PathBuf;

use anyhow::{anyhow, ensure};
use clap::{Arg, Command, value_parser};

use straptrack::{DEFAULT_PARQUET_CHUNK_SIZE, convert_to_parquet, default_parquet_path};

fn main() -> anyhow::Result<()> {
    let matches = Command::new("strap2parquet")
        .about("Convert a .strap or compressed .strap file to parquet")
        .arg(
            Arg::new("input")
                .help("Input .strap, .strap.gz, .strap.zst, or .strap.zip file")
                .required(true)
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("output")
                .help("Output parquet path (defaults to <input>.parquet)")
                .required(false)
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("chunk-size")
                .long("chunk-size")
                .help("Number of rows to buffer per parquet batch")
                .value_parser(value_parser!(usize))
                .default_value("1000"),
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

    ensure!(chunk_size > 0, "--chunk-size must be greater than 0");
    ensure!(
        input_path != output_path,
        "output path must differ from input path"
    );

    convert_to_parquet(&input_path, &output_path, chunk_size).map_err(|err| {
        anyhow!(
            "failed to convert {} to {}: {}",
            input_path.display(),
            output_path.display(),
            err
        )
    })?;

    println!("{}", output_path.display());

    Ok(())
}
