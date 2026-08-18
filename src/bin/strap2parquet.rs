use std::ffi::OsStr;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure};
use clap::{Arg, Command, value_parser};
use indicatif::{ProgressBar, ProgressStyle};
use tempfile::NamedTempFile;

use straptrack::{
    DEFAULT_PARQUET_CHUNK_SIZE, ParquetProgress, ParquetProgressObserver, ParquetProgressPhase,
    StrapTrack, convert_to_parquet_with_progress, default_parquet_path,
};

struct CliProgressObserver {
    progress_bar: ProgressBar,
}

impl CliProgressObserver {
    fn new() -> Self {
        let progress_bar = ProgressBar::new(1);
        let style = ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} eta {eta_precise} {msg}",
        )
        .expect("progress bar template should be valid")
        .progress_chars("=> ");
        progress_bar.set_style(style);
        progress_bar.enable_steady_tick(Duration::from_millis(100));

        Self { progress_bar }
    }
}

impl ParquetProgressObserver for CliProgressObserver {
    fn on_progress(&self, progress: ParquetProgress) {
        let phase_total = progress.total_bytes.max(1);
        let combined_total = phase_total.saturating_mul(2);
        let phase_position = progress.bytes_read.min(phase_total);
        let combined_position = match progress.phase {
            ParquetProgressPhase::DiscoverColumns => phase_position,
            ParquetProgressPhase::WriteParquet => phase_total.saturating_add(phase_position),
        };

        self.progress_bar.set_length(combined_total);
        self.progress_bar.set_position(combined_position);
        self.progress_bar
            .set_message(progress.phase.label().to_string());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InputSource {
    File(PathBuf),
    Stdin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedCliPaths {
    input: InputSource,
    output_path: PathBuf,
}

struct StagedStdinInput {
    temp_file: NamedTempFile,
    column_names: Vec<String>,
}

impl ResolvedCliPaths {
    fn input_display(&self) -> String {
        match &self.input {
            InputSource::File(path) => path.display().to_string(),
            InputSource::Stdin => "stdin".to_string(),
        }
    }
}

fn is_stdin_marker(path: &Path) -> bool {
    path.as_os_str() == OsStr::new("-")
}

fn resolve_cli_paths(
    input_arg: Option<PathBuf>,
    output_arg: Option<PathBuf>,
    stdin_is_terminal: bool,
) -> anyhow::Result<ResolvedCliPaths> {
    match (input_arg, output_arg) {
        (Some(input_path), Some(output_path)) => {
            if is_stdin_marker(&input_path) {
                Ok(ResolvedCliPaths {
                    input: InputSource::Stdin,
                    output_path,
                })
            } else {
                Ok(ResolvedCliPaths {
                    input: InputSource::File(input_path),
                    output_path,
                })
            }
        }
        (Some(single_path), None) => {
            if is_stdin_marker(&single_path) {
                bail!("output parquet path is required when reading from stdin");
            }

            if stdin_is_terminal {
                Ok(ResolvedCliPaths {
                    output_path: default_parquet_path(&single_path),
                    input: InputSource::File(single_path),
                })
            } else {
                Ok(ResolvedCliPaths {
                    input: InputSource::Stdin,
                    output_path: single_path,
                })
            }
        }
        (None, None) => {
            if stdin_is_terminal {
                bail!("input file is required unless data is piped on stdin");
            } else {
                bail!("output parquet path is required when reading from stdin");
            }
        }
        (None, Some(_)) => unreachable!("clap positional parsing cannot yield only an output"),
    }
}

fn stage_stdin_input() -> anyhow::Result<StagedStdinInput> {
    let mut staged = NamedTempFile::with_suffix(".strap.zst")?;
    let mut stdin = io::stdin().lock();
    let mut line = String::new();
    let mut column_names = std::collections::HashSet::new();
    let mut staged_lines = 0usize;

    {
        let mut encoder = zstd::Encoder::new(staged.as_file_mut(), 0)?;

        loop {
            line.clear();
            let bytes_read = stdin.read_line(&mut line)?;
            if bytes_read == 0 {
                break;
            }

            let Some(payload) = StrapTrack::strap_payload(&line) else {
                continue;
            };
            let payload = payload.trim();
            if payload.is_empty() {
                continue;
            }

            let parsed = StrapTrack::parse_line(payload, true);
            for key in parsed.keys() {
                column_names.insert(key.clone());
            }

            encoder.write_all(payload.as_bytes())?;
            encoder.write_all(b"\n")?;
            staged_lines += 1;
        }

        encoder.finish()?;
    }

    ensure!(staged_lines > 0, "stdin did not contain any @strap lines");
    ensure!(
        !column_names.is_empty(),
        "stdin did not contain any parseable STRAP key/value pairs"
    );

    Ok(StagedStdinInput {
        temp_file: staged,
        column_names: column_names.into_iter().collect(),
    })
}

fn main() -> anyhow::Result<()> {
    let matches = Command::new("strap2parquet")
        .about("Convert STRAP data or @strap-marked logs to parquet")
        .arg(
            Arg::new("input")
                .help("Input .strap/.log file, compressed STRAP file, or - for stdin")
                .required(false)
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("output")
                .help("Output parquet path (required for stdin, defaults to <input>.parquet for files)")
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

    let resolved_paths = resolve_cli_paths(
        matches.get_one::<PathBuf>("input").cloned(),
        matches.get_one::<PathBuf>("output").cloned(),
        io::stdin().is_terminal(),
    )?;
    let chunk_size = *matches
        .get_one::<usize>("chunk-size")
        .unwrap_or(&DEFAULT_PARQUET_CHUNK_SIZE);

    ensure!(chunk_size > 0, "--chunk-size must be greater than 0");

    if let InputSource::File(input_path) = &resolved_paths.input {
        ensure!(
            input_path != &resolved_paths.output_path,
            "output path must differ from input path"
        );
    }

    let mut staged_input = None;
    let mut staged_column_names = None;
    let input_path = match &resolved_paths.input {
        InputSource::File(path) => path.clone(),
        InputSource::Stdin => {
            eprintln!("extracting @strap payloads from stdin into a temporary .strap.zst file...");
            let staged = stage_stdin_input()?;
            let path = staged.temp_file.path().to_path_buf();
            staged_column_names = Some(staged.column_names);
            staged_input = Some(staged.temp_file);
            path
        }
    };

    let progress = Arc::new(CliProgressObserver::new());

    if let Some(column_names) = staged_column_names {
        let track = StrapTrack::new(&input_path).map_err(|err| {
            anyhow!(
                "failed to prepare staged stdin data for {}: {}",
                resolved_paths.output_path.display(),
                err
            )
        })?;

        track
            .to_parquet_with_known_columns_and_progress(
                &resolved_paths.output_path,
                chunk_size,
                Some(progress.clone()),
                column_names,
            )
            .map_err(|err| {
                anyhow!(
                    "failed to convert {} to {}: {}",
                    resolved_paths.input_display(),
                    resolved_paths.output_path.display(),
                    err
                )
            })?;
    } else {
        convert_to_parquet_with_progress(
            &input_path,
            &resolved_paths.output_path,
            chunk_size,
            Some(progress.clone()),
        )
        .map_err(|err| {
            anyhow!(
                "failed to convert {} to {}: {}",
                resolved_paths.input_display(),
                resolved_paths.output_path.display(),
                err
            )
        })?;
    }

    drop(staged_input);

    progress
        .progress_bar
        .finish_with_message(format!("wrote {}", resolved_paths.output_path.display()));

    println!("{}", resolved_paths.output_path.display());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_single_argument_as_input_when_stdin_is_terminal() {
        let resolved = resolve_cli_paths(Some(PathBuf::from("sample.strap")), None, true).unwrap();

        assert_eq!(
            resolved.input,
            InputSource::File(PathBuf::from("sample.strap"))
        );
        assert_eq!(resolved.output_path, PathBuf::from("sample.strap.parquet"));
    }

    #[test]
    fn resolves_single_argument_as_output_when_stdin_is_piped() {
        let resolved =
            resolve_cli_paths(Some(PathBuf::from("output.parquet")), None, false).unwrap();

        assert_eq!(resolved.input, InputSource::Stdin);
        assert_eq!(resolved.output_path, PathBuf::from("output.parquet"));
    }

    #[test]
    fn resolves_dash_as_explicit_stdin() {
        let resolved = resolve_cli_paths(
            Some(PathBuf::from("-")),
            Some(PathBuf::from("output.parquet")),
            true,
        )
        .unwrap();

        assert_eq!(resolved.input, InputSource::Stdin);
        assert_eq!(resolved.output_path, PathBuf::from("output.parquet"));
    }

    #[test]
    fn requires_output_when_reading_from_stdin() {
        let error = resolve_cli_paths(None, None, false).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("output parquet path is required when reading from stdin")
        );
    }
}
