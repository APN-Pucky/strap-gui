use clap::{Arg, Command};
use anyhow::{Context, Result};
use std::path::Path;
use straptrack::StrapTrack;

fn main() -> Result<()> {
    let matches = Command::new("strap2parquet")
        .version("1.0")
        .author("Your Name")
        .about("Converts STRAP files to Parquet format using StrapTrack")
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .value_name("INPUT_FILE")
                .help("Input STRAP file path")
                .required(true)
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("OUTPUT_FILE")
                .help("Output Parquet file path")
                .required(true)
        )
        .arg(
            Arg::new("chunk_size")
                .long("chunk-size")
                .value_name("SIZE")
                .help("Chunk size for processing (default: 1000)")
                .value_parser(clap::value_parser!(usize))
                .default_value("1000")
        )
        .get_matches();

    let input_path = matches.get_one::<String>("input").unwrap();
    let output_path = matches.get_one::<String>("output").unwrap();
    let chunk_size = *matches.get_one::<usize>("chunk_size").unwrap();

    if !Path::new(input_path).exists() {
        anyhow::bail!("Input file does not exist: {}", input_path);
    }

    println!("Converting {} to {} (chunks of {})", 
             input_path, 
             output_path,
             chunk_size
             );

    let strap_track = StrapTrack::new(input_path)
        .with_context(|| format!("Failed to open STRAP file: {}", input_path))?;

    if strap_track.to_parquet(output_path, chunk_size).is_err() {
        anyhow::bail!("Failed to convert STRAP to Parquet");
    }
    println!("Conversion completed successfully!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_conversion() {
        // Create a temporary STRAP file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "@strap ts 1640995200.0 price 150.25 volume 1000.0").unwrap();
        writeln!(temp_file, "@strap ts 1640995260.0 price 151.00 volume 500.0").unwrap();
        temp_file.flush().unwrap();

        // Test that we can create a StrapTrack from it
        let strap_track = StrapTrack::new(temp_file.path()).unwrap();
        
        // Verify we can get column names
        let columns = strap_track.get_column_names().unwrap();
        assert!(!columns.is_empty());
        
        // Test conversion to parquet (in-memory, won't actually write)
        let temp_parquet = tempfile::NamedTempFile::new().unwrap();
        let result = strap_track.to_parquet(temp_parquet.path().to_str().unwrap(), 1000);
        assert!(result.is_ok());
    }
}
