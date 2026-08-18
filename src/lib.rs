use std::collections::HashMap;

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use std::sync::Arc;

use flate2::bufread::GzDecoder;
use itertools::Itertools;

use arrow::array::{Float64Array, ArrayRef, new_null_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;
use zip::ZipArchive;
use zstd::stream::read::Decoder as ZstdDecoder;

pub const DEFAULT_PARQUET_CHUNK_SIZE: usize = 2048;


/// Parse a single STRAP line into key-value pairs
fn parse_line(line: &str, all: bool) -> Option<HashMap<String, f64>> {
    let mut result = HashMap::new();
    let line = line.trim();

    // Handle @strap prefix - find first occurrence and continue from there
    let line = if let Some(pos) = line.find("@strap") {
        // Skip past "@strap" and any following digit/space
        let after_strap = &line[pos..]; // Skip "@strap"
        if let Some(pos) = after_strap.find(char::is_whitespace) {
            &after_strap[pos..]
        } else {
            after_strap
        }
        .trim_start() // Remove any leading whitespace
    } else {
        if all {
            line
        } else {
            return None; // Empty
        }
    };

    // Parse key-value pairs separated by whitespace
    let tokens: Vec<&str> = line.split_whitespace().collect();
    for chunk in tokens.chunks(2) {
        if chunk.len() == 2 {
            if let Ok(value) = chunk[1].parse::<f64>() {
                result.insert(chunk[0].to_string(), value);
            }
        }
    }

    Some(result)
}

pub fn default_parquet_path(path: impl AsRef<Path>) -> PathBuf {
    let mut output = path.as_ref().as_os_str().to_os_string();
    output.push(".parquet");
    PathBuf::from(output)
}

pub fn convert_to_parquet(
    input_path: impl AsRef<Path>,
    output_path: impl AsRef<Path>,
    chunk_size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let track = StrapTrack::new(input_path.as_ref())?;
    track.to_parquet(output_path.as_ref(), chunk_size)
}
/// Iterator over STRAP file rows
pub struct StrapIterator<R: BufRead> {
    all:bool,
    reader: R,
}

impl<R: BufRead> Iterator for StrapIterator<R> {
    type Item = Result<HashMap<String, f64>, std::io::Error>;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    let parsed = parse_line(&line, self.all);
                    if let Some(p) = parsed {
                        return Some(Ok(p));
                    }
                    // continue until we find a valid line
                }
                Err(e) => return Some(Err(e)),
            };
        }
    }
}

pub fn strap_to_parquet<R: BufRead>(
    reader: R,
    all: bool,
    filename: &str, // only filename, since we will write temp files
    chunk_size: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // changing when the schema changes:
    let mut column_names = std::collections::HashSet::new();
    let (mut schema, mut writer) = StrapTrack::make_new_schema_and_writer(column_names.clone(), filename, 0)?;
    let mut schema_changes = 0;


    let iter = StrapIterator { all, reader };
    for vhm in &iter.chunks(chunk_size) {
        let chunk_data: Result<Vec<_>, _> = vhm.collect();
        let chunk_data = chunk_data?;
        let mut found_new_columns = false;
        for hm in &chunk_data {
            for key in hm.keys() {
                if column_names.insert(key.clone()) {
                    found_new_columns = true;
                }
            }
        }
        if found_new_columns {
            schema_changes += 1;
            writer.close()?;
            (schema,writer) = StrapTrack::make_new_schema_and_writer(column_names.clone(), filename, schema_changes)?;
        }
                    
        // 3. Build arrays
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for col in &column_names {
            let values: Vec<Option<f64>> = chunk_data.iter()
                .map(|row| row.get(col).copied())
                .collect();
            arrays.push(Arc::new(Float64Array::from(values)) as ArrayRef);
        }


        // 4. Build RecordBatch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        // 5. Write Parquet
        writer.write(&batch)?;
    }
    writer.close()?;

    if schema_changes == 1 {
        // if there was only one schema change, we can just rename the temp file to the final file
        std::fs::rename(StrapTrack::name_parquet_file(filename, 1), filename)?;
        return Ok(());
    }

    // now we merge the temp files into one final file

    (schema,writer) = StrapTrack::make_new_schema_and_writer(column_names.clone(), filename, 0)?;
    for i in 1..=schema_changes {
        let file = File::open(StrapTrack::name_parquet_file(filename, i))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .build()?;

        for batch in reader {
            let batch = batch?;

            let arrays: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .map(|field| {
                    match batch.schema().index_of(field.name()) {
                        Ok(index) => batch.column(index).clone(),

                        Err(_) => {
                            new_null_array(
                                field.data_type(),
                                batch.num_rows(),
                            )
                        }
                    }
                })
                .collect();

            let batch =
                RecordBatch::try_new(schema.clone(), arrays)?;

            writer.write(&batch)?;
        }
        // delete the temp file
        std::fs::remove_file(StrapTrack::name_parquet_file(filename, i))?;
    }

    writer.close()?;


    Ok(())
}



/// Iterator over STRAP file rows
pub struct StrapTrackIterator {
    all: bool,
    reader: Box<dyn BufRead>,
}

impl Iterator for StrapTrackIterator {
    type Item = Result<HashMap<String, f64>, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut line = String::new();
            match self.reader.read_line(&mut line) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    let parsed = parse_line(&line, self.all);
                    if let Some(p) = parsed {
                        return Some(Ok(p));
                    }
                    // continue until we find a valid line
                }
                Err(e) => return Some(Err(e)),
            };
        }
    }
}

/////////////////////////////////////////////
/// Everything below is legacy code that is not used in the new streaming implementation, but is kept for reference and testing.

/// Lazy/streaming parser for STRAP protocol files
#[derive(Debug)]
pub struct StrapTrack {
    file_path: PathBuf,
    //data : Vec<HashMap<String, f64>>,

    //cached_column_names: Option<Vec<String>>,
    //cached_columns: HashMap<String, Vec<f64>>,
}

impl StrapTrack {
    pub fn new(file_path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = file_path.into();
        // Verify file exists
        File::open(&path)?;

        Ok(Self { file_path: path })
    }

    /// Create a reader that handles compression based on file extension
    fn create_reader(&self) -> Result<Box<dyn BufRead>, std::io::Error> {
        let file = File::open(&self.file_path)?;
        let path_str = self.file_path.to_string_lossy().to_lowercase();

        if path_str.ends_with(".gz") || path_str.ends_with(".gzip") {
            // Gzip compressed
            let decoder = GzDecoder::new(BufReader::new(file));
            Ok(Box::new(BufReader::new(decoder)))
        } else if path_str.ends_with(".zst") || path_str.ends_with(".zstd") {
            // Zstd compressed
            let decoder = ZstdDecoder::new(file)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            Ok(Box::new(BufReader::new(decoder)))
        } else if path_str.ends_with(".zip") {
            // ZIP archive - read first entry into memory
            let mut archive = ZipArchive::new(file)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            if archive.len() == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "ZIP archive is empty",
                ));
            }

            // Read the first file in the archive into memory
            let mut zip_file = archive
                .by_index(0)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let mut contents = Vec::new();
            std::io::copy(&mut zip_file, &mut contents)?;
            Ok(Box::new(BufReader::new(std::io::Cursor::new(contents))))
        } else {
            // Uncompressed
            Ok(Box::new(BufReader::new(file)))
        }
    }

    /// Get column names from all rows
    pub fn get_column_names(&self) -> Result<Vec<String>, std::io::Error> {
        let mut unique_keys = std::collections::HashSet::new();

        for hm in self.iter()? {
            for key in hm?.keys() {
                unique_keys.insert(key.clone());
            }
        }
        Ok(unique_keys.into_iter().collect())
    }


    pub fn is_all(&self) -> bool {
        let path_str = self.file_path.to_string_lossy().to_lowercase();
        path_str.ends_with(".strap") || path_str.ends_with(".strap.gz") || path_str.ends_with(".strap.gzip") || path_str.ends_with(".strap.zst") || path_str.ends_with(".strap.zstd") || path_str.ends_with(".strap.zip")
    }

    /// Returns an iterator over all rows
    pub fn iter(&self) -> Result<StrapTrackIterator, std::io::Error> {
        // check if file name contains .strap or .strap.gz etc
        let all = self.is_all();
        let reader = self.create_reader()?;
        Ok(StrapTrackIterator { all, reader })
    }

    /// Stream through all rows with a callback
    pub fn for_each_row<F>(&self, mut callback: F) -> Result<(), std::io::Error>
    where
        F: FnMut(&HashMap<String, f64>) -> bool, // return false to stop
    {
        for parsed in self.iter()? {
            if !callback(&parsed?) {
                break;
            }
        }
        Ok(())
    }

    /// Filter rows based on a predicate
    pub fn filter_rows<F>(&self, predicate: F) -> Result<Vec<HashMap<String, f64>>, std::io::Error>
    where
        F: Fn(&HashMap<String, f64>) -> bool,
    {
        let mut results = Vec::new();
        self.for_each_row(|row| {
            if predicate(&row) {
                results.push(row.clone());
            }
            true // continue
        })?;
        Ok(results)
    }

    /// Aggregate a column with a reduction function
    pub fn aggregate<F, T>(&self, init: T, reducer: F) -> Result<T, std::io::Error>
    where
        F: Fn(T, &HashMap<String, f64>) -> T,
    {
        let mut acc = init;

        for hm in self.iter()? {
            acc = reducer(acc, &hm?);
        }

        Ok(acc)
    }

    pub fn name_parquet_file(filename: &str, schema_changes: usize) -> String {
        if schema_changes == 0 {
            return filename.to_string();
        }
        format!("temp_{}_{}", filename, schema_changes)
    }

    pub fn make_new_schema_and_writer(
        column_names: std::collections::HashSet<String>,
        filename: &str,
        schema_changes: usize,
    ) -> Result<(Arc<Schema>, ArrowWriter<File>), Box<dyn std::error::Error + Send + Sync>> {
        let mut column_names: Vec<_> = column_names.into_iter().collect();
        column_names.sort();
        // 2. Build new schema
        let fields: Vec<Field> = column_names.iter()
            .map(|name| Field::new(name, DataType::Float64, true)) // nullable = true
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Setup Parquet writer
        let file = File::create(StrapTrack::name_parquet_file(filename, schema_changes))?;
        let props = WriterProperties::builder().build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        return Ok((schema, writer));
    }


    /// Convert STRAP data to Parquet format
    pub fn to_parquet(
        &self,
        filename: impl AsRef<Path>,
        chunk_size: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // call strap_to_parquet with the reader
        let reader = self.create_reader()?;
        let filen: &str = filename.as_ref().file_name().unwrap().to_str().unwrap();
        strap_to_parquet(reader, self.is_all(), filen, chunk_size)
    }

    /// Convert STRAP data to Parquet format
    pub fn old_to_parquet(
        &self,
        filename: impl AsRef<Path>,
        chunk_size: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 1. Collect all unique column names
        let mut column_names = self.get_column_names()?;

        column_names.sort(); // optional: deterministic column order

        // 2. Build schema
        let fields: Vec<Field> = column_names
            .iter()
            .map(|name| Field::new(name, DataType::Float64, true)) // nullable = true
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Setup Parquet writer
        let file = File::create(filename.as_ref())?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        for vhm in &self.iter()?.chunks(chunk_size) {
            let chunk_data: Result<Vec<_>, _> = vhm.collect();
            let chunk_data = chunk_data?;

            // 3. Build arrays
            let mut arrays: Vec<ArrayRef> = Vec::new();
            for col in &column_names {
                let values: Vec<Option<f64>> =
                    chunk_data.iter().map(|row| row.get(col).copied()).collect();
                arrays.push(Arc::new(Float64Array::from(values)) as ArrayRef);
            }

            // 4. Build RecordBatch
            let batch = RecordBatch::try_new(schema.clone(), arrays)?;
            // 5. Write Parquet
            writer.write(&batch)?;
        }
        writer.close()?;
        println!("Sparse Parquet written!");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_file(suffix: &str, content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::with_suffix(suffix).unwrap();
        write!(file, "{}", content).unwrap();
        file
    }

    #[test]
    fn test_parse_simple_line() {
        let result = parse_line("alice_sword 2.2 bob_bow 5.0", true).unwrap();
        assert_eq!(result.get("alice_sword"), Some(&2.2));
        assert_eq!(result.get("bob_bow"), Some(&5.0));
        let result = parse_line("alice_sword 2.2 bob_bow 5.0", false);
        // assert empty since no @strap prefix
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_strap_prefix() {
        let result = parse_line("@strap damage 15.0 attacker_alice 1.0", true).unwrap();
        assert_eq!(result.get("damage"), Some(&15.0));
        assert_eq!(result.get("attacker_alice"), Some(&1.0));
        let result = parse_line("@strap damage 15.0 attacker_alice 1.0", false).unwrap();
        assert_eq!(result.get("damage"), Some(&15.0));
        assert_eq!(result.get("attacker_alice"), Some(&1.0));
    }

    #[test]
    fn test_parse_strap1_prefix() {
        let result = parse_line("@strap1 line 5.0", true).unwrap();
        assert_eq!(result.get("line"), Some(&5.0));
        let result = parse_line("@strap1 line 5.0", false).unwrap();
        assert_eq!(result.get("line"), Some(&5.0));
    }

    #[test]
    fn test_parse_strap_with_metadata() {
        let result = parse_line(
            "DATE TIME OR OTHER_METADATA @strap damage 15.0 attacker_alice 1.0 defender_bob 1.0",
            false,
        ).unwrap();
        assert_eq!(result.get("damage"), Some(&15.0));
        assert_eq!(result.get("attacker_alice"), Some(&1.0));
        assert_eq!(result.get("defender_bob"), Some(&1.0));
    }

    #[test]
    fn test_parse_empty_line() {
        let result = parse_line("", true).unwrap();
        assert!(result.is_empty());
        let result = parse_line("", false);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_whitespace_only() {
        let result = parse_line("   \t  ", true).unwrap();
        assert!(result.is_empty());
        let result = parse_line("   \t  ", false);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_odd_number_tokens() {
        let result = parse_line("key1 1.0 key2", true).unwrap();
        assert_eq!(result.get("key1"), Some(&1.0));
        assert!(!result.contains_key("key2"));
        let result = parse_line("key1 1.0 key2", false);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_invalid_float() {
        let result = parse_line("key1 invalid_float key2 2.0", true).unwrap();
        assert!(!result.contains_key("key1"));
        assert_eq!(result.get("key2"), Some(&2.0));
        let result = parse_line("key1 invalid_float key2 2.0", false);
        assert!(result.is_none());
    }

    #[test]
    fn test_iterator() {
        let content = "alice_sword 2.2 bob_bow 5.0\ndamage 2.0 attacker_alice 1.0\n";
        let file = create_test_file(".strap", content);
        let track = StrapTrack::new(file.path()).unwrap();

        let rows: Result<Vec<_>, _> = track.iter().unwrap().collect();
        let rows = rows.unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("alice_sword"), Some(&2.2));
        assert_eq!(rows[1].get("damage"), Some(&2.0));
    }

    #[test]
    fn test_get_column_names() {
        let content = "a 1.0 b 2.0\nc 3.0 d 4.0\na 5.0 e 6.0\n";
        let file = create_test_file(".strap", content);
        let track = StrapTrack::new(file.path()).unwrap();

        let mut columns = track.get_column_names().unwrap();
        columns.sort();

        assert_eq!(columns, vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn test_filter_rows() {
        let content = "type 1.0 value 10.0\ntype 2.0 value 20.0\ntype 1.0 value 15.0\n";
        let file = create_test_file(".strap", content);
        let track = StrapTrack::new(file.path()).unwrap();

        let filtered = track
            .filter_rows(|row| row.get("type") == Some(&1.0))
            .unwrap();

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].get("value"), Some(&10.0));
        assert_eq!(filtered[1].get("value"), Some(&15.0));
    }

    #[test]
    fn test_aggregate() {
        let content = "@strap value 10.0\n@strap value 20.0\n@strap value 15.0\n";
        let file = create_test_file(".log", content);
        let track = StrapTrack::new(file.path()).unwrap();

        let sum = track
            .aggregate(0.0, |acc, row| acc + row.get("value").unwrap_or(&0.0))
            .unwrap();

        assert_eq!(sum, 45.0);
    }

    #[test]
    fn test_mixed_strap_formats() {
        let content = "@strap a 1.0\n@strap1 b 2.0\nNOISE @strap c 3.0\nregular 4.0\n";
        let file = create_test_file(".strap", content);
        let track = StrapTrack::new(file.path()).unwrap();

        let rows: Result<Vec<_>, _> = track.iter().unwrap().collect();
        let rows = rows.unwrap();

        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].get("a"), Some(&1.0));
        assert_eq!(rows[1].get("b"), Some(&2.0));
        assert_eq!(rows[2].get("c"), Some(&3.0));
        assert_eq!(rows[3].get("regular"), Some(&4.0));
    }

    #[test]
    fn test_strap_with_digits() {
        let result = parse_line("@strap2 key 1.0", false).unwrap();
        assert_eq!(result.get("key"), Some(&1.0));
    }

    #[test]
    fn test_scientific_notation() {
        let result = parse_line("temp 3.14e2 pressure 1.01e5", true).unwrap();
        assert_eq!(result.get("temp"), Some(&314.0));
        assert_eq!(result.get("pressure"), Some(&101000.0));
    }

    #[test]
    fn test_negative_values() {
        let result = parse_line("deficit -42.5 surplus 100.0", true).unwrap();
        assert_eq!(result.get("deficit"), Some(&-42.5));
        assert_eq!(result.get("surplus"), Some(&100.0));
    }

    #[test]
    fn test_default_parquet_path_appends_extension() {
        let output = default_parquet_path("/tmp/example.strap.gz");
        assert_eq!(output, PathBuf::from("/tmp/example.strap.gz.parquet"));
    }

    #[test]
    fn test_convert_to_parquet_writes_output_file() {
        let input = create_test_file(".strap", "@strap value 10.0\n@strap value 20.0\n");
        let temp_dir = tempfile::tempdir().unwrap();
        let output = temp_dir.path().join("converted.parquet");

        convert_to_parquet(input.path(), &output, 2).unwrap();

        let metadata = std::fs::metadata(output).unwrap();
        assert!(metadata.len() > 0);
    }
}
