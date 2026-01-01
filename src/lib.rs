use std::collections::HashMap;
use std::hash::Hash;

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;

use std::sync::Arc;

use itertools::Itertools;

use arrow::array::{Float64Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Lazy/streaming parser for STRAP protocol files
#[derive(Debug)]
pub struct StrapTrack {
    file_path: PathBuf,
    //data : Vec<HashMap<String, f64>>,

    //cached_column_names: Option<Vec<String>>,
    //cached_columns: HashMap<String, Vec<f64>>,
}

/// Iterator over STRAP file rows
pub struct StrapTrackIterator {
    reader: BufReader<File>,
}

impl Iterator for StrapTrackIterator {
    type Item = Result<HashMap<String, f64>, std::io::Error>;
    
    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => None, // EOF
            Ok(_) => {
                let parsed = StrapTrack::parse_line(&line);
                Some(Ok(parsed))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl StrapTrack {
    pub fn new(file_path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = file_path.into();
        // Verify file exists
        File::open(&path)?;

        
        Ok(Self {
            file_path: path,
        })
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

    
    /// Parse a single STRAP line into key-value pairs
    fn parse_line(line: &str) -> HashMap<String, f64> {
        let mut result = HashMap::new();
        let line = line.trim();
        
        // Handle @strap prefix
        let line = if line.starts_with("@strap1 ") {
            &line[8..]
        } else if line.starts_with("@strap ") {
            &line[7..]
        } else {
            line
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
        
        result
    }
    
    /// Returns an iterator over all rows
    pub fn iter(&self) -> Result<StrapTrackIterator, std::io::Error> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);
        Ok(StrapTrackIterator { reader })
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
        self.for_each_row(| row| {
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

    pub fn to_unchunked_parquet(&self, filename: &str) -> Result<(), Box<dyn std::error::Error>> {


        // 1. Collect all unique column names
        let mut column_names = self.get_column_names()?;

        column_names.sort(); // optional: deterministic column order

        // 2. Build schema
        let fields: Vec<Field> = column_names.iter()
            .map(|name| Field::new(name, DataType::Float64, true)) // nullable = true
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // 3. Build arrays
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for col in &column_names {
            let values: Vec<Option<f64>> = self.iter()?
                .map(|row| row.ok()?.get(col).copied())
                .collect();
            arrays.push(Arc::new(Float64Array::from(values)) as ArrayRef);
        }


        // 4. Build RecordBatch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        // Setup Parquet writer
        let file = File::create(filename)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        println!("Sparse Parquet written!");
        Ok(())
    }

    pub fn to_parquet(&self, filename: &str) -> Result<(), Box<dyn std::error::Error>> {


        // 1. Collect all unique column names
        let mut column_names = self.get_column_names()?;

        column_names.sort(); // optional: deterministic column order

        // 2. Build schema
        let fields: Vec<Field> = column_names.iter()
            .map(|name| Field::new(name, DataType::Float64, true)) // nullable = true
            .collect();
        let schema = Arc::new(Schema::new(fields));


        // Setup Parquet writer
        let file = File::create(filename)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        for vhm in &self.iter()?.chunks(1000) {
            let chunk_data: Result<Vec<_>, _> = vhm.collect();
            let chunk_data = chunk_data?;
            
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
        println!("Sparse Parquet written!");
        Ok(())
    }

}