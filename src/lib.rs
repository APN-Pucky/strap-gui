use std::collections::HashMap;
use std::hash::Hash;

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;

use std::sync::Arc;

use arrow::array::{Float64Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Lazy/streaming parser for STRAP protocol files
#[derive(Debug)]
pub struct StatTrack {
    //file_path: PathBuf,
    data : Vec<HashMap<String, f64>>,

    cached_column_names: Option<Vec<String>>,
    cached_columns: HashMap<String, Vec<f64>>,
}

impl StatTrack {
    pub fn new(file_path: impl Into<PathBuf>) -> std::io::Result<Self> {
        println!("Loading STRAP file: ");
        let mut data = Vec::new();
        let path = file_path.into();
        // Verify file exists
        File::open(&path)?;

        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        
        for line in reader.lines() {
            let line = line?;
            let parsed = Self::parse_line(&line);
            data.push(parsed.clone());
        }
        println!("Loaded {} rows", data.len());
        
        Ok(Self {
            data,
            cached_column_names: None,
            cached_columns: HashMap::new(),
        })
    }

    /// Get column names from all rows
    pub fn get_column_names(&mut self) -> &Vec<String> {
        if self.cached_column_names.is_none() {
            let unique_keys = std::collections::HashSet::new();

            let ret = self.aggregate(unique_keys, |mut set, hm| {
                set.extend(hm.keys().cloned());
                set
            }).into_iter().collect();
            
            self.cached_column_names = Some(ret);
        }
        self.cached_column_names.as_ref().unwrap()
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
    
    /// Get a specific row as HashMap
    pub fn get_row(&self, row_index: usize) -> Option<HashMap<String, f64>> {
        for (i, parsed) in self.data.iter().enumerate() {
            if i == row_index {
                return Some(parsed.clone());
            }
        }
        None
    }
    
    /// Get all values for a specific column (streaming)
    pub fn get_column(&mut self, column_name: &str) -> Vec<f64> {
        if ! self.cached_columns.contains_key(column_name) {
            let mut values = Vec::new();
            
            for parsed in self.data.iter() {
                if let Some(&value) = parsed.get(column_name) {
                    values.push(value);
                }
            }
            self.cached_columns.insert(column_name.to_string(), values);
        }
        self.cached_columns.get(column_name).unwrap().clone()
    }
    
    /// Stream through all rows with a callback
    pub fn for_each_row<F>(&self, mut callback: F)
    where
        F: FnMut(&HashMap<String, f64>) -> bool, // return false to stop
    {
        for parsed in self.data.iter() {
            if !callback(parsed) {
                break;
            }
        }
    }
    
    /// Filter rows based on a predicate
    pub fn filter_rows<F>(&self, predicate: F) -> Vec<HashMap<String, f64>>
    where
        F: Fn(&HashMap<String, f64>) -> bool,
    {
        let mut results = Vec::new();
        self.for_each_row(| row| {
            if predicate(&row) {
                results.push(row.clone());
            }
            true // continue
        });
        results
    }
    
    /// Aggregate a column with a reduction function
    pub fn aggregate<F, T>(&self, init: T, reducer: F) -> T
    where
        F: Fn(T, &HashMap<String, f64>) -> T,
    {
        let mut acc = init;
        
        for parsed in self.data.iter() {
            acc = reducer(acc, parsed);
        }
        
        acc
    }

    pub fn to_parquet(&self, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        // 1. Collect all unique column names
        let mut column_names = self.data.iter()
            .flat_map(|row| row.keys())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();

        column_names.sort(); // optional: deterministic column order

        // 2. Build arrays
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for col in &column_names {
            let values: Vec<Option<f64>> = self.data.iter()
                .map(|row| row.get(col).copied())
                .collect();
            arrays.push(Arc::new(Float64Array::from(values)) as ArrayRef);
        }

        // 3. Build schema
        let fields: Vec<Field> = column_names.iter()
            .map(|name| Field::new(name, DataType::Float64, true)) // nullable = true
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // 4. Build RecordBatch
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        // 5. Write Parquet
        let file = File::create(filename)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        println!("Sparse Parquet written!");
        Ok(())
    }

}