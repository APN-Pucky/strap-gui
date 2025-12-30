use stattrak::StatTrack;
use std::env;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() != 2 {
        eprintln!("Usage: {} <strap_file>", args[0]);
        std::process::exit(1);
    }
    
    let file_path = &args[1];
    let stats = StatTrack::new(file_path)?;
    
    // Get all column names
    let column_names = stats.get_column_names()?;
    
    if column_names.is_empty() {
        println!("No columns found in STRAP file");
        return Ok(());
    }
    
    println!("Aggregating {} columns from STRAP file: {}", column_names.len(), file_path);
    println!();
    
    // For each column, calculate the sum
    for column_name in &column_names {
        let sum = stats.aggregate(0.0, |acc, row| {
            if let Some(&value) = row.get(column_name) {
                acc + value
            } else {
                acc
            }
        })?;
        
        println!("{}: {}", column_name, sum);
    }
    
    Ok(())
}
