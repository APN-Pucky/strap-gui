use stattrak::StatTrak;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read file
    let decoded = StatTrak::<String, u64>::read_bin("stats.bin")?;
    println!("{:#?}", decoded);

    Ok(())
}
