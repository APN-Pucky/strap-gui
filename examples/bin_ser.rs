use stattrak::StatTrak;


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stats: StatTrak<String, u64> = StatTrak::new();

    stats.increment(&["Alice".into(), "Sword".into()], 2);
    stats.increment(&["Bob".into(), "Bow".into()], 5);

    stats.write_bin("stats.bin")?;

    Ok(())
}