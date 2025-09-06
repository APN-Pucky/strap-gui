use stattrak::StatTrak;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stats: StatTrak<String, u64> = StatTrak::new();
    stats.increment(&["Alice".into()], 3);
    stats.increment(&["Alice".into(), "Sword".into()], 2);
    stats.increment(&["Bob".into(), "Bow".into()], 5);

    // Serialize to YAML string
    let yaml = serde_yaml::to_string(&stats)?;
    println!("Serialized YAML:\n{}", yaml);

    // Deserialize back
    let deserialized: StatTrak<String, u64> = serde_yaml::from_str(&yaml)?;
    println!("Deserialized struct:\n{:?}", deserialized);

    Ok(())
}