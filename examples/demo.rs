use stattrak::StatTrak;

fn main() {
    let mut map = StatTrak::<String, u64>::new();
    map.increment(&["Bob".into()], 1);
    map.increment(&["Alice".into(), "Level1".into(), "Sword".into()], 1);
    map.increment(&["Alice".into(), "Level1".into(), "Sword".into()], 1);
    map.increment(&["Bob".into(), "Level2".into(), "Bow".into()], 1);

    println!("{:#?}", map);
}
