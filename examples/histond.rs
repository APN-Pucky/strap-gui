use stattrak::StatTrak;

pub struct LinearBinner {
    pub min: f64,
    pub max: f64,
    pub bins: usize,
}

impl LinearBinner {
    pub fn bin(&self, value: f64) -> usize {
        if value <= self.min {
            0
        } else if value >= self.max {
            self.bins - 1
        } else {
            let f = (value - self.min) / (self.max - self.min);
            (f * self.bins as f64) as usize
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let binner = LinearBinner { min: 0.0, max: 1.0, bins: 10 };
    let mut hist = StatTrak::<usize, u64>::new();

    // Example data points (4D)
    let data = vec![
        [0.1, 0.2, 0.3, 0.4],
        [0.15, 0.25, 0.35, 0.45],
        [0.8, 0.9, 0.95, 0.99],
        [0.1, 0.2, 0.3, 0.4],
    ];

    for point in data {
        let bins: Vec<usize> = point.iter().map(|&x| binner.bin(x)).collect();
        hist.increment(&bins, 1);
    }

    println!("{:#?}", hist);
    let yaml = serde_yaml::to_string(&hist)?;
    println!("Serialized YAML:\n{}", yaml);

    Ok(())
}
