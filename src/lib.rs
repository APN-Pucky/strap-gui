use std::collections::HashMap;
use std::hash::Hash;
use serde::de::DeserializeOwned;
use serde::{Serialize, Serializer, Deserialize, Deserializer};
use serde::ser::SerializeMap;

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: Deserialize<'de> + Eq + Hash + Clone, V: Deserialize<'de> + Default + Copy"
))]
pub struct StatTrak<K, V> 
{
    children: HashMap<K, Box<StatTrak<K, V>>>,
    value: V,
}

impl<K, V> StatTrak<K, V>
where
    K: Eq + Hash + Clone,
    V: Default + Copy,
{
    pub fn new() -> Self {
        Self {
            children: HashMap::new(),
            value: V::default(),
        }
    }

    pub fn absolute(&mut self, keys : &[K], value : V)
    where
        V: std::ops::Add<Output = V>,
    {
        if let Some((first, rest)) = keys.split_first() {
            let child = self.children.entry(first.clone()).or_insert_with(|| Box::new(Self::new()));
            child.absolute(rest, value);
        } else {
            self.value = value;
        }
    }

    pub fn increment(&mut self, keys: &[K], delta: V)
    where
        V: std::ops::AddAssign,
    {
        if let Some((first, rest)) = keys.split_first() {
            let child = self.children.entry(first.clone()).or_insert_with(|| Box::new(Self::new()));
            child.increment(rest, delta);
        } else {
            self.value += delta;
        }
    }
}

