use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Record {
    pub topic: String,
    pub data: String,
    pub key: Option<String>
}

impl Record {
    pub fn new(topic: String, data: String, key: Option<String>) -> Record {
        Record { topic, data, key }
    }
}