use crate::Rule;

use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct Config {
    rules: std::vec::Vec<Rule>,
}

impl Default for Config {
    fn default() -> Self {
        Config::default_config()
    }
}

impl Config {
    fn default_config() -> Self {
        Config { rules: vec![] }
    }
}
