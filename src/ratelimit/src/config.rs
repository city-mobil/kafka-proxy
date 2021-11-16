use crate::Rule;

use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct Config {
    pub rules: std::vec::Vec<Rule>,
    pub enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config::default_config()
    }
}

impl Config {
    fn default_config() -> Self {
        Config {
            rules: vec![],
            enabled: false,
        }
    }

    pub fn get_rules(&self) -> &std::vec::Vec<Rule> {
        self.rules.as_ref()
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }
}
