use crate::kafka;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use yaml_rust::{Yaml, YamlLoader};

pub struct Config {
    config_path: String,
    output_file: String,
    http_config: HttpConfig,
    kafka_config: kafka::kafka::config::Config,
}

impl Config {
    pub fn new(arg_matches: clap::ArgMatches) -> Config {
        let config_file = arg_matches.value_of("config").unwrap();
        // NOTE(a.petrukhin): default initialization.
        return Config {
            config_path: String::from(config_file),
            output_file: String::from(""),
            http_config: HttpConfig {
                port: DEFAULT_HTTP_PORT,
                metrics_port: DEFAULT_METRICS_PORT,
            },
            kafka_config: kafka::kafka::config::Config::new_empty(),
        };
    }

    pub fn prepare(&mut self) {
        if self.config_path == "" {
            panic!("No configuration file found");
        }

        println!("Using file {}", self.config_path);

        let path = std::path::Path::new(&self.config_path);
        let mut file = match File::open(&path) {
            Err(why) => panic!(
                "Could not open {}: {}",
                path.display(),
                why.source().unwrap(),
            ),
            Ok(file) => file,
        };

        let mut s = String::new();
        match file.read_to_string(&mut s) {
            Err(why) => panic!("Could not read file to string: {}", why.source().unwrap()),
            Ok(_) => {}
        }

        // NOTE(a.petrukhin): some copypaste from the internet :)
        let docs = YamlLoader::load_from_str(&s).unwrap();
        let doc = &docs[0].as_hash().unwrap();

        let output_file_op = doc.get(&Yaml::from_str("output_file"));
        let mut output_file = "";
        match output_file_op {
            Some(str) => output_file = str.as_str().unwrap(),
            None => (),
        }
        self.output_file = String::from(output_file);

        let kafka = doc.get(&Yaml::from_str("kafka"));
        let http = doc.get(&Yaml::from_str("http"));

        let http_config = HttpConfig::new_from_yaml(&http);
        self.http_config = http_config;

        self.kafka_config = kafka::kafka::config::Config::new_from_yaml(&kafka);
    }

    pub fn get_http_config(&self) -> HttpConfig {
        self.http_config.clone()
    }

    pub fn get_output_file(&self) -> String {
        self.output_file.clone()
    }

    pub fn get_kafka_config(&self) -> kafka::kafka::config::Config {
        self.kafka_config.clone()
    }
}

#[derive(Clone)]
pub struct HttpConfig {
    port: u16,
    metrics_port: u16,
}

const DEFAULT_HTTP_PORT: u16 = 4242;
const DEFAULT_METRICS_PORT: u16 = 8088;

impl HttpConfig {
    pub fn new_from_yaml(yml: &Option<&Yaml>) -> HttpConfig {
        let hash = yml.unwrap().as_hash().unwrap();
        let port = hash.get(&Yaml::from_str("port")).unwrap().as_i64().unwrap();

        let metrics_port_raw = hash.get(&Yaml::from_str("metrics_port"));
        let mut metrics_port = DEFAULT_METRICS_PORT;
        if metrics_port_raw.is_some() {
            metrics_port = metrics_port_raw.unwrap().as_i64().unwrap() as u16;
        }

        return HttpConfig {
            port: port as u16,
            metrics_port,
        };
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn metrics_port(&self) -> u16 {
        self.metrics_port
    }
}
