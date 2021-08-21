use crate::kafka;
use config::{Config, ConfigError};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct KafkaProxyConfig {
    #[serde(default)]
    output_file: String,

    #[serde(default)]
    http: HttpConfig,

    #[serde(default)]
    kafka: kafka::kafka::config::KafkaConfig,
}

impl KafkaProxyConfig {
    pub fn new(arg_matches: clap::ArgMatches) -> KafkaProxyConfig {
        let config_file = arg_matches.value_of("config").unwrap();
        // NOTE(a.petrukhin): default initialization.
        let config = KafkaProxyConfig::initialize_config(&String::from(config_file));
        if config.is_err() {
            panic!(
                "failed to initialize config: {}",
                config.err().unwrap().to_string()
            );
        }
        return config.unwrap();
    }

    fn initialize_config(config_path: &String) -> Result<Self, ConfigError> {
        let mut cfg = Config::default();

        let merge_result = cfg.merge(config::File::with_name(config_path));
        if merge_result.is_err() {
            return Err(merge_result.err().unwrap());
        }

        cfg.try_into()
    }

    pub fn get_http_config(&self) -> HttpConfig {
        self.http.clone()
    }

    pub fn get_output_file(&self) -> String {
        self.output_file.clone()
    }

    pub fn get_kafka_config(&self) -> kafka::kafka::config::KafkaConfig {
        self.kafka.clone()
    }
}

#[derive(Clone, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "HttpConfig::default_http_port")]
    port: Option<u16>,

    #[serde(default = "HttpConfig::default_metrics_port")]
    metrics_port: Option<u16>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        HttpConfig {
            metrics_port: Some(HttpConfig::DEFAULT_METRICS_PORT),
            port: Some(HttpConfig::DEFAULT_HTTP_PORT),
        }
    }
}

impl HttpConfig {
    const DEFAULT_HTTP_PORT: u16 = 4242;

    const DEFAULT_METRICS_PORT: u16 = 8088;

    pub fn port(&self) -> u16 {
        self.port.unwrap()
    }

    pub fn metrics_port(&self) -> u16 {
        self.metrics_port.unwrap()
    }

    fn default_http_port() -> Option<u16> {
        Some(HttpConfig::DEFAULT_HTTP_PORT)
    }

    fn default_metrics_port() -> Option<u16> {
        Some(HttpConfig::DEFAULT_METRICS_PORT)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::KafkaProxyConfig;

    #[test]
    fn test_kafkaproxy_config_new() {
        //
        let config_path = String::from("testdata/config.yaml");
        let config = KafkaProxyConfig::initialize_config(&config_path);
        assert_eq!(
            config.is_err(),
            false,
            "failed to initialize kafka-proxy config: got error: {}",
            config.err().unwrap().to_string()
        );
        let config_unwrapped = config.unwrap();
        assert_eq!(config_unwrapped.http.port.unwrap(), 4242);
        assert_eq!(config_unwrapped.http.metrics_port.unwrap(), 8089);
        assert_eq!(config_unwrapped.kafka.request_required_acks.unwrap(), 1);
        assert_eq!(config_unwrapped.kafka.queue_buffering_max_ms.unwrap(), 20);
    }
}
