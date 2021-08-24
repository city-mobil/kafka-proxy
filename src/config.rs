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

    #[serde(default)]
    ratelimit: ratelimit::config::Config,
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

    pub fn get_ratelimit_config(&self) -> ratelimit::config::Config {
        self.ratelimit.clone()
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

    fn prepare_config(config_path: &String) -> KafkaProxyConfig {
        let config = KafkaProxyConfig::initialize_config(config_path);
        assert_eq!(
            config.is_err(),
            false,
            "failed to initialize kafka-proxy config: got error: {}",
            config.err().unwrap().to_string()
        );

        config.unwrap()
    }

    #[test]
    fn test_kafkaproxy_config_new() {
        let config_path = String::from("testdata/kafka_config.yaml");
        let config = prepare_config(&config_path);

        assert_eq!(config.http.port.unwrap(), 4242); // default value
        assert_eq!(config.kafka.brokers.len(), 1);
        assert_eq!(config.http.metrics_port.unwrap(), 8089);
        assert_eq!(config.kafka.request_required_acks.unwrap(), 1);
        assert_eq!(config.kafka.queue_buffering_max_ms.unwrap(), 20);
    }

    #[test]
    fn test_kafkaproxy_config_ratelimit() {
        let config_path = String::from("testdata/ratelimit.yaml");
        let config = prepare_config(&config_path);

        assert_eq!(config.ratelimit.enabled(), true);
        assert_eq!(config.ratelimit.get_rules().len(), 2);
    }

    #[test]
    fn test_kafkaproxy_config() {
        let config_path = String::from("testdata/config.yaml");
        let config = prepare_config(&config_path);

        assert_eq!(config.http.port.unwrap(), 4242); // default value
        assert_eq!(config.http.metrics_port.unwrap(), 8089);

        assert_eq!(config.kafka.brokers.len(), 1);
        assert_eq!(config.kafka.request_required_acks.unwrap(), 1);
        assert_eq!(config.kafka.queue_buffering_max_ms.unwrap(), 20);
        assert_eq!(config.kafka.queue_buffering_max_kbytes.unwrap(), 2048);

        assert_eq!(config.ratelimit.enabled(), true);
        assert_eq!(config.ratelimit.get_rules().len(), 2);
    }
}
