pub mod config {
    use std::collections::HashMap;
    use yaml_rust::Yaml;

    const DEFAULT_USER: &str = "";
    const DEFAULT_PASSWORD: &str = "";
    const DEFAULT_MESSAGE_MAX_BYTES: u32 = 1024 * 1024; // 1 MiB
    const DEFAULT_QUEUE_BUFFERING_MAX_MESSAGES: u32 = 100000;
    const DEFAULT_QUEUE_BUFFERING_MAX_MS: u32 = 10; // librdkafka default value is '5'
    const DEFAULT_RETRIES: u32 = 3; // librdkafka default is 2^32 - 1
    const DEFAULT_MESSAGE_TIMEOUT_MS: u32 = 2000; // librdkafka default is 300000
    const DEFAULT_REQUEST_REQUIRED_ACKS: i32 = -1;
    const DEFAULT_REQUEST_TIMEOUT_MS: u32 = 30000;
    const DEFAULT_QUEUE_BUFFERING_MAX_KBYTES: u32 = 1048576;

    #[derive(Debug, Clone)]
    pub struct Config {
        pub brokers: Vec<String>,
        pub user: String,
        pub password: String,
        pub message_max_bytes: u32,
        pub queue_buffering_max_messages: u32,
        pub queue_buffering_max_ms: u32,
        pub queue_buffering_max_kbytes: u32,
        pub retries: u32,
        pub message_timeout_ms: u32,
        pub request_required_acks: i32,
        pub request_timeout_ms: u32,
    }

    impl Config {
        pub fn to_hash(&self) -> HashMap<String, String> {
            let mut mp: HashMap<String, String> = HashMap::new();
            mp.insert(String::from("bootstrap.servers"), self.brokers.join(","));
            if self.user != "" {
                mp.insert(String::from("sasl.username"), self.user.clone());
            }
            if self.password != "" {
                mp.insert(String::from("sasl.password"), self.password.clone());
            }
            mp.insert(
                String::from("message.max.bytes"),
                self.message_max_bytes.to_string(),
            );
            mp.insert(
                String::from("queue.buffering.max.messages"),
                self.queue_buffering_max_messages.to_string(),
            );
            mp.insert(
                String::from("queue.buffering.max.ms"),
                self.queue_buffering_max_ms.to_string(),
            );
            mp.insert(String::from("retries"), self.retries.to_string());
            mp.insert(
                String::from("message.timeout.ms"),
                self.message_timeout_ms.to_string(),
            );
            mp.insert(
                String::from("request.timeout.ms"),
                self.request_timeout_ms.to_string(),
            );
            mp.insert(
                String::from("request.required.acks"),
                self.request_required_acks.to_string(),
            );
            mp.insert(
                String::from("queue.buffering.max.kbytes"),
                self.queue_buffering_max_kbytes.to_string(),
            );
            return mp;
        }

        pub fn new_empty() -> Config {
            return Config {
                brokers: vec![],
                user: "".to_string(),
                password: "".to_string(),
                message_max_bytes: 0,
                queue_buffering_max_messages: 0,
                queue_buffering_max_ms: 0,
                queue_buffering_max_kbytes: 0,
                retries: 0,
                message_timeout_ms: 0,
                request_required_acks: 0,
                request_timeout_ms: 0,
            };
        }

        pub fn new_from_yaml(yml: &Option<&Yaml>) -> Config {
            let hash = yml.unwrap().as_hash().unwrap();

            // NOTE(a.petrukhin): panic here is OK due to bad config.
            let brokers_arr = hash
                .get(&Yaml::from_str("brokers"))
                .unwrap()
                .clone()
                .into_vec()
                .unwrap();

            let mut brokers: Vec<String> = Vec::new();
            for v in brokers_arr.iter() {
                brokers.push(v.clone().into_string().unwrap());
            }

            // NOTE(a.petrukhin): I believe there is a better way to do this.
            let user_raw = hash.get(&Yaml::from_str("user"));
            let mut user = String::from(DEFAULT_USER);
            if !user_raw.is_none() {
                user = user_raw.unwrap().clone().into_string().unwrap();
            }

            let password_raw = hash.get(&Yaml::from_str("password"));
            let mut password = String::from(DEFAULT_PASSWORD);
            if !password_raw.is_none() {
                password = password_raw.unwrap().clone().into_string().unwrap();
            }

            let message_max_bytes_raw = hash.get(&Yaml::from_str("message_max_bytes"));
            let mut message_max_bytes = DEFAULT_MESSAGE_MAX_BYTES;
            if !message_max_bytes_raw.is_none() {
                message_max_bytes =
                    message_max_bytes_raw.unwrap().clone().into_i64().unwrap() as u32;
            }

            let queue_buffering_max_messages_raw =
                hash.get(&Yaml::from_str("queue_buffering_max_messages"));
            let mut queue_buffering_max_messages = DEFAULT_QUEUE_BUFFERING_MAX_MESSAGES;
            if !queue_buffering_max_messages_raw.is_none() {
                queue_buffering_max_messages = queue_buffering_max_messages_raw
                    .unwrap()
                    .clone()
                    .into_i64()
                    .unwrap() as u32;
            }

            let queue_buffering_max_ms_raw =
                hash.get(&Yaml::from_str("queue_buffering_max_ms")).clone();
            let mut queue_buffering_max_ms = DEFAULT_QUEUE_BUFFERING_MAX_MS;
            if !queue_buffering_max_ms_raw.is_none() {
                queue_buffering_max_ms = queue_buffering_max_ms_raw
                    .unwrap()
                    .clone()
                    .into_i64()
                    .unwrap() as u32;
            }

            let retries_raw = hash.get(&Yaml::from_str("retries")).clone();
            let mut retries = DEFAULT_RETRIES;
            if !retries_raw.is_none() {
                retries = retries_raw.clone().unwrap().clone().as_i64().unwrap() as u32;
            }

            let message_timeout_ms_raw = hash.get(&Yaml::from_str("message_timeout_ms")).clone();
            let mut message_timeout_ms = DEFAULT_MESSAGE_TIMEOUT_MS;
            if !message_timeout_ms_raw.is_none() {
                message_timeout_ms =
                    message_timeout_ms_raw.unwrap().clone().as_i64().unwrap() as u32;
            }

            let queue_buffering_max_kbytes_raw = hash
                .get(&Yaml::from_str("queue_buffering_max_kbytes"))
                .clone();
            let mut queue_buffering_max_kbytes = DEFAULT_QUEUE_BUFFERING_MAX_KBYTES;
            if !queue_buffering_max_messages_raw.is_none() {
                queue_buffering_max_kbytes = queue_buffering_max_kbytes_raw
                    .unwrap()
                    .clone()
                    .as_i64()
                    .unwrap() as u32;
            }

            let request_required_acks_raw = hash.get(&Yaml::from_str("request_required_acks"));
            let mut request_required_acks = DEFAULT_REQUEST_REQUIRED_ACKS;
            if !request_required_acks_raw.is_none() {
                request_required_acks =
                    request_required_acks_raw.unwrap().clone().as_i64().unwrap() as i32;
            }

            let request_timeout_ms_raw = hash.get(&Yaml::from_str("request_timeout_ms"));
            let mut request_timeout_ms = DEFAULT_REQUEST_TIMEOUT_MS;
            if !request_timeout_ms_raw.is_none() {
                request_timeout_ms =
                    request_required_acks_raw.unwrap().clone().as_i64().unwrap() as u32;
            }

            return Config {
                brokers,
                user,
                password,
                message_max_bytes,
                queue_buffering_max_messages,
                queue_buffering_max_ms,
                queue_buffering_max_kbytes,
                retries,
                message_timeout_ms,
                request_required_acks,
                request_timeout_ms,
            };
        }
    }
}

pub mod producer {
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::FromClientConfig;
    use rdkafka::producer::future_producer::OwnedDeliveryResult;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    pub struct Producer {
        producer: FutureProducer<DefaultClientContext>,
        sent_messages_counter: prometheus::IntCounterVec,
        queue_size_gauge: prometheus::IntGaugeVec,
        error_counter: prometheus::IntCounterVec,
        message_send_duration: prometheus::HistogramVec,
    }

    impl Producer {
        pub async fn send(
            &self,
            topic: &String,
            data: &String,
            key: &String,
            timeout: Duration,
        ) -> OwnedDeliveryResult {
            self.queue_size_gauge.with_label_values(&[&topic]).inc();
            self.sent_messages_counter
                .with_label_values(&[&topic])
                .inc();
            let record = FutureRecord {
                topic,
                partition: None,
                payload: Some(data),
                key: Some(key),
                timestamp: None,
                headers: None,
            };

            let start = SystemTime::now();
            let result = self.producer.send(record, timeout).await;
            self.message_send_duration
                .with_label_values(&[&topic])
                .observe(
                    (SystemTime::now().duration_since(start).unwrap().as_micros() as f64) / 1000.0,
                );

            self.queue_size_gauge.with_label_values(&[&topic]).dec();
            if result.is_err() {
                let (err, _) = result.clone().unwrap_err();
                self.error_counter
                    .with_label_values(&[
                        &topic,
                        &(err.rdkafka_error_code().unwrap() as i32).to_string(),
                    ])
                    .inc();
            }
            return result;
        }
    }

    pub fn new(cfg: super::config::Config) -> Arc<Producer> {
        let cf = cfg.to_hash();
        let mut client_config = rdkafka::ClientConfig::new();
        for (k, v) in cf.iter() {
            client_config.set(k, v);
        }

        let result = FutureProducer::from_config(&client_config);
        match result {
            Err(err) => panic!("Failed to create threaded producer: {}", err.to_string()),
            Ok(producer) => Arc::new(Producer {
                producer,
                queue_size_gauge: prometheus::register_int_gauge_vec!(
                    "kafka_internal_queue_size",
                    "Kafka internal queue size",
                    &["topic"]
                )
                .unwrap(),
                error_counter: prometheus::register_int_counter_vec!(
                    "kafka_errors_count",
                    "Kafka internal errors count",
                    &["topic", "error_code"]
                )
                .unwrap(),
                sent_messages_counter: prometheus::register_int_counter_vec!(
                    "kafka_sent_messages",
                    "Kafka sent messages count",
                    &["topic"]
                )
                .unwrap(),
                message_send_duration: prometheus::register_histogram_vec!(
                    "kafka_message_send_duration",
                    "Kafka message send duration",
                    &["topic"],
                    prometheus::exponential_buckets(5.0, 2.0, 5).unwrap()
                )
                .unwrap(),
            }),
        }
    }
}
