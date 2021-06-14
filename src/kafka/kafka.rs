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
    const DEFAULT_STATISTICS_INTERVAL_MS: u32 = 0;

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
        pub statistics_interval_ms: u32,
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
            mp.insert(
                String::from("statistics.interval.ms"),
                self.statistics_interval_ms.to_string(),
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
                statistics_interval_ms: 0,
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

            let statistics_interval_ms_raw = hash.get(&Yaml::from_str("statistics_interval_ms"));
            let mut statistics_interval_ms = DEFAULT_STATISTICS_INTERVAL_MS;
            if statistics_interval_ms_raw.is_some() {
                statistics_interval_ms = statistics_interval_ms_raw
                    .unwrap()
                    .clone()
                    .as_i64()
                    .unwrap() as u32;
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
                statistics_interval_ms,
            };
        }
    }
}

pub mod producer {
    use rdkafka::config::FromClientConfigAndContext;
    use rdkafka::producer::future_producer::OwnedDeliveryResult;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{ClientContext, Statistics};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    const BROKER_STATE_INIT: i64 = 0;
    const BROKER_STATE_DOWN: i64 = 1;
    const BROKER_STATE_CONNECT: i64 = 2;
    const BROKER_STATE_AUTH: i64 = 3;
    const BROKER_STATE_APIVERSION_QUERY: i64 = 4;
    const BROKER_STATE_AUTH_HANDSHAKE: i64 = 5;
    const BROKER_STATE_UP: i64 = 6;
    const BROKER_STATE_UPDATE: i64 = 7;

    struct KprfClientContext {
        /// The number of operations (callbacks, events, etc.) waiting in queue.
        ///
        /// librdkafka 'replyq'
        reply_queue_size: prometheus::IntCounter,
        /// The current number of messages in producer queues.
        ///
        /// librdkafka 'msg_cnt'
        current_messages_in_queue: prometheus::IntCounter,
        /// The current total size of messages in producer queues.
        ///
        /// librdkafka 'msg_size'
        current_messages_in_queue_bytes: prometheus::IntCounter,
        /// The total number of requests sent to brokers.
        ///
        /// librdkafka 'tx'
        total_requests_count: prometheus::IntCounter,
        /// The total number of bytes transmitted to brokers.
        ///
        /// librdkafka 'tx_bytes'
        total_bytes_sent: prometheus::IntCounter, // librdkafka 'tx_bytes'
        /// The total number of responses received from brokers.
        ///
        /// librdkafka 'rx'
        total_responses_received: prometheus::IntCounter,
        /// The total number of bytes received from brokers.
        ///
        /// librdkafka 'rx_bytes'
        total_bytes_received: prometheus::IntCounter,
        /// The total number of messages transmitted (produced) to brokers.
        ///
        /// librdkafka 'txmsgs'
        total_messages_sent: prometheus::IntCounter,
        /// The total number of bytes transmitted (produced) to brokers.
        ///
        /// librdkafka 'txmsg_bytes'
        total_messages_sent_bytes: prometheus::IntCounter,
        /// Number of topics in the metadata cache.
        ///
        /// librdkafka 'metadata_cache_count'
        metadata_cache_topics_count: prometheus::IntGauge,
        /// The broker state (INIT, DOWN, CONNECT, AUTH, APIVERSION_QUERY,
        /// AUTH_HANDSHAKE, UP, UPDATE).
        ///
        /// librdkafka 'brokers.state'
        broker_state: prometheus::IntGaugeVec,
        /// The time since the last broker state change, in microseconds.
        ///
        /// librdkafka 'brokers.stateage'
        broker_stateage: prometheus::IntGaugeVec,
        /// The number of requests awaiting transmission to the broker.
        ///
        /// librdkafka 'brokers.outbuf_cnt'
        broker_outbuf_count: prometheus::IntCounterVec,
        /// The number of messages awaiting transmission to the broker.
        ///
        /// librdkafka 'brokers.outbuf_msg_cnt'
        broker_outbuf_msg_count: prometheus::IntCounterVec,
        /// The number of requests in-flight to the broker that are awaiting a
        /// response.
        ///
        /// librdkafka 'brokers.waitresp_cnt'
        broker_waitresp_count: prometheus::IntCounterVec,
        /// The number of messages in-flight to the broker that are awaiting a
        /// response.
        ///
        /// librdkafka 'brokers.waitresp_msg_cnt'
        broker_waitresp_msg_count: prometheus::IntCounterVec,
        /// The total number of requests sent to the broker.
        ///
        /// librdkafka 'brokers.tx'
        broker_requests_sent: prometheus::IntCounterVec,
        /// The total number of bytes sent to the broker.
        ///
        /// librdkafka 'brokers.txbytes'
        broker_requests_sent_bytes: prometheus::IntCounterVec,
        /// The total number of transmission errors.
        ///
        /// librdkafka 'brokers.txerrs'
        broker_transmission_errors: prometheus::IntCounterVec,
        /// The total number of request retries.
        ///
        /// librdkafka 'brokers.txretries'
        broker_request_retries: prometheus::IntCounterVec,
        /// The total number of requests that timed out.
        ///
        /// librdkafka 'brokers.req_timeouts'
        broker_request_timeouts: prometheus::IntCounterVec,
        /// The total number of responses received from the broker.
        ///
        /// librdkafka 'brokers.rx'
        broker_responses_count: prometheus::IntCounterVec,
        /// The total number of bytes received from the broker.
        ///
        /// librdkafka 'brokers.rxbytes'
        broker_bytes_received: prometheus::IntCounterVec,
        /// The total number of received errors.
        ///
        /// librdkafka 'brokers.rxerrs'
        broker_errors_count: prometheus::IntCounterVec,
        /// The age of the client's metadata for this topic, in milliseconds.
        ///
        /// librdkafka 'topic.metadata_age'
        topic_metadata_age: prometheus::IntGaugeVec,
        /// Rolling window statistics for batch sizes, in bytes.
        ///
        /// librdkafka 'topic.batchsize'
        topic_batchsize_avg: prometheus::IntGaugeVec,
        /// Rolling window statistics for batch message counts.
        ///
        /// librdkafka 'topic.batchcount'
        topic_batchcount_avg: prometheus::IntGaugeVec,
        // TODO(shmel1k): think about wakeups, connects, rtt stats collection.
    }

    impl ClientContext for KprfClientContext {
        fn stats(&self, statistics: Statistics) {
            self.reply_queue_size.inc_by(statistics.replyq as u64);
            self.current_messages_in_queue
                .inc_by(statistics.msg_cnt as u64);
            self.current_messages_in_queue_bytes
                .inc_by(statistics.msg_size as u64);
            self.total_requests_count.inc_by(statistics.tx as u64);
            self.total_bytes_sent.inc_by(statistics.tx_bytes as u64);
            self.total_responses_received.inc_by(statistics.rx as u64);
            self.total_bytes_received.inc_by(statistics.rx_bytes as u64);
            self.total_messages_sent.inc_by(statistics.txmsgs as u64);
            self.total_messages_sent_bytes
                .inc_by(statistics.txmsg_bytes as u64);
            self.metadata_cache_topics_count
                .set(statistics.metadata_cache_cnt);

            for (k, v) in statistics.brokers.iter() {
                let labels = [k.as_str()];
                let state = KprfClientContext::parse_state(&v.state);
                self.broker_state.with_label_values(&labels).set(state);
                self.broker_stateage
                    .with_label_values(&labels)
                    .set(v.stateage);
                self.broker_outbuf_count
                    .with_label_values(&labels)
                    .inc_by(v.outbuf_cnt as u64);
                self.broker_outbuf_msg_count
                    .with_label_values(&labels)
                    .inc_by(v.outbuf_msg_cnt as u64);
                self.broker_waitresp_count
                    .with_label_values(&labels)
                    .inc_by(v.waitresp_cnt as u64);
                self.broker_waitresp_msg_count
                    .with_label_values(&labels)
                    .inc_by(v.waitresp_msg_cnt as u64);
                self.broker_requests_sent
                    .with_label_values(&labels)
                    .inc_by(v.tx as u64);
                self.broker_requests_sent_bytes
                    .with_label_values(&labels)
                    .inc_by(v.txbytes as u64);
                self.broker_transmission_errors
                    .with_label_values(&labels)
                    .inc_by(v.txerrs as u64);
                self.broker_request_retries
                    .with_label_values(&labels)
                    .inc_by(v.txretries as u64);
                self.broker_request_timeouts
                    .with_label_values(&labels)
                    .inc_by(v.req_timeouts as u64);
                self.broker_responses_count
                    .with_label_values(&labels)
                    .inc_by(v.rx as u64);
                self.broker_bytes_received
                    .with_label_values(&labels)
                    .inc_by(v.rxbytes as u64);
                self.broker_errors_count
                    .with_label_values(&labels)
                    .inc_by(v.rxerrs as u64);
            }
            for (k, v) in statistics.topics.iter() {
                let labels = [k.as_str()];
                self.topic_metadata_age
                    .with_label_values(&labels)
                    .set(v.metadata_age);
                self.topic_batchsize_avg
                    .with_label_values(&labels)
                    .set(v.batchsize.avg);
                self.topic_batchcount_avg
                    .with_label_values(&labels)
                    .set(v.batchcnt.avg);
            }
        }
    }

    impl KprfClientContext {
        fn parse_state(state: &String) -> i64 {
            return match state.as_str() {
                "INIT" => BROKER_STATE_INIT,
                "DOWN" => BROKER_STATE_DOWN,
                "CONNECT" => BROKER_STATE_CONNECT,
                "AUTH" => BROKER_STATE_AUTH,
                "APIVERSION_QUERY" => BROKER_STATE_APIVERSION_QUERY,
                "AUTH_HANDSHAKE" => BROKER_STATE_AUTH_HANDSHAKE,
                "UP" => BROKER_STATE_UP,
                "UPDATE" => BROKER_STATE_UPDATE,
                _ => BROKER_STATE_DOWN,
            };
        }

        fn new() -> KprfClientContext {
            KprfClientContext {
                reply_queue_size: prometheus::register_int_counter!(
                    "kafka_producer_reply_queue_size",
                    "Kafka producer reply queue size"
                )
                .unwrap(),
                current_messages_in_queue: prometheus::register_int_counter!(
                    "kafka_producer_current_messages_in_queue",
                    "Kafka producer messages currently in queue"
                )
                .unwrap(),
                current_messages_in_queue_bytes: prometheus::register_int_counter!(
                    "kafka_producer_current_messages_in_queue_bytes",
                    "Kafka producer messages currently in queue as bytes"
                )
                .unwrap(),
                total_requests_count: prometheus::register_int_counter!(
                    "kafka_producer_total_requests_count",
                    "Kafka producer total number of requests sent to brokers"
                )
                .unwrap(),
                total_bytes_sent: prometheus::register_int_counter!(
                    "kafka_producer_total_bytes_sent",
                    "Kafka producer total number of bytes transmitted to brokers"
                )
                .unwrap(),
                total_responses_received: prometheus::register_int_counter!(
                    "kafka_producer_total_responses_received",
                    "Kafka producer total number of responses received from brokers"
                )
                .unwrap(),
                total_bytes_received: prometheus::register_int_counter!(
                    "kafka_producer_total_bytes_received",
                    "Kafka producer total number of bytes received from brokers"
                )
                .unwrap(),
                total_messages_sent: prometheus::register_int_counter!(
                    "kafka_producer_total_messages_sent",
                    "Kafka producer total number of messages transmitted (produced) to brokers"
                )
                .unwrap(),
                total_messages_sent_bytes: prometheus::register_int_counter!(
                    "kafka_producer_total_messages_bytes_sent",
                    "Kafka producer total number of bytes transmitted (produced) to brokers"
                )
                .unwrap(),
                metadata_cache_topics_count: prometheus::register_int_gauge!(
                    "kafka_producer_metadata_cache_topics_count",
                    "Kafka producer number of topics in the metadata cache"
                )
                .unwrap(),
                broker_state: prometheus::register_int_gauge_vec!(
                    "kafka_producer_broker_state",
                    "Kafka producer broker state",
                    &["broker"]
                )
                .unwrap(),
                broker_stateage: prometheus::register_int_gauge_vec!(
                    "kafka_producer_broker_state_age",
                    "Kafka producer time since the last broker state change, in microseconds",
                    &["broker"]
                )
                .unwrap(),
                broker_outbuf_count: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_outbuf_count",
                    "Kafka producer number of requests awaiting transmission to the broker",
                    &["broker"]
                )
                .unwrap(),
                broker_outbuf_msg_count: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_outbuf_msg_count",
                    "Kafka producer number of messages awaiting transmission to the broker",
                    &["broker"]
                )
                .unwrap(),
                broker_waitresp_count: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_waitresp_count",
                    "Kafka producer number of requests awaiting transmission to the broker",
                    &["broker"]
                )
                .unwrap(),
                broker_waitresp_msg_count: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_waitresp_msg_count",
                    "Kafka producer total number of requests sent to the broker",
                    &["broker"]
                )
                .unwrap(),
                broker_requests_sent: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_requests_sent",
                    "Kafka producer total number of requests sent to the broker",
                    &["broker"]
                )
                .unwrap(),
                broker_requests_sent_bytes: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_requests_sent_bytes",
                    "Kafka producer total number of bytes sent to the broker",
                    &["broker"]
                )
                .unwrap(),
                broker_transmission_errors: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_transmission_errors",
                    "Kafka producer total number of transmission errors",
                    &["broker"]
                )
                .unwrap(),
                broker_request_retries: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_request_retries",
                    "Kafka producer total number of request retries",
                    &["broker"]
                )
                .unwrap(),
                broker_request_timeouts: prometheus::register_int_counter_vec!(
                    "kafka_producer_request_timeouts",
                    "Kafka producer total number of requests that timed out",
                    &["broker"]
                )
                .unwrap(),
                broker_responses_count: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_responses_count",
                    "Kafka producer total number of responses received from the broker",
                    &["broker"]
                )
                .unwrap(),
                broker_bytes_received: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_bytes_received",
                    "Kafka producer total number of bytes received from the broker",
                    &["broker"]
                )
                .unwrap(),
                broker_errors_count: prometheus::register_int_counter_vec!(
                    "kafka_producer_broker_errors_count",
                    "Kafka producer total number of received errors",
                    &["broker"]
                )
                .unwrap(),
                topic_metadata_age: prometheus::register_int_gauge_vec!(
                    "kafka_producer_topic_metadata_age",
                    "Kafka producer age of the client's metadata for this topic, in milliseconds",
                    &["topic"]
                )
                .unwrap(),
                topic_batchsize_avg: prometheus::register_int_gauge_vec!(
                    "kafka_producer_topic_batchsize_avg",
                    "Kafka producer rolling window statistics for batch sizes, in bytes",
                    &["topic"]
                )
                .unwrap(),
                topic_batchcount_avg: prometheus::register_int_gauge_vec!(
                    "kafka_producer_topic_batchcount_avg",
                    "Kafka producer rolling window statistics for batch message counts",
                    &["topic"]
                )
                .unwrap(),
            }
        }
    }

    pub struct Producer {
        producer: FutureProducer<KprfClientContext>,
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
            key: Option<&String>,
            partition: Option<i32>,
            timeout: Duration,
        ) -> OwnedDeliveryResult {
            self.queue_size_gauge.with_label_values(&[&topic]).inc();
            self.sent_messages_counter
                .with_label_values(&[&topic])
                .inc();
            let record = FutureRecord {
                topic,
                partition,
                payload: Some(data),
                key,
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

        let client_context = KprfClientContext::new();

        let result = FutureProducer::from_config_and_context(&client_config, client_context);
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
