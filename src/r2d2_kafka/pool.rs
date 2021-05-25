use rdkafka::config::FromClientConfig;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::producer::{FutureProducer, Producer};
use rdkafka::ClientConfig;
use std::time::Duration;

const DEFAULT_VALIDATION_DURATION: u64 = 100;

#[derive(Clone, Debug)]
pub struct KafkaConnectorManager {
    config: ClientConfig,
    validation_duration: Duration,
}

impl KafkaConnectorManager {
    pub fn new(config: ClientConfig) -> KafkaConnectorManager {
        let validation_duration = Duration::from_micros(DEFAULT_VALIDATION_DURATION);
        KafkaConnectorManager {
            config,
            validation_duration,
        }
    }
}

impl r2d2::ManageConnection for KafkaConnectorManager {
    type Connection = FutureProducer;
    type Error = KafkaError;

    fn connect(&self) -> KafkaResult<FutureProducer> {
        FutureProducer::from_config(&self.config.clone())
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        match conn
            .client()
            .fetch_metadata(Some(""), self.validation_duration)
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.is_valid(conn).is_err()
    }
}
