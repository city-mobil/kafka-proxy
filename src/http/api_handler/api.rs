use crate::http::api_handler::api::requests::PushResponseError;
use crate::kafka::kafka::producer;
use crate::log::kflog;
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::Timestamp;
use std::sync::Arc;
use std::time::Duration;
//use uuid::Uuid;

pub(crate) mod requests {
    use serde::Deserialize;
    use serde::Serialize;

    #[derive(Debug, Deserialize)]
    pub struct Record {
        pub data: String,
        pub topic: String,
        pub key: Option<String>,
        pub partition: Option<i32>,
    }

    #[derive(Debug, Deserialize)]
    pub struct PushRequest {
        pub records: Vec<Record>,
        pub wait_for_send: Option<bool>,
    }

    #[derive(Serialize)]
    pub struct PushResponseError {
        pub error: bool,
        pub message: Option<String>,
    }

    #[derive(Serialize)]
    pub struct PushResponse {
        pub status: String,
        pub errors: Vec<PushResponseError>,
    }
}

pub struct ApiHandler {
    logger: kflog::Logger,
    kafka_producer: Arc<producer::Producer>,
    ratelimiter: Arc<ratelimit::Limiter>,
}

struct ProduceHelper {
    idx: usize,
    result: OwnedDeliveryResult,
    // ratelimit identified if ratelimit occurred.
    ratelimit: bool,
}

struct Request {
    logger: kflog::Logger,
    kafka_producer: Arc<producer::Producer>,
    ratelimiter: Arc<ratelimit::Limiter>,
}

static MESSAGE_RATELIMIT: &str = "ratelimit";

lazy_static::lazy_static!(
    static ref RATELIMIT_MESSAGES_COUNT: prometheus::IntCounterVec =
        prometheus::register_int_counter_vec!(
            "ratelimit_messages_count",
            "Total number of ratelimited messages",
            &["topic"]
        )
        .unwrap();
);

impl Request {
    pub(crate) fn new(
        logger: kflog::Logger,
        kafka_producer: Arc<producer::Producer>,
        ratelimiter: Arc<ratelimit::Limiter>,
    ) -> Request {
        Request {
            logger,
            kafka_producer,
            ratelimiter,
        }
    }

    fn new_ratelimit_error() -> OwnedDeliveryResult {
        return Err((
            KafkaError::Canceled,
            OwnedMessage::new(
                None,
                None,
                "".to_string(),
                Timestamp::NotAvailable,
                0,
                0,
                None,
            ),
        ));
    }

    fn check_ratelimit(&self, topic: &String) -> Result<(), ProduceHelper> {
        let ratelimit_result = self.ratelimiter.check(topic);
        if ratelimit_result.is_err() {
            slog::info!(
                self.logger,
                "got error when tried to check ratelimit";
                "topic" => topic,
                "error" => ratelimit_result.err(),
            );
            return Err(ProduceHelper {
                idx: 0,
                result: Request::new_ratelimit_error(),
                ratelimit: true,
            });
        }
        if !ratelimit_result.unwrap() {
            return Err(ProduceHelper {
                idx: 0,
                result: Request::new_ratelimit_error(),
                ratelimit: true,
            });
        }

        Ok(())
    }

    async fn produce_records(
        &self,
        records: &Vec<requests::Record>,
    ) -> Result<(), Vec<PushResponseError>> {
        let futures = records
            .iter()
            .enumerate()
            .map(|record| async move {
                let ratelimit_result = self.check_ratelimit(&record.1.topic);
                if ratelimit_result.is_err() {
                    let mut err = ratelimit_result.unwrap_err();
                    err.idx = record.0 as usize;
                    return err;
                }

                let result = self
                    .kafka_producer
                    .clone()
                    .send(
                        &record.1.topic,
                        &record.1.data,
                        record.1.key.as_ref(),
                        record.1.partition,
                        Duration::from_millis(100),
                    )
                    .await;
                ProduceHelper {
                    idx: record.0 as usize,
                    result,
                    ratelimit: false,
                }
            })
            .collect::<Vec<_>>();

        let mut has_errors = false;
        // NOTE(shmel1k): Empty vector is used in order to avoid unnecessary allocations.
        let mut error_vec = vec![];
        for future in futures {
            let f = future.await;
            if f.ratelimit {
                // TODO(shmel1k): think about moving this stats recording
                // to some other place.
                RATELIMIT_MESSAGES_COUNT
                    .with_label_values(&[&records[f.idx].topic])
                    .inc();

                slog::warn!(
                    self.logger,
                    "message was not sent due to ratelimit overflow";
                    "topic" => &records[f.idx].topic,
                );
                error_vec.push(PushResponseError {
                    error: true,
                    message: Some(MESSAGE_RATELIMIT.to_string()),
                });
                has_errors = true;
                continue;
            }
            if !f.result.is_err() {
                error_vec.push(PushResponseError {
                    error: false,
                    message: None,
                });
                continue;
            }

            let (err, _) = f.result.unwrap_err();
            let err_str = err.to_string();
            let topic = &records[f.idx].topic;
            slog::error!(
                self.logger,
                "got error when tried to send message";
                "error" => &err_str,
                "topic" => topic,
            );
            error_vec.push(PushResponseError {
                error: true,
                message: Option::Some(err_str),
            });
            has_errors = true;
        }

        if has_errors {
            return Err(error_vec);
        }
        Ok(())
    }

    pub(crate) async fn push_async(
        &self,
        data: &requests::PushRequest,
    ) -> Result<(), Vec<PushResponseError>> {
        if data.records.is_empty() {
            return Ok(());
        }

        // NOTE(shmel1k): possible API improvement. Add
        // msg_id for each unique message sent.
        self.produce_records(&data.records).await
    }
}

static RESPONSE_STATUS_OK: &str = "ok";
static RESPONSE_STATUS_ERR: &str = "err";

impl ApiHandler {
    // fn generate_request_id() -> String {
    //    Uuid::new_v4().to_string()
    //}

    pub async fn handle_push(&self, req: requests::PushRequest) -> requests::PushResponse {
        let request = Request::new(
            self.logger.clone(),
            self.kafka_producer.clone(),
            self.ratelimiter.clone(),
        );

        if !req.wait_for_send.unwrap_or_default() {
            tokio::spawn(async move {
                let _ = request.push_async(&req).await;
            });
            return requests::PushResponse {
                status: RESPONSE_STATUS_OK.to_string(),
                errors: vec![],
            };
        }

        // TODO(a.petrukhin): return back after context implementation.
        // let req_id = request_id_cloned.clone();
        let await_result = request.push_async(&req).await;
        if !await_result.is_err() {
            return requests::PushResponse {
                status: RESPONSE_STATUS_OK.to_string(),
                errors: vec![],
            };
        }

        let err = await_result.unwrap_err();
        for e in err.iter() {
            if e.message.is_none() {
                continue;
            }

            // TODO(shmel1k): add request_id from future context.
            let msg = e.message.clone().unwrap();
            slog::error!(
                self.logger.clone(),
                "got error when tried to push message in sync mode";
                "error" => msg,
            );
        }

        requests::PushResponse {
            errors: err,
            status: RESPONSE_STATUS_ERR.to_string(),
        }
    }

    pub fn new(
        logger: kflog::Logger,
        kafka_producer: Arc<producer::Producer>,
        ratelimiter: Arc<ratelimit::Limiter>,
    ) -> Arc<ApiHandler> {
        Arc::new(ApiHandler {
            logger,
            kafka_producer,
            ratelimiter,
        })
    }
}
