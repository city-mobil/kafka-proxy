use crate::http::api_handler::api::requests::PushResponseError;
use crate::kafka::kafka::producer;
use crate::log::kflog;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub(crate) mod requests {
    use serde::Deserialize;
    use serde::Serialize;

    #[derive(Debug, Deserialize)]
    pub struct Record {
        pub data: String,
        pub topic: String,
        pub key: Option<String>,
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
}

struct ProduceHelper {
    idx: u16,
    result: OwnedDeliveryResult,
}

impl ApiHandler {
    fn generate_request_id() -> String {
        Uuid::new_v4().to_string()
    }

    async fn produce_records(
        &self,
        records: &Vec<requests::Record>,
    ) -> Result<(), Vec<PushResponseError>> {
        let futures = records
            .iter()
            .enumerate()
            .map(|record| async move {
                let result = self
                    .kafka_producer
                    .clone()
                    .send(
                        &record.1.topic,
                        &record.1.data,
                        record.1.key.as_ref(),
                        Duration::from_millis(100),
                    )
                    .await;
                ProduceHelper {
                    idx: record.0 as u16,
                    result,
                }
            })
            .collect::<Vec<_>>();

        let mut has_errors = false;
        let mut error_vec = vec![];
        for future in futures {
            let f = future.await;
            if !f.result.is_err() {
                error_vec.push(PushResponseError {
                    error: false,
                    message: None,
                });
                continue;
            }

            let (err, _) = f.result.unwrap_err();
            let err_str = err.to_string();
            slog::error!(
                self.logger,
                "got error when tried to send message";
                "error" => &err_str,
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

    async fn push_async(&self, data: &requests::PushRequest) -> Result<(), Vec<PushResponseError>> {
        if data.records.is_empty() {
            return Ok(());
        }

        // NOTE(shmel1k): possible API improvement. Add
        // msg_id for each unique message sent.
        self.produce_records(&data.records).await
    }

    pub async fn handle_push(&'static self, req: &requests::PushRequest) -> requests::PushResponse {
        if req.wait_for_send.unwrap_or_default() {
            tokio::spawn(async move {
                self.push_async(&req).await;
            });
            return requests::PushResponse {
                status: "ok".to_string(),
                errors: vec![],
            };
        }

        // TODO(a.petrukhin): return back after context implementation.
        // let req_id = request_id_cloned.clone();
        let await_result = self.push_async(&req).await;
        if !await_result.is_err() {
            return requests::PushResponse {
                status: "ok".to_string(),
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
            status: "error".to_string(),
        }
    }

    pub fn new(
        logger: kflog::Logger,
        kafka_producer: Arc<producer::Producer>,
    ) -> &'static ApiHandler {
        &ApiHandler {
            logger,
            kafka_producer,
        }
    }
}
