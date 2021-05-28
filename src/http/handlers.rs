mod requests {
    use serde::Deserialize;

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
}

pub mod filter {
    use super::handler;
    use crate::kafka::kafka;
    use crate::kafka::kafka::producer;
    use crate::log::kflog;
    use std::sync::Arc;
    use warp::Filter;

    pub fn new_api(
        logger: kflog::Logger,
        kafka_producer: Arc<producer::Producer>,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        return warp::path!("push")
            .and(warp::post())
            .and(warp::body::json())
            .and(with_logger(logger))
            .and(with_kafka_producer(kafka_producer))
            .and_then(handler::push);
    }

    fn with_kafka_producer(
        kafka_producer: Arc<kafka::producer::Producer>,
    ) -> impl Filter<Extract = (Arc<kafka::producer::Producer>,), Error = std::convert::Infallible> + Clone
    {
        warp::any().map(move || kafka_producer.clone())
    }

    fn with_logger(
        logger: kflog::Logger,
    ) -> impl Filter<Extract = (kflog::Logger,), Error = std::convert::Infallible> + Clone {
        warp::any().map(move || logger.clone())
    }
}

mod handler {
    use super::requests::PushRequest;
    use crate::kafka::kafka::producer::Producer;
    use crate::log::kflog;
    use rdkafka::error::KafkaError;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};
    use uuid::Uuid;
    use warp::Reply;

    lazy_static::lazy_static! {
        static ref REQUEST_DURATION: prometheus::HistogramVec = prometheus::register_histogram_vec!(
            "http_requests_duration",
            "Duration of HTTP requests",
            &["code", "method"],
            prometheus::exponential_buckets(5.0, 2.0, 5).unwrap()
        ).unwrap();
    }

    fn generate_request_id() -> String {
        Uuid::new_v4().to_string()
    }

    async fn push_async(
        logger: &kflog::Logger,
        kafka_producer: Arc<Producer>,
        data: &PushRequest,
        request_id: &String,
    ) -> Option<KafkaError> {
        // NOTE(shmel1k): wal should be implemented here.
        if data.records.is_empty() {
            return None;
        }

        let producer = &kafka_producer.clone();
        let futures = data
            .records
            .iter()
            .map(|record| async move {
                producer
                    .send(
                        &record.topic,
                        &record.data,
                        record.key.as_ref(),
                        Duration::from_millis(300),
                    )
                    .await
            })
            .collect::<Vec<_>>();

        let mut error_vec = vec![];
        for future in futures {
            let result = future.await;
            if !result.is_err() {
                continue;
            }

            let (err, _) = result.unwrap_err();

            slog::error!(
                logger,
                "got error when tried to send message";
                "error" => err.to_string(),
                "request_id" => request_id,
            );
            error_vec.push(err);
        }

        // NOTE(shmel1k): return only last error to user. All other errors are logged
        // in producer.
        if error_vec.len() > 0 {
            return error_vec.pop();
        }

        return None;
    }

    pub async fn push(
        req: PushRequest,
        logger: kflog::Logger,
        kafka_producer: Arc<Producer>,
    ) -> Result<impl Reply, Infallible> {
        let request_id = generate_request_id();
        let start = SystemTime::now();

        // NOTE(a.petrukhin): sharding is based on uuid from request.
        // It leads to allocations.
        let request_id_cloned = request_id.clone();
        let is_sync_request = req.wait_for_send;
        let mut err = String::from("");
        let logger_cloned = logger.clone();
        if is_sync_request.is_none() || !is_sync_request.unwrap() {
            tokio::spawn(async move {
                push_async(&logger_cloned, kafka_producer, &req, &request_id_cloned).await
            });
        } else {
            let req_id = request_id_cloned.clone();
            let await_result =
                push_async(&logger_cloned, kafka_producer, &req, &request_id_cloned).await;
            if await_result.is_some() {
                err = await_result.unwrap().to_string();
                slog::error!(
                    logger,
                    "got error when tried to push message in sync mode";
                    "request_id" => req_id,
                    "error" => &err,
                );
            }
        }

        let passed_result = SystemTime::now().duration_since(start);

        let passed = match passed_result {
            Err(e) => {
                slog::warn!(logger,
            "got error when tried to get duration_since";
            "error" => e.to_string());
                0.0
            }
            Ok(psd) => (psd.as_micros() as f64) / 1000.0,
        };

        let mut status_code = warp::http::StatusCode::OK;
        let mut result = Ok(warp::reply::with_status("{\"status\":\"ok\"}", status_code));
        if err != "" {
            status_code = warp::http::StatusCode::INTERNAL_SERVER_ERROR;
            result = Ok(warp::reply::with_status(
                "{\"status\":\"error\"}",
                status_code,
            ));
        }

        let mut method = "push/async";
        if is_sync_request.is_some() && is_sync_request.unwrap() {
            method = "push/sync";
        }

        REQUEST_DURATION
            .with_label_values(&[&status_code.as_u16().to_string(), &method])
            .observe(passed);

        slog::info!(
            logger,
            "proceeded_request";
            "request_id" => request_id,
            "passed" => (passed).to_string() + "ms",
            "error" => &err,
        );
        return result;
    }
}
