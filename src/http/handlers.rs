pub mod filter {
    use super::handler;
    use crate::http::api_handler::api::ApiHandler;
    use crate::log::kflog;
    use std::sync::Arc;
    use warp::Filter;

    pub fn new_api(
        logger: kflog::Logger,
        api_handler: Arc<ApiHandler>,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        return warp::path!("push")
            .and(warp::post())
            .and(warp::body::json())
            .and(with_logger(logger))
            .and(with_api_handler(api_handler))
            .and_then(handler::push);
    }

    fn with_api_handler(
        handler: Arc<ApiHandler>,
    ) -> impl Filter<Extract = (Arc<ApiHandler>,), Error = std::convert::Infallible> + Clone {
        warp::any().map(move || handler.clone())
    }

    fn with_logger(
        logger: kflog::Logger,
    ) -> impl Filter<Extract = (kflog::Logger,), Error = std::convert::Infallible> + Clone {
        warp::any().map(move || logger.clone())
    }
}

mod handler {
    use crate::http::api_handler::api::{requests, ApiHandler};
    use crate::log::kflog;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::time::SystemTime;
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

    pub async fn push(
        req: requests::PushRequest,
        logger: kflog::Logger,
        handler: Arc<ApiHandler>,
    ) -> Result<impl Reply, Infallible> {
        let request_id = generate_request_id();
        let start = SystemTime::now();

        // NOTE(a.petrukhin): sharding is based on uuid from request.
        // It leads to allocations.
        // let request_id_cloned = request_id.clone();
        let is_sync_request = req.wait_for_send;

        let push_result = handler.handle_push(req).await;

        let passed_result = SystemTime::now().duration_since(start);

        let passed = match passed_result {
            Err(e) => {
                slog::error!(logger,
                    "got error when tried to get duration_since";
                    "error" => e.to_string(),
                    "request_id" => request_id,
                );
                0.0
            }
            Ok(psd) => (psd.as_micros() as f64) / 1000.0,
        };

        let json = warp::reply::json(&push_result);

        let mut status_code = warp::http::StatusCode::OK;
        if push_result.status != "ok" {
            status_code = warp::http::StatusCode::INTERNAL_SERVER_ERROR;
        }
        let result = Ok(warp::reply::with_status(json, status_code));

        let mut method = "push/async";
        if is_sync_request.is_some() && is_sync_request.unwrap() {
            method = "push/sync";
        }

        REQUEST_DURATION
            .with_label_values(&[&status_code.as_u16().to_string(), &method])
            .observe(passed);

        if passed >= 0.5 {
            slog::warn!(
                logger,
                "handle_push was too slow";
                "request_id" => request_id,
                "passed" => (passed).to_string() + "ms",
            );
        }

        slog::debug!(
            logger,
            "proceeded_request";
            "request_id" => request_id,
            "passed" => (passed).to_string() + "ms",
        );
        return result;
    }
}
