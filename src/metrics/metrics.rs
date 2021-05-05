use crate::log::kflog::Logger;
use warp::{Filter, Rejection, Reply};

pub fn with_logger(
    logger: Logger,
) -> impl Filter<Extract = (Logger,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || logger.clone())
}

pub async fn handler(logger: Logger) -> Result<impl Reply, Rejection> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        slog::error!(
            logger,
            "failed to encode prometheus metrics";
            "error" => e.to_string(),
        );
    };

    let result = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            slog::error!(
                logger,
                "failed to encode prometheus metrics from utf8";
                "error" => e.to_string(),
            );
            String::default()
        }
    };

    buffer.clear();
    Ok(result)
}
