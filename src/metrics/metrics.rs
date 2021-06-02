use crate::log::kflog;
use crate::log::kflog::Logger;
use crate::metrics::metrics;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use warp::{Filter, Rejection, Reply};

pub(crate) fn with_logger(
    logger: Logger,
) -> impl Filter<Extract = (Logger,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || logger.clone())
}

pub(crate) async fn handler(logger: Logger) -> Result<impl Reply, Rejection> {
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

pub struct ServerConfig {
    pub port: u16,
}

pub struct Server {
    config: ServerConfig,
}

impl Server {
    pub fn new(config: ServerConfig) -> Server {
        return Server { config };
    }

    pub fn start_server(
        &self,
        logger: kflog::Logger,
        shutdown_rx: Receiver<String>,
    ) -> Receiver<i8> {
        let route = warp::path!("metrics")
            .and(metrics::with_logger(logger.clone()))
            .and_then(metrics::handler);

        let (shutdown_completed_tx, shutdown_completed_rx) = oneshot::channel::<i8>();

        slog::info!(
            logger,
            "starting metrics server";
            "port" => self.config.port
        );
        let (_, server) =
            warp::serve(route).bind_with_graceful_shutdown(([0, 0, 0, 0], self.config.port), {
                async move {
                    shutdown_rx.await.ok();
                    slog::info!(logger, "shutting down metrics server");
                    let send_result = shutdown_completed_tx.send(0);
                    if send_result.is_err() {
                        slog::error!(
                            logger,
                            "failed to send data to main_server shutdown channel: {}",
                            send_result.err().unwrap()
                        );
                    }
                }
            });

        let result = tokio::task::spawn(server);
        tokio::task::spawn(async move { result.await });
        return shutdown_completed_rx;
    }
}
