use crate::http::api_handler::api::ApiHandler;
use crate::http::handlers;
use crate::kafka::kafka;
use crate::log::kflog;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

pub struct Config {
    port: u16,
}

impl Config {
    pub fn new(port: u16) -> Config {
        return Config { port };
    }
}

pub struct Server {
    config: Config,
}

impl Server {
    pub fn new_from_config(config: Config) -> Server {
        return Server { config };
    }

    pub fn start_server(
        &mut self,
        logger: kflog::Logger,
        kafka_producer: Arc<kafka::producer::Producer>,
        shutdown_rx: Receiver<String>,
    ) -> Receiver<i8> {
        let logger_cloned = logger.clone();
        let api_handler = ApiHandler::new(logger_cloned.clone(), kafka_producer.clone());
        let routes = handlers::filter::new_api(logger.clone(), api_handler);

        let (shutdown_completed_tx, shutdown_completed_rx) = oneshot::channel::<i8>();

        let (_, server) = warp::serve(routes).bind_with_graceful_shutdown(
            ([0, 0, 0, 0], self.config.port),
            async move {
                shutdown_rx.await.ok();
                slog::info!(logger_cloned, "shutting down http-server");
                let send_result = shutdown_completed_tx.send(0);
                if send_result.is_err() {
                    panic!(
                        "failed to send data to main_server shutdown channel: {}",
                        send_result.err().unwrap()
                    );
                }
            },
        );
        let task = tokio::task::spawn(server);
        tokio::task::spawn(async move { task.await });
        return shutdown_completed_rx;
    }
}
