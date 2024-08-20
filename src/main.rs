mod config;
mod http;
mod kafka;
mod log;
mod metrics;

use crate::log::kflog;
use clap::Parser;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Kafka Producer Proxy
#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct App {
    #[arg(short, long, help = "Config file path")]
    pub config: std::path::PathBuf,
}

#[tokio::main]
async fn main() {
    let app = App::parse();
    let cfg = config::KafkaProxyConfig::new(&app);

    let logger = kflog::new_logger(&cfg.get_output_file());

    let app_info = cfg.get_app_info();
    slog::info!(
        logger,
        "starting application";
        "version" => app_info.get_version(),
        "commit" => app_info.get_commit_hash(),
    );

    let http_config = cfg.get_http_config();
    let mut http_server = init_http_server(http_config.clone());

    let ratelimiter = ratelimit::Limiter::new(cfg.get_ratelimit_config());

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<String>();
    let (shutdown_metrics_tx, shutdown_metrics_rx) = oneshot::channel::<String>();

    let kafka_producer = kafka::kafka::producer::new(cfg.get_kafka_config());

    let metrics_server = metrics::metrics::Server::new(metrics::metrics::ServerConfig {
        port: http_config.metrics_port(),
    });
    let metrics_shutdown_rx =
        metrics_server.start_server(logger.clone(), shutdown_metrics_rx, app_info.clone());

    // TODO(shmel1k): improve graceful shutdown behavior.
    let main_server_shutdown_rx = http_server.start_server(
        logger.clone(),
        kafka_producer.clone(),
        Arc::new(ratelimiter),
        shutdown_rx,
    );

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            slog::info!(logger, "shutting down application");
            shutdown_tx.send(String::from("shutdown")).expect("failed to shutdown kafka-http server");
            shutdown_metrics_tx.send(String::from("shutdown")).expect("failed to shutdown metrics-http server");
            metrics_shutdown_rx.await.ok();
            main_server_shutdown_rx.await.ok();
        }
    }
}

fn init_http_server(http_config: config::HttpConfig) -> http::server::Server {
    let http_server_config = http::server::Config::new(http_config.port());
    http::server::Server::new_from_config(http_server_config)
}
