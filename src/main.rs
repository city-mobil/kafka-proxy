mod config;
mod http;
mod kafka;
mod log;
mod metrics;

use crate::log::kflog;
use clap::ArgMatches;
use tokio::sync::oneshot;

fn app_args<'a>() -> ArgMatches<'a> {
    return clap::App::new("kprf")
        .version(clap::crate_version!())
        .about("Kafka Producer Proxy")
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .help("Config file path")
                .takes_value(true),
        )
        .get_matches();
}

#[tokio::main]
async fn main() {
    let args = app_args();
    let cfg = config::KafkaProxyConfig::new(args);

    let logger = kflog::new_logger(&cfg.get_output_file());

    let app_info = cfg.get_app_info();
    slog::info!(
        logger,
        "starting application";
        "version" => app_info.get_version(),
        "commit" => app_info.get_commit_hash(),
    );

    let mut http_server = init_http_server(cfg.get_http_config());

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<String>();
    let (shutdown_metrics_tx, shutdown_metrics_rx) = oneshot::channel::<String>();

    let kafka_producer = kafka::kafka::producer::new(cfg.get_kafka_config());

    let metrics_server = metrics::metrics::Server::new(metrics::metrics::ServerConfig {
        port: cfg.get_http_config().metrics_port(),
    });
    let metrics_shutdown_rx = metrics_server.start_server(logger.clone(), shutdown_metrics_rx);

    // TODO(shmel1k): improve graceful shutdown behavior.
    slog::info!(
        logger,
        "starting main http server";
        "port" => cfg.get_http_config().metrics_port(),
    );
    let main_server_shutdown_rx =
        http_server.start_server(logger.clone(), kafka_producer.clone(), shutdown_rx);
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
