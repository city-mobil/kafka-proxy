use slog::Drain;
use std::sync::Arc;

const DEFAULT_OUTPUT: &str = "/dev/stdout";

pub type Logger = Arc<slog::Logger>;

pub fn new_logger(output: &String) -> Logger {
    let mut output_path = DEFAULT_OUTPUT;
    if output != "" {
        output_path = output;
    }

    if output_path != DEFAULT_OUTPUT && output_path != "" {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(output_path);
        let drain = slog_json::Json::new(file.unwrap())
            .add_default_keys()
            .build()
            .fuse();
        let async_drain = slog_async::Async::new(drain).build();
        return Arc::new(slog::Logger::root(
            async_drain.fuse(),
            slog::o!("service" => "kprf"),
        ));
    }

    let drain = slog_json::Json::new(std::io::stdout())
        .add_default_keys()
        .build()
        .fuse();
    let async_drain = slog_async::Async::new(drain).build();
    let logger = slog::Logger::root(async_drain.fuse(), slog::o!("service" => "kprf"));
    return Arc::new(logger);
}
