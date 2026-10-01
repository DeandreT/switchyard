use server::StartupError;
use tracing_subscriber::EnvFilter;

pub(super) fn initialize() -> Result<(), StartupError> {
    initialize_bridge()?;
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_| StartupError::LoggingInitialization)
}

fn initialize_bridge() -> Result<(), StartupError> {
    // These dependencies log rule text, HTTP headers, and wire payloads. Deny
    // them before dispatch, independently of user-selected tracing levels.
    tracing_log::LogTracer::builder()
        .ignore_crate("sqlparser")
        .ignore_crate("tungstenite")
        .ignore_crate("tokio_tungstenite")
        .init()
        .map_err(|_| StartupError::LoggingInitialization)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use super::*;

    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("diagnostic capture")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn explicit_dependency_trace_cannot_log_private_data_but_diagnostics_work() {
        initialize_bridge().expect("the binary test owns its log bridge");
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::clone(&output);
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(
                "trace,sqlparser=trace,tungstenite=trace,tokio_tungstenite=trace",
            ))
            .without_time()
            .with_ansi(false)
            .with_writer(move || Capture(Arc::clone(&writer)))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            for target in [
                "sqlparser",
                "sqlparser::parser",
                "sqlparser::dialect",
                "tungstenite",
                "tungstenite::protocol::frame",
                "tungstenite::handshake::server",
                "tokio_tungstenite",
                "tokio_tungstenite::compat",
            ] {
                assert!(
                    !log::logger().enabled(
                        &log::Metadata::builder()
                            .target(target)
                            .level(log::Level::Trace)
                            .build()
                    )
                );
                log::trace!(target: target, "private_websocket_credential private_message_body");
            }
            assert!(
                log::logger().enabled(
                    &log::Metadata::builder()
                        .target("switchyard::diagnostic")
                        .level(log::Level::Info)
                        .build()
                )
            );
            domain::SqlProgram::compile("private_rule_key = 'private_rule_literal'")
                .expect("a valid source that the parser would log at debug");
            log::info!(target: "switchyard::diagnostic", "healthy dependency event");
            tracing::info!("healthy native event");
        });
        let output = String::from_utf8(output.lock().expect("diagnostic capture").clone())
            .expect("UTF-8 diagnostics");
        assert!(output.contains("healthy dependency event"), "{output}");
        assert!(output.contains("healthy native event"), "{output}");
        assert!(!output.contains("private_rule_key"), "{output}");
        assert!(!output.contains("private_rule_literal"), "{output}");
        assert!(!output.contains("private_websocket_credential"), "{output}");
        assert!(!output.contains("private_message_body"), "{output}");
    }
}
