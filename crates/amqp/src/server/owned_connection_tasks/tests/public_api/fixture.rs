use super::super::fixture::{MarkedIo, hello};
use super::*;
use crate::{
    ConnectionOptions, EngineError, ScopedConnectionAcceptance, ServerConnection,
    ServerConnectionAcceptor,
};

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Ordinary,
    Posting,
    WorkDefaults,
}

type Opening = Result<
    (
        Result<ScopedConnectionAcceptance<MarkedIo>, EngineError>,
        io::Result<()>,
    ),
    tokio::time::error::Elapsed,
>;

pub(super) struct Setup {
    pub(super) connection: Option<ServerConnection>,
    pub(super) failed: Option<Opening>,
}

pub(super) async fn open(
    acceptor: ServerConnectionAcceptor,
    io: MarkedIo,
    peer: &mut tokio::io::DuplexStream,
    mode: Mode,
) -> Setup {
    let accepting = async {
        let options = ConnectionOptions::default();
        match mode {
            Mode::Ordinary => {
                acceptor
                    .accept_with_options(io, "server", None, options)
                    .await
            }
            Mode::Posting => {
                acceptor
                    .accept_with_transactional_ingress(io, "server", None, options)
                    .await
            }
            Mode::WorkDefaults => {
                acceptor
                    .accept_with_transactional_work_defaults(io, "server", None, options)
                    .await
            }
        }
    };
    let opened = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(accepting, hello(peer))
    })
    .await;
    match opened {
        Ok((Ok(ScopedConnectionAcceptance::Accepted(connection)), Ok(()))) => Setup {
            connection: Some(connection),
            failed: None,
        },
        failed => Setup {
            connection: None,
            failed: Some(failed),
        },
    }
}

impl Setup {
    // Call only after the external root's actual barriers.
    pub(super) fn checked(self) -> TestResult {
        if self.failed.is_some() {
            return Err(io::Error::other("public acceptance setup did not complete").into());
        }
        Ok(())
    }
}

pub(super) fn expected_wire() -> Result<Vec<u8>, EngineError> {
    let mut expected = b"AMQP\0\x01\0\0".to_vec();
    expected.extend(crate::encode_frame(&crate::server::checked_open_frame(
        crate::Open {
            max_frame_size: crate::server::DEFAULT_MAX_FRAME_SIZE,
            idle_time_out: Some(ConnectionOptions::default().advertised_idle_timeout()),
            ..crate::Open::new("server")
        },
    )?)?);
    Ok(expected)
}
