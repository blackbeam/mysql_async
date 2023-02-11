use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::constants::Command;
#[cfg(feature = "tracing")]
use tracing::{debug_span, Instrument};

use crate::Conn;

use super::Routine;

/// A routine that executes `COM_PING`.
#[derive(Debug, Copy, Clone)]
pub struct PingRoutine;

impl Routine<()> for PingRoutine {
    fn call<'a>(&'a mut self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>> {
        #[cfg(feature = "tracing")]
        let span = debug_span!("mysql_async::ping", mysql_async.connection.id = conn.id());

        let fut = async move {
            conn.write_command_data(Command::COM_PING, &[]).await?;
            conn.read_packet().await?;
            Ok(())
        };

        #[cfg(feature = "tracing")]
        let fut = async {
            fut.await.or_else(|e| {
                tracing::error!(error = %e);
                Err(e)
            })
        }
        .instrument(span);

        fut.boxed()
    }
}
