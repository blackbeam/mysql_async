use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::constants::Command;
#[cfg(feature = "tracing")]
use tracing::{field, info_span, Instrument, Level};

use crate::{Conn, TextProtocol};

use super::Routine;

/// A routine that performs `COM_QUERY`.
#[derive(Debug, Copy, Clone)]
pub struct QueryRoutine<'a> {
    data: &'a [u8],
}

impl<'a> QueryRoutine<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl Routine<()> for QueryRoutine<'_> {
    fn call<'a>(&'a mut self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>> {
        #[cfg(feature = "tracing")]
        let span = info_span!(
            "mysql_async::query",
            mysql_async.connection.id = conn.id(),
            mysql_async.query.sql = field::Empty
        );
        #[cfg(feature = "tracing")]
        if tracing::span_enabled!(Level::DEBUG) {
            // The statement may contain sensitive data. Restrict to DEBUG.
            span.record(
                "mysql_async.query.sql",
                String::from_utf8_lossy(self.data).as_ref(),
            );
        }

        let fut = async move {
            conn.write_command_data(Command::COM_QUERY, self.data)
                .await?;
            conn.read_result_set::<TextProtocol>(true).await?;
            Ok(())
        };

        #[cfg(feature = "tracing")]
        let fut = fut.instrument(span);

        fut.boxed()
    }
}
