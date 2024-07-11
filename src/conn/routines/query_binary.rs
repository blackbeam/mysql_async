use std::marker::PhantomData;

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::constants::Command;
#[cfg(feature = "tracing")]
use tracing::{field, span_enabled, Level};

use crate::tracing_utils::TracingLevel;
use crate::{BinaryProtocol, Conn};

use super::Routine;

/// A routine that performs `COM_QUERY`.
#[derive(Debug, Copy, Clone)]
pub struct QueryBinaryRoutine<'a, L: TracingLevel> {
    data: &'a [u8],
    _phantom: PhantomData<L>,
}

impl<'a, L: TracingLevel> QueryBinaryRoutine<'a, L> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            _phantom: PhantomData,
        }
    }
}

impl<L: TracingLevel> Routine<()> for QueryBinaryRoutine<'_, L> {
    fn call<'a>(&'a mut self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>> {
        #[cfg(feature = "tracing")]
        let span = create_span!(
            L::LEVEL,
            "mysql_async::query",
            mysql_async.connection.id = conn.id(),
            mysql_async.query.sql = field::Empty,
        );

        #[cfg(feature = "tracing")]
        if span_enabled!(Level::DEBUG) {
            // The statement may contain sensitive data. Restrict to DEBUG.
            span.record(
                "mysql_async.query.sql",
                String::from_utf8_lossy(self.data).as_ref(),
            );
        }
        dbg!("about to wrint command data", self.data);
        let fut = async move {
            conn.write_command_data(Command::COM_QUERY, self.data)
                .await?;
            conn.read_result_set::<BinaryProtocol>(true).await?;
            Ok(())
        };

        #[cfg(feature = "tracing")]
        let fut = instrument_result!(fut, span);

        fut.boxed()
    }
}
