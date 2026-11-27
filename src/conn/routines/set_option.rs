use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::constants::Command;
#[cfg(feature = "tracing")]
use tracing::debug_span;

use crate::Conn;

use super::Routine;

/// A routine that performs `COM_SET_OPTION` for multi-statements.
#[derive(Debug, Copy, Clone)]
pub struct SetMultiStatementsRoutine(pub bool);

impl Routine<()> for SetMultiStatementsRoutine {
    fn call<'a>(self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>>
    where
        Self: 'a,
    {
        #[cfg(feature = "tracing")]
        let span = debug_span!(
            "mysql_async::set_option",
            mysql_async.connection.id = conn.id()
        );

        // MYSQL_OPTION_MULTI_STATEMENTS_ON = 0, MYSQL_OPTION_MULTI_STATEMENTS_OFF = 1
        let option: u16 = if self.0 { 0 } else { 1 };

        let fut = async move {
            conn.write_command_data(Command::COM_SET_OPTION, option.to_le_bytes())
                .await?;
            conn.read_packet().await?;
            Ok(())
        };

        #[cfg(feature = "tracing")]
        let fut = instrument_result!(fut, span);

        fut.boxed()
    }
}
