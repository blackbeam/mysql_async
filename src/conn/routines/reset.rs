use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::constants::Command;

use crate::Conn;

use super::Routine;

/// A routine that performs `COM_RESET_CONNECTION`.
#[derive(Debug, Copy, Clone)]
pub struct ResetRoutine;

impl Routine<()> for ResetRoutine {
    fn call<'a>(&'a mut self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>> {
        async move {
            conn.write_command_data(Command::COM_RESET_CONNECTION, &[])
                .await?;
            conn.read_packet().await?;
            Ok(())
        }
        .boxed()
    }
}
