use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::constants::Command;

use crate::Conn;

use super::Routine;

/// A routine that executes `COM_PING`.
#[derive(Debug, Copy, Clone)]
pub struct PingRoutine;

impl Routine<()> for PingRoutine {
    fn call<'a>(&'a mut self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>> {
        async move {
            conn.write_command_data(Command::COM_PING, &[]).await?;
            conn.read_packet().await?;
            Ok(())
        }
        .boxed()
    }
}
