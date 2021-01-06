use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::constants::Command;

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
        async move {
            conn.write_command_data(Command::COM_QUERY, self.data)
                .await?;
            conn.read_result_set::<TextProtocol>(true).await?;
            Ok(())
        }
        .boxed()
    }
}
