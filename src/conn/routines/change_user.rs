use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::{
    constants::{UTF8MB4_GENERAL_CI, UTF8_GENERAL_CI},
    packets::{ComChangeUser, ComChangeUserMoreData},
};
#[cfg(feature = "tracing")]
use tracing::debug_span;

use crate::Conn;

use super::Routine;

/// A routine that performs `COM_CHANGE_USER`.
#[derive(Debug, Copy, Clone)]
pub struct ChangeUser;

impl Routine<()> for ChangeUser {
    fn call<'a>(&'a mut self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>> {
        #[cfg(feature = "tracing")]
        let span = debug_span!(
            "mysql_async::change_user",
            mysql_async.connection.id = conn.id()
        );

        let com_change_user = ComChangeUser::new()
            .with_user(conn.opts().user().map(|x| x.as_bytes()))
            .with_database(conn.opts().db_name().map(|x| x.as_bytes()))
            .with_auth_plugin_data(
                conn.inner
                    .auth_plugin
                    .gen_data(conn.opts().pass(), &conn.inner.nonce)
                    .as_deref(),
            )
            .with_more_data(Some(
                ComChangeUserMoreData::new(if conn.inner.version >= (5, 5, 3) {
                    UTF8MB4_GENERAL_CI
                } else {
                    UTF8_GENERAL_CI
                })
                .with_auth_plugin(Some(conn.inner.auth_plugin.clone()))
                .with_connect_attributes(None),
            ))
            .into_owned();

        let fut = async move {
            conn.write_command(&com_change_user).await?;
            conn.inner.auth_switched = false;
            conn.continue_auth().await?;
            Ok(())
        };

        #[cfg(feature = "tracing")]
        let fut = instrument_result!(fut, span);

        fut.boxed()
    }
}
