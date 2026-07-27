use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use mysql_common::{
    auth::plugins::ChallengeResponsePlugin,
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
    fn call<'a>(self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>>
    where
        Self: 'a,
    {
        #[cfg(feature = "tracing")]
        let span = debug_span!(
            "mysql_async::change_user",
            mysql_async.connection.id = conn.id()
        );

        let fut = async move {
            // Initialize auth context and proc to generate auth data
            let auth_context = crate::conn::AuthContext {
                pass: conn.opts().pass().unwrap_or_default().to_owned(),
                is_ipc_transport: conn.is_socket(),
                is_tls_transport: conn.is_secure(),
                scramble: conn.inner.nonce.clone(),
                server_key_pem: conn.inner.server_key.clone(),
            };

            let mut auth_proc = conn.inner.auth_plugin.init()?;
            let nonce = conn.inner.nonce.clone();
            let response = auth_proc.run(auth_context.clone(), &nonce)?;

            // Build COM_CHANGE_USER packet with generated auth data
            let com_change_user = ComChangeUser::new()
                .with_user(conn.opts().user().map(|x| x.as_bytes()))
                .with_database(conn.opts().db_name().map(|x| x.as_bytes()))
                .with_auth_plugin_data(response.data())
                .with_more_data(Some(
                    ComChangeUserMoreData::new(if conn.inner.version >= (5, 5, 3) {
                        UTF8MB4_GENERAL_CI
                    } else {
                        UTF8_GENERAL_CI
                    })
                    .with_auth_plugin(Some(conn.inner.auth_plugin.clone()))
                    .with_connect_attributes(conn.opts().connect_attributes().cloned()),
                ))
                .into_owned();

            // Send COM_CHANGE_USER command
            conn.write_command(&com_change_user).await?;

            // Handle authentication challenge-response loop
            conn.continue_auth(false, auth_context, auth_proc, response)
                .await?;
            Ok(())
        };

        #[cfg(feature = "tracing")]
        let fut = instrument_result!(fut, span);

        fut.boxed()
    }
}
