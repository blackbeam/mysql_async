// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::error;
use tokio::prelude::*;

use std::{fmt, future::Future, marker::Unpin, pin::Pin, sync::Arc};

pub mod builtin;

/// Trait used to handle local infile requests.
///
/// Be aware of security issues with [LOAD DATA LOCAL][1].
/// Using [`crate::WhiteListFsLocalInfileHandler`] is advised.
///
/// Simple handler example:
///
/// ```rust
/// # use mysql_async::{prelude::*, test_misc::get_opts};
/// # use tokio::prelude::*;
/// # use std::env;
/// # #[tokio::main]
/// # async fn main() -> Result<(), mysql_async::error::Error> {
/// #
/// /// This example hanlder will return contained bytes in response to a local infile request.
/// struct ExampleHandler(&'static [u8]);
///
/// impl LocalInfileHandler for ExampleHandler {
///     fn handle(&self, _: &[u8]) -> mysql_async::InfileHandlerFuture {
///         let handler = Box::new(self.0) as Box<_>;
///         Box::pin(async move { Ok(handler) })
///     }
/// }
///
/// # let database_url = get_opts();
///
/// let mut opts = mysql_async::OptsBuilder::from_opts(database_url);
/// opts.local_infile_handler(Some(ExampleHandler(b"foobar")));
///
/// let pool = mysql_async::Pool::new(opts);
///
/// let mut conn = pool.get_conn().await?;
/// conn.drop_query("CREATE TEMPORARY TABLE tmp (a TEXT);").await?;
/// match conn.drop_query("LOAD DATA LOCAL INFILE 'baz' INTO TABLE tmp;").await {
///     Ok(()) => (),
///     Err(mysql_async::error::Error::Server(ref err)) if err.code == 1148 => {
///         // The used command is not allowed with this MySQL version
///         return Ok(());
///     },
///     Err(mysql_async::error::Error::Server(ref err)) if err.code == 3948 => {
///         // Loading local data is disabled;
///         // this must be enabled on both the client and server sides
///         return Ok(());
///     }
///     e@Err(_) => e.unwrap(),
/// };
/// let result = conn.prep_exec("SELECT * FROM tmp;", ()).await?;
/// let result = result.map_and_drop(|row| {
///     mysql_async::from_row::<(String,)>(row).0
/// }).await?;
///
/// assert_eq!(result.len(), 1);
/// assert_eq!(result[0], "foobar");
/// drop(conn);
/// pool.disconnect().await?;
/// # Ok(())
/// # }
/// ```
///
/// [1]: https://dev.mysql.com/doc/refman/8.0/en/load-data-local.html
pub trait LocalInfileHandler: Sync + Send {
    /// `file_name` is the file name in `LOAD DATA LOCAL INFILE '<file name>' INTO TABLE ...;`
    /// query.
    fn handle(&self, file_name: &[u8]) -> InfileHandlerFuture;
}

pub type InfileHandlerFuture = Pin<
    Box<
        dyn Future<Output = Result<Box<dyn AsyncRead + Send + Unpin + 'static>, error::Error>>
            + Send
            + 'static,
    >,
>;

/// Object used to wrap `T: LocalInfileHandler` inside of Opts.
#[derive(Clone)]
pub struct LocalInfileHandlerObject(Arc<dyn LocalInfileHandler>);

impl LocalInfileHandlerObject {
    pub fn new<T: LocalInfileHandler + 'static>(handler: T) -> Self {
        LocalInfileHandlerObject(Arc::new(handler))
    }

    pub fn clone_inner(&self) -> Arc<dyn LocalInfileHandler> {
        self.0.clone()
    }
}

impl PartialEq for LocalInfileHandlerObject {
    fn eq(&self, other: &LocalInfileHandlerObject) -> bool {
        self.0.as_ref() as *const dyn LocalInfileHandler
            == other.0.as_ref() as *const dyn LocalInfileHandler
    }
}

impl Eq for LocalInfileHandlerObject {}

impl fmt::Debug for LocalInfileHandlerObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Local infile handler object")
    }
}
