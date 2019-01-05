// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use tokio_io::AsyncRead;

use std::{fmt, sync::Arc};

use crate::BoxFuture;

pub mod builtin;

/// Trait used to handle local infile requests.
///
/// Simple handler example:
///
/// ```rust
/// # extern crate futures;
/// # extern crate mysql_async as my;
/// # extern crate tokio;
/// # extern crate tokio_io;
///
/// # use futures::Future;
/// # use my::prelude::*;
/// # use tokio_io::AsyncRead;
/// # use std::env;
/// # fn main() {
///
/// # pub fn run<F, T, U>(future: F) -> Result<T, U>
/// # where
/// #     F: Future<Item = T, Error = U> + Send + 'static,
/// #     T: Send + 'static,
/// #     U: Send + 'static,
/// # {
/// #     let mut runtime = tokio::runtime::Runtime::new().unwrap();
/// #     let result = runtime.block_on(future);
/// #     runtime.shutdown_on_idle().wait().unwrap();
/// #     result
/// # }
///
/// struct ExampleHandler(&'static [u8]);
///
/// impl LocalInfileHandler for ExampleHandler {
///     fn handle(&self, _: &[u8]) -> Box<Future<Item=Box<AsyncRead + Send>, Error=my::error::Error> + Send> {
///         Box::new(futures::future::ok(Box::new(self.0) as Box<_>))
///     }
/// }
///
/// # let database_url: String = if let Ok(url) = env::var("DATABASE_URL") {
/// #     let opts = my::Opts::from_url(&url).expect("DATABASE_URL invalid");
/// #     if opts.get_db_name().expect("a database name is required").is_empty() {
/// #         panic!("database name is empty");
/// #     }
/// #     url
/// # } else {
/// #     "mysql://root:password@127.0.0.1:3307/mysql".into()
/// # };
///
/// let mut opts = my::OptsBuilder::from_opts(&*database_url);
/// opts.local_infile_handler(Some(ExampleHandler(b"foobar")));
///
/// let pool = my::Pool::new(opts);
///
/// let future = pool.get_conn()
///     .and_then(|conn| conn.drop_query("CREATE TEMPORARY TABLE tmp (a TEXT);"))
///     .and_then(|conn| conn.drop_query("LOAD DATA LOCAL INFILE 'baz' INTO TABLE tmp;"))
///     .and_then(|conn| conn.prep_exec("SELECT * FROM tmp;", ()))
///     .and_then(|result| result.map_and_drop(|row| my::from_row::<(String,)>(row).0))
///     .map(|(_ /* conn */, result)| {
///         assert_eq!(result.len(), 1);
///         assert_eq!(result[0], "foobar");
///     })
///     .and_then(|_| pool.disconnect())
///     .map_err(|err| match err {
///         my::error::Error::Server(ref err) if err.code == 1148 => {
///             // The used command is not allowed with this MySQL version
///         },
///         _ => panic!("{}", err),
///     });
///
///     run(future);
/// # }
/// ```
pub trait LocalInfileHandler: Sync + Send {
    /// `file_name` is the file name in `LOAD DATA LOCAL INFILE '<file name>' INTO TABLE ...;`
    /// query.
    fn handle(&self, file_name: &[u8]) -> BoxFuture<Box<dyn AsyncRead + Send + 'static>>;
}

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
