// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use errors::*;
use std::fmt;
use std::sync::Arc;
use tokio_io::AsyncRead;

pub mod builtin;

/// Trait used to handle local infile requests.
///
/// Simple handler example:
///
/// ```rust
/// # extern crate futures;
/// # extern crate mysql_async as my;
/// # extern crate tokio_core as tokio;
/// # extern crate tokio_io;
///
/// # use futures::Future;
/// # use my::prelude::*;
/// # use tokio::reactor::Core;
/// # use tokio_io::AsyncRead;
/// # use std::env;
/// # use std::io;
/// # fn main() {
/// struct ExampleHandler(&'static [u8]);
///
/// impl LocalInfileHandler for ExampleHandler {
///     fn handle(&self, _: &[u8]) -> my::errors::Result<Box<AsyncRead>> {
///         Ok(Box::new(self.0))
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
/// let mut lp = Core::new().unwrap();
///
/// let mut opts = my::OptsBuilder::from_opts(&*database_url);
/// opts.local_infile_handler(Some(ExampleHandler(b"foobar")));
///
/// let pool = my::Pool::new(opts, &lp.handle());
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
///     .and_then(|_| pool.disconnect());
///
/// lp.run(future).unwrap();
/// # }
/// ```
pub trait LocalInfileHandler {
    /// `file_name` is the file name in `LOAD DATA LOCAL INFILE '<file name>' INTO TABLE ...;`
    /// query.
    fn handle(&self, file_name: &[u8]) -> Result<Box<AsyncRead>>;
}

/// Object used to wrap `T: LocalInfileHandler` inside of Opts.
#[derive(Clone)]
pub struct LocalInfileHandlerObject(Arc<LocalInfileHandler>);

impl LocalInfileHandlerObject {
    pub fn new<T: LocalInfileHandler + 'static>(handler: T) -> Self
    {
        LocalInfileHandlerObject(Arc::new(handler))
    }

    pub fn clone_inner(&self) -> Arc<LocalInfileHandler> {
        self.0.clone()
    }
}

impl PartialEq for LocalInfileHandlerObject {
    fn eq(&self, other: &LocalInfileHandlerObject) -> bool {
        self.0.as_ref() as *const LocalInfileHandler ==
            other.0.as_ref() as *const LocalInfileHandler
    }
}

impl Eq for LocalInfileHandlerObject {}

impl fmt::Debug for LocalInfileHandlerObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Local infile handler object")
    }
}
