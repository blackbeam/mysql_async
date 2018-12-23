// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! ## mysql-async
//! Tokio based asynchronous MySql client library for rust programming language.
//!
//! ### Installation
//! Library hosted on [crates.io](https://crates.io/crates/mysql_async/).
//!
//! ```toml
//! [dependencies]
//! mysql_async = "<desired version>"
//! ```
//!
//! ### Example
//!
//! ```rust
//! extern crate futures;
//! #[macro_use]
//! extern crate mysql_async as my;
//! extern crate tokio;
//! // ...
//!
//! use futures::Future;
//! use my::prelude::*;
//! # use std::env;
//!
//! #[derive(Debug, PartialEq, Eq, Clone)]
//! struct Payment {
//!     customer_id: i32,
//!     amount: i32,
//!     account_name: Option<String>,
//! }
//!
//! /// Same as `tokio::run`, but will panic if future panics and will return the result
//! /// of future execution.
//! pub fn run<F, T, U>(future: F) -> Result<T, U>
//! where
//!     F: Future<Item = T, Error = U> + Send + 'static,
//!     T: Send + 'static,
//!     U: Send + 'static,
//! {
//!     let mut runtime = tokio::runtime::Runtime::new().unwrap();
//!     let result = runtime.block_on(future);
//!     runtime.shutdown_on_idle().wait().unwrap();
//!     result
//! }
//!
//! fn main() {
//!     let payments = vec![
//!         Payment { customer_id: 1, amount: 2, account_name: None },
//!         Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
//!         Payment { customer_id: 5, amount: 6, account_name: None },
//!         Payment { customer_id: 7, amount: 8, account_name: None },
//!         Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
//!     ];
//!     let payments_clone = payments.clone();
//!
//!     # let database_url: String = if let Ok(url) = env::var("DATABASE_URL") {
//!     #     let opts = my::Opts::from_url(&url).expect("DATABASE_URL invalid");
//!     #     if opts.get_db_name().expect("a database name is required").is_empty() {
//!     #         panic!("database name is empty");
//!     #     }
//!     #     url
//!     # } else {
//!     #     "mysql://root:password@127.0.0.1:3307/mysql".into()
//!     # };
//!
//!     let pool = my::Pool::new(database_url);
//!     let future = pool.get_conn().and_then(|conn| {
//!         // Create temporary table
//!         conn.drop_query(
//!             r"CREATE TEMPORARY TABLE payment (
//!                 customer_id int not null,
//!                 amount int not null,
//!                 account_name text
//!             )"
//!         )
//!     }).and_then(move |conn| {
//!         // Save payments
//!         let params = payments_clone.into_iter().map(|payment| {
//!             params! {
//!                 "customer_id" => payment.customer_id,
//!                 "amount" => payment.amount,
//!                 "account_name" => payment.account_name.clone(),
//!             }
//!         });
//!
//!         conn.batch_exec(r"INSERT INTO payment (customer_id, amount, account_name)
//!                         VALUES (:customer_id, :amount, :account_name)", params)
//!     }).and_then(|conn| {
//!         // Load payments from database.
//!         conn.prep_exec("SELECT customer_id, amount, account_name FROM payment", ())
//!     }).and_then(|result| {
//!         // Collect payments
//!         result.map_and_drop(|row| {
//!             let (customer_id, amount, account_name) = my::from_row(row);
//!             Payment {
//!                 customer_id: customer_id,
//!                 amount: amount,
//!                 account_name: account_name,
//!             }
//!         })
//!     }).and_then(|(_ /* conn */, payments)| {
//!         // The destructor of a connection will return it to the pool,
//!         // but pool should be disconnected explicitly because it's
//!         // an asynchronous procedure.
//!         pool.disconnect().map(|_| payments)
//!     });
//!
//!     let loaded_payments = run(future).unwrap();
//!     assert_eq!(loaded_payments, payments);
//! }
//! ```

#![recursion_limit = "1024"]
#![cfg_attr(feature = "nightly", feature(test, const_fn))]

#[cfg(feature = "nightly")]
extern crate test;

extern crate bit_vec;
extern crate byteorder;
extern crate bytes;
#[macro_use]
extern crate error_chain;
extern crate fnv;
#[macro_use]
extern crate futures as lib_futures;
#[cfg(test)]
#[macro_use]
extern crate lazy_static;
extern crate mio;
extern crate mysql_common as myc;
#[cfg(feature = "ssl")]
extern crate native_tls;
extern crate regex;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate tokio_codec;
extern crate tokio_io;
extern crate twox_hash;
extern crate url;

pub use myc::{chrono, constants as consts, time, uuid};

// Until `macro_reexport` stabilisation.
/// This macro is a convenient way to pass named parameters to a statement.
///
/// ```ignore
/// let foo = 42;
/// conn.prep_exec("SELECT :foo, :foo2x", params! {
///     foo,
///     "foo2x" => foo * 2,
/// });
/// ```
#[macro_export]
macro_rules! params {
    () => {};
    (@to_pair $name:expr => $value:expr) => (
        (::std::string::String::from($name), $crate::Value::from($value))
    );
    (@to_pair $name:ident) => (
        (::std::string::String::from(stringify!($name)), $crate::Value::from($name))
    );
    (@expand $vec:expr;) => {};
    (@expand $vec:expr; $name:expr => $value:expr, $($tail:tt)*) => {
        $vec.push(params!(@to_pair $name => $value));
        params!(@expand $vec; $($tail)*);
    };
    (@expand $vec:expr; $name:expr => $value:expr $(, $tail:tt)*) => {
        $vec.push(params!(@to_pair $name => $value));
        params!(@expand $vec; $($tail)*);
    };
    (@expand $vec:expr; $name:ident, $($tail:tt)*) => {
        $vec.push(params!(@to_pair $name));
        params!(@expand $vec; $($tail)*);
    };
    (@expand $vec:expr; $name:ident $(, $tail:tt)*) => {
        $vec.push(params!(@to_pair $name));
        params!(@expand $vec; $($tail)*);
    };
    ($i:ident, $($tail:tt)*) => {
        {
            let mut output = ::std::vec::Vec::new();
            params!(@expand output; $i, $($tail)*);
            output
        }
    };
    ($i:expr => $($tail:tt)*) => {
        {
            let mut output = ::std::vec::Vec::new();
            params!(@expand output; $i => $($tail)*);
            output
        }
    };
    ($i:ident) => {
        {
            let mut output = ::std::vec::Vec::new();
            params!(@expand output; $i);
            output
        }
    }
}

#[macro_use]
pub mod macros;
mod conn;
mod connection_like;
/// Errors used in this crate
pub mod errors;
mod io;
mod local_infile_handler;
mod opts;
mod queryable;

pub type BoxFuture<T> = Box<lib_futures::Future<Item = T, Error = errors::Error> + Send + 'static>;

/// Alias for `Future` with library error as `Future::Error`.
pub trait MyFuture<T>:
    lib_futures::Future<Item = T, Error = errors::Error> + Send + 'static
{
}
impl<T, U> MyFuture<T> for U where
    U: lib_futures::Future<Item = T, Error = errors::Error> + Send + 'static
{
}

#[doc(inline)]
pub use self::conn::Conn;

#[doc(inline)]
pub use self::conn::pool::Pool;

#[doc(inline)]
pub use self::queryable::transaction::IsolationLevel;

#[doc(inline)]
pub use self::opts::{Opts, OptsBuilder, SslOpts};

#[doc(inline)]
pub use self::local_infile_handler::builtin::WhiteListFsLocalInfileHandler;

#[doc(inline)]
pub use myc::packets::Column;

#[doc(inline)]
pub use myc::row::Row;

#[doc(inline)]
pub use myc::params::Params;

#[doc(inline)]
pub use myc::value::Value;

#[doc(inline)]
pub use myc::row::convert::{from_row, from_row_opt, FromRowError};

#[doc(inline)]
pub use myc::value::convert::{from_value, from_value_opt, FromValueError};

#[doc(inline)]
pub use myc::value::json::{Deserialized, Serialized};

#[doc(inline)]
pub use self::queryable::query_result::QueryResult;

#[doc(inline)]
pub use self::queryable::transaction::{Transaction, TransactionOptions};

#[doc(inline)]
pub use self::queryable::{BinaryProtocol, TextProtocol};

#[doc(inline)]
pub use self::queryable::stmt::Stmt;

/// Futures used in this crate
pub mod futures {
    pub use queryable::query_result::{
        ForEach, ForEachAndDrop, Map, MapAndDrop, Reduce, ReduceAndDrop,
    };
}

/// Traits used in this crate
pub mod prelude {
    #[doc(inline)]
    pub use local_infile_handler::LocalInfileHandler;
    #[doc(inline)]
    pub use myc::row::convert::FromRow;
    #[doc(inline)]
    pub use myc::value::convert::{ConvIr, FromValue, ToValue};
    #[doc(inline)]
    pub use queryable::Queryable;
}

#[cfg(test)]
mod test_misc {
    use opts;
    use std::env;
    lazy_static! {
        pub static ref DATABASE_URL: String = {
            if let Ok(url) = env::var("DATABASE_URL") {
                let opts = opts::Opts::from_url(&url).expect("DATABASE_URL invalid");
                if opts
                    .get_db_name()
                    .expect("a database name is required")
                    .is_empty()
                {
                    panic!("database name is empty");
                }
                url
            } else {
                "mysql://root:password@127.0.0.1:3307/mysql".into()
            }
        };
    }
}
