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
//! use mysql_async::prelude::*;
//! # use std::env;
//!
//! #[derive(Debug, PartialEq, Eq, Clone)]
//! struct Payment {
//!     customer_id: i32,
//!     amount: i32,
//!     account_name: Option<String>,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), mysql_async::error::Error> {
//!     let payments = vec![
//!         Payment { customer_id: 1, amount: 2, account_name: None },
//!         Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
//!         Payment { customer_id: 5, amount: 6, account_name: None },
//!         Payment { customer_id: 7, amount: 8, account_name: None },
//!         Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
//!     ];
//!     let payments_clone = payments.clone();
//!
//!     let database_url = /* ... */
//!     # if let Ok(url) = env::var("DATABASE_URL") {
//!     #     let opts = mysql_async::Opts::from_url(&url).expect("DATABASE_URL invalid");
//!     #     if opts.get_db_name().expect("a database name is required").is_empty() {
//!     #         panic!("database name is empty");
//!     #     }
//!     #     url
//!     # } else {
//!     #     "mysql://root:password@127.0.0.1:3307/mysql".to_string()
//!     # };
//!
//!     let pool = mysql_async::Pool::new(database_url);
//!     let conn = pool.get_conn().await?;
//!
//!     // Create temporary table
//!     let conn = conn.drop_query(
//!         r"CREATE TEMPORARY TABLE payment (
//!             customer_id int not null,
//!             amount int not null,
//!             account_name text
//!         )"
//!     ).await?;
//!
//!     // Save payments
//!     let params = payments_clone.into_iter().map(|payment| {
//!         params! {
//!             "customer_id" => payment.customer_id,
//!             "amount" => payment.amount,
//!             "account_name" => payment.account_name.clone(),
//!         }
//!     });
//!
//!     let conn = conn.batch_exec(r"INSERT INTO payment (customer_id, amount, account_name)
//!                     VALUES (:customer_id, :amount, :account_name)", params).await?;
//!
//!     // Load payments from database.
//!     let result = conn.prep_exec("SELECT customer_id, amount, account_name FROM payment", ()).await?;
//!
//!     // Collect payments
//!     let (_ /* conn */, loaded_payments) = result.map_and_drop(|row| {
//!         let (customer_id, amount, account_name) = mysql_async::from_row(row);
//!         Payment {
//!             customer_id: customer_id,
//!             amount: amount,
//!             account_name: account_name,
//!         }
//!     }).await?;
//!
//!     // The destructor of a connection will return it to the pool,
//!     // but pool should be disconnected explicitly because it's
//!     // an asynchronous procedure.
//!     pool.disconnect().await?;
//!
//!     assert_eq!(loaded_payments, payments);
//!
//!     // the async fn returns Result, so
//!     Ok(())
//! }
//! ```

#![recursion_limit = "1024"]
#![cfg_attr(feature = "nightly", feature(test, const_fn))]

#[cfg(feature = "nightly")]
extern crate test;

pub use mysql_common::{chrono, constants as consts, params, time, uuid};

#[macro_use]
mod macros;
mod conn;
mod connection_like;
/// Errors used in this crate
pub mod error;
mod io;
mod local_infile_handler;
mod opts;
mod queryable;

pub type BoxFuture<T> = ::std::pin::Pin<
    Box<dyn ::std::future::Future<Output = Result<T, error::Error>> + Send + 'static>,
>;

#[doc(inline)]
pub use self::conn::Conn;

#[doc(inline)]
pub use self::conn::pool::Pool;

#[doc(inline)]
pub use self::queryable::transaction::IsolationLevel;

#[doc(inline)]
pub use self::opts::{Opts, OptsBuilder, PoolConstraints, SslOpts};

#[doc(inline)]
pub use self::local_infile_handler::{builtin::WhiteListFsLocalInfileHandler, InfileHandlerFuture};

#[doc(inline)]
pub use mysql_common::packets::Column;

#[doc(inline)]
pub use mysql_common::row::Row;

#[doc(inline)]
pub use mysql_common::params::Params;

#[doc(inline)]
pub use mysql_common::value::Value;

#[doc(inline)]
pub use mysql_common::row::convert::{from_row, from_row_opt, FromRowError};

#[doc(inline)]
pub use mysql_common::value::convert::{from_value, from_value_opt, FromValueError};

#[doc(inline)]
pub use mysql_common::value::json::{Deserialized, Serialized};

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
    pub use crate::conn::pool::futures::{DisconnectPool, GetConn};
}

/// Traits used in this crate
pub mod prelude {
    #[doc(inline)]
    pub use crate::local_infile_handler::LocalInfileHandler;
    #[doc(inline)]
    pub use crate::queryable::Queryable;
    #[doc(inline)]
    pub use mysql_common::row::convert::FromRow;
    #[doc(inline)]
    pub use mysql_common::value::convert::{ConvIr, FromValue, ToValue};

    /// Everything that is connection.
    pub trait ConnectionLike: crate::connection_like::ConnectionLike {}
    impl<T: crate::connection_like::ConnectionLike> ConnectionLike for T {}

    /// Trait for protocol markers [`TextProtocol`] and [`BinaryProtocol`].
    pub trait Protocol: crate::queryable::Protocol {}
    impl<T: crate::queryable::Protocol> Protocol for T {}

    pub use mysql_common::params;
}

#[cfg(test)]
mod test_misc {
    use lazy_static::lazy_static;

    use std::env;

    use crate::opts::{Opts, OptsBuilder, SslOpts};

    #[allow(dead_code)]
    fn error_should_implement_send_and_sync() {
        fn _dummy<T: Send + Sync>(_: T) {}
        _dummy(crate::error::Error::from("foo"));
    }

    lazy_static! {
        pub static ref DATABASE_URL: String = {
            if let Ok(url) = env::var("DATABASE_URL") {
                let opts = Opts::from_url(&url).expect("DATABASE_URL invalid");
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

    pub fn get_opts() -> OptsBuilder {
        let mut builder = OptsBuilder::from_opts(&**DATABASE_URL);
        // to suppress warning on unused mut
        builder.stmt_cache_size(None);
        if ["true", "1"].contains(&&*env::var("SSL").unwrap_or("".into())) {
            builder.prefer_socket(false);
            let mut ssl_opts = SslOpts::default();
            ssl_opts.set_danger_skip_domain_validation(true);
            ssl_opts.set_danger_accept_invalid_certs(true);
            builder.ssl_opts(ssl_opts);
        }
        builder
    }
}
