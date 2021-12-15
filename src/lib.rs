// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! ## mysql-async
//! Tokio based asynchronous MySql client library for The Rust Programming Language.
//!
//! ### Installation
//! The library is hosted on [crates.io](https://crates.io/crates/mysql_async/).
//!
//! ```toml
//! [dependencies]
//! mysql_async = "<desired version>"
//! ```
//!
//! ### Example
//!
//! ```rust
//! # use mysql_async::{Result, test_misc::get_opts};
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
//! async fn main() -> Result<()> {
//!     let payments = vec![
//!         Payment { customer_id: 1, amount: 2, account_name: None },
//!         Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
//!         Payment { customer_id: 5, amount: 6, account_name: None },
//!         Payment { customer_id: 7, amount: 8, account_name: None },
//!         Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
//!     ];
//!
//!     let database_url = /* ... */
//!     # get_opts();
//!
//!     let pool = mysql_async::Pool::new(database_url);
//!     let mut conn = pool.get_conn().await?;
//!
//!     // Create a temporary table
//!     r"CREATE TEMPORARY TABLE payment (
//!         customer_id int not null,
//!         amount int not null,
//!         account_name text
//!     )".ignore(&mut conn).await?;
//!
//!     // Save payments
//!     r"INSERT INTO payment (customer_id, amount, account_name)
//!       VALUES (:customer_id, :amount, :account_name)"
//!         .with(payments.iter().map(|payment| params! {
//!             "customer_id" => payment.customer_id,
//!             "amount" => payment.amount,
//!             "account_name" => payment.account_name.as_ref(),
//!         }))
//!         .batch(&mut conn)
//!         .await?;
//!
//!     // Load payments from the database. Type inference will work here.
//!     let loaded_payments = "SELECT customer_id, amount, account_name FROM payment"
//!         .with(())
//!         .map(&mut conn, |(customer_id, amount, account_name)| Payment { customer_id, amount, account_name })
//!         .await?;
//!
//!     // Dropped connection will go to the pool
//!     drop(conn);
//!
//!     // The Pool must be disconnected explicitly because
//!     // it's an asynchronous operation.
//!     pool.disconnect().await?;
//!
//!     assert_eq!(loaded_payments, payments);
//!
//!     // the async fn returns Result, so
//!     Ok(())
//! }
//! ```

#![recursion_limit = "1024"]
#![cfg_attr(feature = "nightly", feature(test))]

#[cfg(feature = "nightly")]
extern crate test;

pub use mysql_common::{constants as consts, params};

use std::sync::Arc;

mod buffer_pool;

#[macro_use]
mod macros;
mod conn;
mod connection_like;
/// Errors used in this crate
mod error;
mod io;
mod local_infile_handler;
mod opts;
mod query;
mod queryable;

type BoxFuture<'a, T> = futures_core::future::BoxFuture<'a, Result<T>>;

static BUFFER_POOL: once_cell::sync::Lazy<Arc<crate::buffer_pool::BufferPool>> =
    once_cell::sync::Lazy::new(|| Default::default());

#[doc(inline)]
pub use self::conn::{binlog_stream::BinlogStream, Conn};

#[doc(inline)]
pub use self::conn::pool::Pool;

#[doc(inline)]
pub use self::error::{DriverError, Error, IoError, ParseError, Result, ServerError, UrlError};

#[doc(inline)]
pub use self::query::QueryWithParams;

#[doc(inline)]
pub use self::queryable::transaction::IsolationLevel;

#[doc(inline)]
pub use self::opts::{
    Opts, OptsBuilder, PoolConstraints, PoolOpts, SslOpts, DEFAULT_INACTIVE_CONNECTION_TTL,
    DEFAULT_POOL_CONSTRAINTS, DEFAULT_STMT_CACHE_SIZE, DEFAULT_TTL_CHECK_INTERVAL,
};

#[doc(inline)]
pub use self::local_infile_handler::{builtin::WhiteListFsLocalInfileHandler, InfileHandlerFuture};

#[doc(inline)]
pub use mysql_common::packets::{
    binlog_request::BinlogRequest,
    session_state_change::{
        Gtids, Schema, SessionStateChange, SystemVariable, TransactionCharacteristics,
        TransactionState, Unsupported,
    },
    BinlogDumpFlags, Column, Interval, OkPacket, SessionStateInfo, Sid,
};

pub mod binlog {
    #[doc(inline)]
    pub use mysql_common::binlog::consts::*;

    #[doc(inline)]
    pub use mysql_common::binlog::{events, jsonb, jsondiff, row, value};
}

#[doc(inline)]
pub use mysql_common::proto::codec::Compression;

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
pub use self::queryable::query_result::{result_set_stream::ResultSetStream, QueryResult};

#[doc(inline)]
pub use self::queryable::transaction::{Transaction, TxOpts};

#[doc(inline)]
pub use self::queryable::{BinaryProtocol, TextProtocol};

#[doc(inline)]
pub use self::queryable::stmt::Statement;

/// Futures used in this crate
pub mod futures {
    pub use crate::conn::pool::futures::{DisconnectPool, GetConn};
}

/// Traits used in this crate
pub mod prelude {
    #[doc(inline)]
    pub use crate::local_infile_handler::LocalInfileHandler;
    #[doc(inline)]
    pub use crate::query::{BatchQuery, Query, WithParams};
    #[doc(inline)]
    pub use crate::queryable::Queryable;
    #[doc(inline)]
    pub use mysql_common::row::convert::FromRow;
    #[doc(inline)]
    pub use mysql_common::value::convert::{ConvIr, FromValue, ToValue};

    /// Everything that is a statement.
    ///
    /// ```no_run
    /// # use std::{borrow::Cow, sync::Arc};
    /// # use mysql_async::{Statement, prelude::StatementLike};
    /// fn type_is_a_stmt<T: StatementLike>() {}
    ///
    /// type_is_a_stmt::<Cow<'_, str>>();
    /// type_is_a_stmt::<&'_ str>();
    /// type_is_a_stmt::<String>();
    /// type_is_a_stmt::<Box<str>>();
    /// type_is_a_stmt::<Arc<str>>();
    /// type_is_a_stmt::<Statement>();
    ///
    /// fn ref_to_a_clonable_stmt_is_also_a_stmt<T: StatementLike + Clone>() {
    ///     type_is_a_stmt::<&T>();
    /// }
    /// ```
    pub trait StatementLike: crate::queryable::stmt::StatementLike {}
    impl<T: crate::queryable::stmt::StatementLike> StatementLike for T {}

    /// Everything that is a connection.
    ///
    /// Note that you could obtain a `'static` connection by giving away `Conn` or `Pool`.
    pub trait ToConnection<'a, 't: 'a>: crate::connection_like::ToConnection<'a, 't> {}
    // explicitly implemented because of rusdoc
    impl<'a> ToConnection<'a, 'static> for &'a crate::Pool {}
    impl<'a> ToConnection<'static, 'static> for crate::Pool {}
    impl ToConnection<'static, 'static> for crate::Conn {}
    impl<'a> ToConnection<'a, 'static> for &'a mut crate::Conn {}
    impl<'a, 't> ToConnection<'a, 't> for &'a mut crate::Transaction<'t> {}

    /// Trait for protocol markers [`crate::TextProtocol`] and [`crate::BinaryProtocol`].
    pub trait Protocol: crate::queryable::Protocol {}
    impl Protocol for crate::BinaryProtocol {}
    impl Protocol for crate::TextProtocol {}

    pub use mysql_common::params;
}

#[doc(hidden)]
pub mod test_misc {
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
                    .db_name()
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
        let mut builder = OptsBuilder::from_opts(Opts::from_url(&**DATABASE_URL).unwrap());
        if test_ssl() {
            let ssl_opts = SslOpts::default()
                .with_danger_skip_domain_validation(true)
                .with_danger_accept_invalid_certs(true);
            builder = builder.prefer_socket(false).ssl_opts(ssl_opts);
        }
        if test_compression() {
            builder = builder.compression(crate::Compression::default());
        }
        builder
    }

    pub fn test_compression() -> bool {
        ["true", "1"].contains(&&*env::var("COMPRESS").unwrap_or_default())
    }

    pub fn test_ssl() -> bool {
        ["true", "1"].contains(&&*env::var("SSL").unwrap_or_default())
    }
}
