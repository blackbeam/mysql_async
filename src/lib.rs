//! ## mysql-async
//! Tokio based asynchronous MySql client library for rust progrumming language.
//!
//! ### Installation
//! Library hosted on [crates.io](https://crates.io/crates/mysql_async/).
//!
//! ```toml
//! [dependencies]
//! mysql = "<desired version>"
//! ```
//!
//! ### Example
//!
//! ```rust
//! extern crate futures;
//! #[macro_use]
//! extern crate mysql_async as my;
//! extern crate tokio_core as tokio;
//! // ...
//!
//! use futures::Future;
//! use my::prelude::*;
//! use tokio::reactor::Core;
//!
//! #[derive(Debug, PartialEq, Eq)]
//! struct Payment {
//!     customer_id: i32,
//!     amount: i32,
//!     account_name: Option<String>,
//! }
//!
//! fn main() {
//!     let mut lp = Core::new().unwrap();
//!
//!     let payments = vec![
//!         Payment { customer_id: 1, amount: 2, account_name: None },
//!         Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
//!         Payment { customer_id: 5, amount: 6, account_name: None },
//!         Payment { customer_id: 7, amount: 8, account_name: None },
//!         Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
//!     ];
//!
//!     let pool = my::Pool::new("mysql://root:password@localhost:3307", &lp.handle());
//!     let future = pool.get_conn().and_then(|conn| {
//!         // Create temporary table
//!         conn.query(
//!             r"CREATE TEMPORARY TABLE tmp.payment (
//!                 customer_id int not null,
//!                 amount int not null,
//!                 account_name text
//!             )"
//!         ).and_then(|result| result.drop_result())
//!     }).and_then(|conn| {
//!         // Save payments
//!         let params = payments.iter().map(|payment| {
//!             params! {
//!                 "customer_id" => payment.customer_id,
//!                 "amount" => payment.amount,
//!                 "account_name" => payment.account_name.clone(),
//!             }
//!         }).collect();
//!
//!         conn.batch_exec(r"INSERT INTO tmp.payment (customer_id, amount, account_name)
//!                         VALUES (:customer_id, :amount, :account_name)", params)
//!     }).and_then(|conn| {
//!         // Load payments from database.
//!         conn.prep_exec("SELECT customer_id, amount, account_name FROM tmp.payment", ())
//!     }).and_then(|result| {
//!         // Collect payments
//!         result.map(|row| {
//!             let (customer_id, amount, account_name) = my::from_row(row);
//!             Payment {
//!                 customer_id: customer_id,
//!                 amount: amount,
//!                 account_name: account_name,
//!             }
//!         })
//!     }).map(|(payments, _ /* conn */)| {
//!         // Drop connection
//!         payments
//!     });
//!
//!     let loaded_payments = lp.run(future).unwrap();
//!
//!     assert_eq!(loaded_payments, payments);
//! }
//! ```

#![recursion_limit = "1024"]
#![cfg_attr(feature = "nightly", feature(test, const_fn, drop_types_in_const))]

#[cfg(feature = "nightly")]
extern crate test;

#[macro_use]
extern crate bitflags;
extern crate byteorder;
pub extern crate chrono;
pub extern crate either;
#[macro_use]
extern crate error_chain;
extern crate fnv;
#[macro_use]
extern crate futures as lib_futures;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate regex;
extern crate sha1;
pub extern crate time;
extern crate tokio_core as tokio;
extern crate url;

#[cfg(test)]
extern crate env_logger;

#[macro_use]
pub mod macros;
#[macro_use]
mod value;
mod conn;
/// Mysql constants
pub mod consts;
/// Errors used in this crate
pub mod errors;
mod io;
mod opts;
mod proto;
mod scramble;

#[doc(inline)]
pub use self::conn::Conn;

#[doc(inline)]
pub use self::conn::futures::query_result::{
    BinaryResult,
    BinQueryResult,
    ResultSet,
    TextQueryResult,
    TextResult,
};

#[doc(inline)]
pub use self::conn::pool::{
    Pool,
};

#[doc(inline)]
pub use self::conn::stmt::{
    Stmt,
};

#[doc(inline)]
pub use self::opts::{
    Opts,
    OptsBuilder,
};

#[doc(inline)]
pub use self::proto::{
    Column,
    ErrPacket,
    Row,
};

#[doc(inline)]
pub use self::value::{
    ConvIr,
    FromRow,
    FromValue,
    from_row,
    from_row_opt,
    from_value,
    from_value_opt,
    Params,
    ToValue,
    Value,
};

/// Futures used in this crate
pub mod futures {
    #[doc(inline)] pub use conn::futures::BatchExec;
    #[doc(inline)] pub use conn::futures::Disconnect;
    #[doc(inline)] pub use conn::futures::First;
    #[doc(inline)] pub use conn::futures::FirstExec;
    #[doc(inline)] pub use conn::futures::NewConn;
    #[doc(inline)] pub use conn::futures::Ping;
    #[doc(inline)] pub use conn::futures::Prepare;
    #[doc(inline)] pub use conn::futures::PrepExec;
    #[doc(inline)] pub use conn::futures::Query;
    #[doc(inline)] pub use conn::futures::Reset;
    #[doc(inline)] pub use conn::futures::query_result::futures::Collect;
    #[doc(inline)] pub use conn::futures::query_result::futures::CollectAll;
    #[doc(inline)] pub use conn::futures::query_result::futures::DropResult;
    #[doc(inline)] pub use conn::futures::query_result::futures::ForEach;
    #[doc(inline)] pub use conn::futures::query_result::futures::Map;
    #[doc(inline)] pub use conn::futures::query_result::futures::Reduce;
    #[doc(inline)] pub use conn::pool::futures::DisconnectPool;
    #[doc(inline)] pub use conn::pool::futures::GetConn;
    #[doc(inline)] pub use conn::stmt::futures::Batch;
    #[doc(inline)] pub use conn::stmt::futures::Execute;
    #[doc(inline)] pub use conn::stmt::futures::First as StmtFirst;
}

/// Traits used in this crate
pub mod prelude {
    #[doc(inline)] pub use conn::futures::query_result::QueryResult;
    #[doc(inline)] pub use conn::futures::query_result::ResultKind;
    #[doc(inline)] pub use conn::futures::query_result::UnconsumedQueryResult;
}

#[cfg(test)]
mod test_misc {
    use std::env;
    lazy_static! {
        pub static ref DATABASE_URL: String = {
            env::var("DATABASE_URL").unwrap_or("mysql://root:password@127.0.0.1:3307/".into())
        };
    }
}
