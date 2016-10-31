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
    TextQueryResult,
    BinQueryResult,
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
    #[doc(inline)] pub use conn::futures::Disconnect;
    #[doc(inline)] pub use conn::futures::First;
    #[doc(inline)] pub use conn::futures::NewConn;
    #[doc(inline)] pub use conn::futures::Ping;
    #[doc(inline)] pub use conn::futures::Prepare;
    #[doc(inline)] pub use conn::futures::Query;
    #[doc(inline)] pub use conn::futures::Reset;
    #[doc(inline)] pub use conn::futures::query_result::BinaryResult;
    #[doc(inline)] pub use conn::futures::query_result::ResultSet;
    #[doc(inline)] pub use conn::futures::query_result::TextResult;
    #[doc(inline)] pub use conn::futures::query_result::futures::Collect;
    #[doc(inline)] pub use conn::futures::query_result::futures::CollectAll;
    #[doc(inline)] pub use conn::futures::query_result::futures::ForEach;
    #[doc(inline)] pub use conn::futures::query_result::futures::Map;
    #[doc(inline)] pub use conn::futures::query_result::futures::Reduce;
    #[doc(inline)] pub use conn::stmt::futures::Execute;
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
