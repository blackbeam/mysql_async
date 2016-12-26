// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::query_result::futures::DropResult as DropQueryResult;
use conn::futures::query_result::BinQueryResult;
use conn::futures::query_result::TextQueryResult;
use conn::stmt::Stmt;
use lib_futures::AndThen;
use lib_futures::Map;

mod batch_exec;
mod columns;
mod drop_result;
mod first;
mod first_exec;
mod handle_local_infile;
mod new_conn;
mod new_raw_query_result;
mod ping;
mod prepare;
mod prep_exec;
mod query;
pub mod query_result;
mod read_packet;
mod reset;
mod send_long_data;
mod write_packet;

pub use self::batch_exec::BatchExec;
pub use self::batch_exec::new as new_batch_exec;

pub use self::columns::Columns;
pub use self::columns::new as new_columns;

pub use self::drop_result::DropResult;
pub use self::drop_result::new as new_drop_result;

pub use self::first::First;
pub use self::first::new as new_first;

pub use self::first_exec::FirstExec;
pub use self::first_exec::new as new_first_exec;

pub use self::handle_local_infile::HandleLocalInfile;
pub use self::handle_local_infile::new as new_handle_local_infile;

pub use self::new_conn::NewConn;
pub use self::new_conn::new as new_new_conn;

pub use self::new_raw_query_result::NewRawQueryResult;
pub use self::new_raw_query_result::new as new_new_raw_query_result;

pub use self::ping::Ping;
pub use self::ping::new as new_ping;

pub use self::prepare::Prepare;
pub use self::prepare::new as new_prepare;

pub use self::prep_exec::PrepExec;
pub use self::prep_exec::new as new_prep_exec;

pub use self::query::Query;
pub use self::query::new_new as new_query;

pub use self::read_packet::ReadPacket;
pub use self::read_packet::new as new_read_packet;

pub use self::reset::Reset;
pub use self::reset::new as new_reset;

pub use self::send_long_data::SendLongData;
pub use self::send_long_data::new as new_send_long_data;

pub use self::write_packet::WritePacket;
pub use self::write_packet::new as new_write_packet;

/// Future that executes query, drops result and resolves to `Conn`.
pub type DropQuery = AndThen<Query,
                             DropQueryResult<TextQueryResult>,
                             fn(TextQueryResult) -> DropQueryResult<TextQueryResult>>;

/// Future that executes statement, drops result and resolves to `Conn`.
pub type DropExec = Map<AndThen<PrepExec,
                                DropQueryResult<BinQueryResult>,
                                fn(BinQueryResult) -> DropQueryResult<BinQueryResult>>,
                        fn(Stmt) -> Conn>;

/// Future that disconnects `Conn` from server and resolves to `()`.
pub type Disconnect = Map<WritePacket, fn(Conn) -> ()>;

/// Future that resolves to `Conn` with `wait_timeout` stored in it.
pub type ReadWaitTimeout = Map<First<(u32,)>, fn((Option<(u32,)>, Conn)) -> Conn>;

/// Future that resolves to `Conn` with value of MySql's max_allowed_packet stored in it.
pub type ReadMaxAllowedPacket = Map<First<(u64,)>, fn((Option<(u64,)>, Conn)) -> Conn>;
