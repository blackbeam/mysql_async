// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::futures::query_result::futures::DropResult as DropQueryResult;
use conn::futures::query_result::TextQueryResult;
use lib_futures::AndThen;

mod batch_exec;
mod columns;
mod disconnect;
mod drop_result;
mod first;
mod first_exec;
mod new_conn;
mod new_raw_query_result;
mod ping;
mod prepare;
mod prep_exec;
mod query;
pub mod query_result;
mod read_max_allowed_packet;
mod read_packet;
mod reset;
mod send_long_data;
mod write_packet;

pub use self::batch_exec::BatchExec;
pub use self::batch_exec::new as new_batch_exec;

pub use self::columns::Columns;
pub use self::columns::new as new_columns;

pub use self::disconnect::Disconnect;
pub use self::disconnect::new as new_disconnect;

pub use self::drop_result::DropResult;
pub use self::drop_result::new as new_drop_result;

pub use self::first::First;
pub use self::first::new as new_first;

pub use self::first_exec::FirstExec;
pub use self::first_exec::new as new_first_exec;

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

pub use self::read_max_allowed_packet::ReadMaxAllowedPacket;
pub use self::read_max_allowed_packet::new as new_read_max_allowed_packet;

pub use self::read_packet::ReadPacket;
pub use self::read_packet::new as new_read_packet;

pub use self::reset::Reset;
pub use self::reset::new as new_reset;

pub use self::send_long_data::SendLongData;
pub use self::send_long_data::new as new_send_long_data;

pub use self::write_packet::WritePacket;
pub use self::write_packet::new as new_write_packet;

pub type DropQuery = AndThen<
    Query,
    DropQueryResult<TextQueryResult>,
    fn(TextQueryResult) -> DropQueryResult<TextQueryResult>
>;