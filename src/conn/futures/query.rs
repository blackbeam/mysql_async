// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::futures::new_raw_query_result::NewRawQueryResult;
use conn::futures::query_result::TextQueryResult;
use conn::futures::query_result::TextResult;
use conn::futures::write_packet::WritePacket;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;


steps! {
    Query {
        WriteCommandData(WritePacket),
        HandleResultSet(NewRawQueryResult<TextResult>),
    }
}

/// Future that resolves to result of a query execution.
pub struct Query {
    step: Step,
}

pub fn new_new(write_packet: WritePacket) -> Query {
    Query {
        step: Step::WriteCommandData(write_packet),
    }
}

impl Future for Query {
    type Item = TextQueryResult;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::WriteCommandData(conn) => {
                self.step = Step::HandleResultSet(conn.handle_result_set(None));
                self.poll()
            },
            Out::HandleResultSet(raw_query_result) => {
                Ok(Ready(raw_query_result.into()))
            }
        }
    }
}
