use conn::{
    Conn,
};

use errors::*;

use lib_futures::{
    Async,
    Future,
    Poll,
};
use lib_futures::Async::Ready;

use super::{
    NewTextQueryResult,
    TextQueryResult,
    TextQueryResultNew,
    WritePacket,
};

use conn::futures::query_result::{
    RawQueryResult,
    TextResult,
};
use conn::futures::NewRawQueryResult;

enum StepNew {
    WriteCommandData(WritePacket),
    HandleResultSet(NewRawQueryResult<TextResult>),
}

enum OutNew {
    WriteCommandData(Conn),
    HandleResultSet(RawQueryResult<TextResult>),
}

pub struct QueryNew {
    step: StepNew,
}

pub fn new_new(write_packet: WritePacket) -> QueryNew {
    QueryNew {
        step: StepNew::WriteCommandData(write_packet),
    }
}

impl QueryNew {
    fn either_poll(&mut self) -> Result<Async<OutNew>> {
        match self.step {
            StepNew::WriteCommandData(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(OutNew::WriteCommandData(val)))
            },
            StepNew::HandleResultSet(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(OutNew::HandleResultSet(val)))
            }
        }
    }
}

impl Future for QueryNew {
    type Item = TextQueryResultNew;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            OutNew::WriteCommandData(conn) => {
                self.step = StepNew::HandleResultSet(conn.handle_result_set());
                self.poll()
            },
            OutNew::HandleResultSet(raw_query_result) => {
                Ok(Ready(raw_query_result.into()))
            }
        }
    }
}
