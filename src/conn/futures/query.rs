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
    WritePacket,
};

enum Step {
    WriteCommandData(WritePacket),
    HandleTextResultset(NewTextQueryResult),
}

enum Out {
    WriteCommandData(Conn),
    HandleTextResultset(TextQueryResult),
}

pub struct Query {
    step: Step,
}

pub fn new(write_packet: WritePacket) -> Query {
    Query {
        step: Step::WriteCommandData(write_packet),
    }
}

impl Query {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::WriteCommandData(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::WriteCommandData(val)))
            },
            Step::HandleTextResultset(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::HandleTextResultset(val)))
            },
        }
    }
}

impl Future for Query {
    type Item = TextQueryResult;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::WriteCommandData(conn) => {
                self.step = Step::HandleTextResultset(conn.handle_text_resultset());
                self.poll()
            },
            Out::HandleTextResultset(query_result) => Ok(Ready(query_result)),
        }
    }
}
