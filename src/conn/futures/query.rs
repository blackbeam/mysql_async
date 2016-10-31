use Conn;
use conn::futures::new_raw_query_result::NewRawQueryResult;
use conn::futures::query_result::RawQueryResult;
use conn::futures::query_result::TextQueryResult;
use conn::futures::query_result::TextResult;
use conn::futures::write_packet::WritePacket;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;


enum Step {
    WriteCommandData(WritePacket),
    HandleResultSet(NewRawQueryResult<TextResult>),
}

enum Out {
    WriteCommandData(Conn),
    HandleResultSet(RawQueryResult<TextResult>),
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

impl Query {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::WriteCommandData(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::WriteCommandData(val)))
            },
            Step::HandleResultSet(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::HandleResultSet(val)))
            }
        }
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
