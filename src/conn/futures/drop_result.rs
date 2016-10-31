use Conn;

use either::{
    Either,
    Left,
    Right,
};

use errors::*;

use conn::futures::query_result::BinaryResult;
use conn::futures::query_result::new_raw as new_raw_query_result;
use conn::futures::query_result::QueryResult;
use conn::futures::query_result::TextQueryResultNew;
use conn::futures::query_result::TextResult;

use lib_futures::{
    Async,
    Future,
    Poll,
};
use lib_futures::Async::Ready;
use lib_futures::stream::Stream;

use MaybeRow;

enum Step {
    NextQueryResult(Box<QueryResult>),
    Dropped(Option<Conn>),
}

enum Out {
    NextQueryResult(MaybeRow),
    Dropped(Conn),
}

pub struct DropResult {
    step: Step,
}

impl DropResult {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::NextQueryResult(ref mut fut) => {
                let val = try_ready!(fut.poll()).expect("QueryResult polled twice");
                Ok(Ready(Out::NextQueryResult(val)))
            },
            Step::Dropped(ref mut val) => {
                let conn = val.take().expect("DropResult pooled twice");
                Ok(Ready(Out::Dropped(conn)))
            }
        }
    }
}

pub fn new(mut conn: Conn) -> DropResult {
    match conn.has_result.take() {
        Some((columns, ok_packet, inner_stmt)) => {
            let query_result: Box<QueryResult> = if inner_stmt.is_some() {
                let query_result = new_raw_query_result::<BinaryResult, _>(conn,
                                                                           columns,
                                                                           ok_packet,
                                                                           inner_stmt);
                Box::new(query_result)
            } else {
                let query_result = new_raw_query_result::<TextResult, _>(conn,
                                                                         columns,
                                                                         ok_packet,
                                                                         inner_stmt);
                Box::new(query_result)
            };
            let step = Step::NextQueryResult(query_result);
            DropResult {
                step: step,
            }
        },
        None => {
            DropResult {
                step: Step::Dropped(Some(conn)),
            }
        }
    }
}

impl Future for DropResult {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::NextQueryResult(MaybeRow::Row(_)) => {
                self.poll()
            }
            Out::NextQueryResult(MaybeRow::End(Left(text_result_set))) => {
                self.step = Step::NextQueryResult(text_result_set);
                self.poll()
            },
            Out::NextQueryResult(MaybeRow::End(Right(conn))) => {
                Ok(Ready(conn))
            },
            Out::Dropped(conn) => {
                Ok(Ready(conn))
            }
        }
    }
}
