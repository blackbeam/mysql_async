use Conn;

use either::{
    Either,
    Left,
    Right,
};

use errors::*;

use conn::futures::query_result::new as new_text_query_result;

use futures::TextQueryResult;

use lib_futures::{
    Async,
    Future,
    Poll,
};
use lib_futures::Async::Ready;
use lib_futures::stream::Stream;

use MaybeRow;

enum Step {
    NextQueryResult(TextQueryResult),
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
                let val = try_ready!(fut.poll()).expect("TextQueryResult polled twice");
                Ok(Ready(Out::NextQueryResult(val)))
            },
            Step::Dropped(ref mut val) => {
                let conn = val.take().expect("Pooled twice");
                Ok(Ready(Out::Dropped(conn)))
            }
        }
    }
}

pub fn new(mut conn: Conn) -> DropResult {
    match conn.has_result.take() {
        Some((columns, ok_packet, is_bin)) => {
            let text_query_result = new_text_query_result(conn, columns, ok_packet, is_bin);
            let step = Step::NextQueryResult(text_query_result);
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
