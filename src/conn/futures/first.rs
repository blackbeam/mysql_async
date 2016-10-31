use conn::Conn;
use conn::futures::query::Query;
use conn::futures::query_result::futures::CollectAll;
use conn::futures::query_result::ResultSet;
use conn::futures::query_result::TextQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use from_row;
use FromRow;
use Row;
use std::marker::PhantomData;


enum Step {
    WaitForResult(Query),
    CollectingResult(CollectAll<TextQueryResult>),
}

enum Out {
    WaitForResult(TextQueryResult),
    CollectingResult((Vec<ResultSet<Row, TextQueryResult>>, Conn)),
}

/// Future that returns first row of query result.
///
/// It is parametrized by `R: FromRow` and calls `R::from_row(Row)` internally.
pub struct First<R> {
    step: Step,
    _phantom: PhantomData<R>,
}

impl<R> First<R> {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::WaitForResult(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::WaitForResult(val)))
            },
            Step::CollectingResult(ref mut fut) => {
                let val = try_ready!(<CollectAll<TextQueryResult> as Future>::poll(fut));
                Ok(Ready(Out::CollectingResult(val)))
            }
        }
    }
}

pub fn new<R>(query: Query) -> First<R> {
    First {
        step: Step::WaitForResult(query),
        _phantom: PhantomData,
    }
}

impl<R> Future for First<R>
where R: FromRow
{
    type Item = (Option<R>, Conn);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::WaitForResult(query_result) => {
                self.step = Step::CollectingResult(query_result.collect_all());
                self.poll()
            },
            Out::CollectingResult((sets, conn)) => {
                for rows in sets.into_iter() {
                    for row in rows.into_iter() {
                        return Ok(Ready((Some(from_row(row)), conn)));
                    }
                }
                return Ok(Ready((None, conn)))
            }
        }
    }
}
