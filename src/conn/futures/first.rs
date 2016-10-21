use Conn;
use conn::futures::query::QueryNew;
use conn::futures::query_result::{
    QueryResult,
    ResultSetNew,
    TextQueryResultNew,
};
use conn::futures::query_result::futures::collect_all::CollectAllNew;

use errors::*;

use from_row;

use FromRow;

use futures::{
    CollectAll,
    TextQueryResult,
};

use ResultSet;
use Row;


use lib_futures::{
    Async,
    Future,
    Poll,
};
use lib_futures::Async::Ready;

use std::marker::PhantomData;

enum Step {
    WaitForResult(QueryNew),
    CollectingResult(CollectAllNew<TextQueryResultNew>),
}

enum Out {
    WaitForResult(TextQueryResultNew),
    CollectingResult((Vec<ResultSetNew<Row, TextQueryResultNew>>, Conn)),
}

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
                let val = try_ready!(<CollectAllNew<TextQueryResultNew> as Future>::poll(fut));
                Ok(Ready(Out::CollectingResult(val)))
            }
        }
    }
}

pub fn new<R>(query: QueryNew) -> First<R> {
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
                        return Ok(Async::Ready((Some(from_row(row)), conn)));
                    }
                }
                return Ok(Async::Ready((None, conn)))
            }
        }
    }
}
