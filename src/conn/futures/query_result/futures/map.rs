use conn::futures::query_result::InnerQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use either::Left;
use either::Right;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::Row;
use std::mem;


/// Future that calls `F: FnMut(Row) -> U` on each Row of a `QueryResult`.
///
/// It resolves to a pair of `Vec<U>` an output of corresponding `ResultKind`.
pub struct Map<F, U, T> {
    query_result: T,
    fun: F,
    acc: Vec<U>,
}

pub fn new_new<F, U, T>(query_result: T, fun: F) -> Map<F, U, T>
    where F: FnMut(Row) -> U,
{
    Map {
        query_result: query_result,
        fun: fun,
        acc: Vec::new(),
    }
}

impl<F, U, T> Future for Map<F, U, T>
where F: FnMut(Row) -> U,
      T: InnerQueryResult,
      T: UnconsumedQueryResult,
{
    type Item = (Vec<U>, T::Output);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.poll()) {
            Left(row) => {
                let val = (&mut self.fun)(row);
                self.acc.push(val);
                self.poll()
            },
            Right(output) => {
                let acc = mem::replace(&mut self.acc, Vec::new());
                Ok(Ready((acc, output)))
            }
        }
    }
}
