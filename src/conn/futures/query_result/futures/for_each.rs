use conn::futures::query_result::InnerQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use either::Left;
use either::Right;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::Row;


/// Future that calls `F: FnMut(Row)` on each Row of a `QueryResult`.
///
/// It resolves to an output of corresponding `ResultKind`.
pub struct ForEach<F, T> {
    query_result: T,
    fun: F,
}

pub fn new_new<F, T: Sized>(query_result: T, fun: F) -> ForEach<F, T>
    where F: FnMut(Row),
{
    ForEach {
        query_result: query_result,
        fun: fun,
    }
}

impl<F, T> Future for ForEach<F, T>
where F: FnMut(Row),
      T: InnerQueryResult,
      T: UnconsumedQueryResult,
{
    type Item = T::Output;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.poll()) {
            Left(row) => {
                (&mut self.fun)(row);
                self.poll()
            },
            Right(output) => {
                Ok(Ready(output))
            },
        }
    }
}
