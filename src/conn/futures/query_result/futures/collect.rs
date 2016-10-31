use conn::futures::query_result::InnerQueryResult;
use conn::futures::query_result::ResultSet;
use conn::futures::query_result::UnconsumedQueryResult;
use either::Left;
use either::Right;
use errors::*;
use from_row;
use FromRow;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use std::mem;


/// Future that collects result of a query or statement execution.
///
/// It resolves to a pair of `ResultSet` and to an output of corresponding `ResultKind`.
pub struct Collect<R, T> {
    vec: Vec<R>,
    query_result: Option<T>,
}

pub fn new_new<R, T>(query_result: T) -> Collect<R, T> {
    Collect {
        vec: Vec::new(),
        query_result: Some(query_result),
    }
}

impl<R, T> Future for Collect<R, T>
where R: FromRow,
      T: InnerQueryResult,
      T: UnconsumedQueryResult,
{
    type Item = (ResultSet<R, T>, T::Output);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.as_mut().unwrap().poll()) {
            Left(row) => {
                self.vec.push(from_row::<R>(row));
                self.poll()
            },
            Right(output) => {
                let query_result = self.query_result.take().unwrap();
                let vec = mem::replace(&mut self.vec, Vec::new());
                Ok(Ready((ResultSet(vec, query_result), output)))
            }
        }
    }
}
