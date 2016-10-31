use conn::futures::query_result::InnerQueryResult;
use conn::futures::query_result::QueryResultOutput;
use conn::futures::query_result::ResultSet;
use conn::futures::query_result::UnconsumedQueryResult;
use either::Left;
use either::Right;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::Row;
use std::mem;


/// Future that collects all result sets of all results of this query or statement execution.
///
/// It makes sense only for multi-result sets and collects all of it's results.
/// It resolves to a pair of `Vec<ResultSet>` and `Conn` or `Stmt` depending on `T: ResultKind`.
pub struct CollectAll<T> {
    vec: Vec<ResultSet<Row, T>>,
    row_vec: Vec<Row>,
    query_result: Option<T>,
}

pub fn new_new<T: Sized>(query_result: T) -> CollectAll<T> {
    CollectAll {
        vec: Vec::new(),
        row_vec: Vec::new(),
        query_result: Some(query_result),
    }
}

impl<T, F> Future for CollectAll<T>
where T: InnerQueryResult,
      T: UnconsumedQueryResult,
      T::Output: QueryResultOutput<Payload=T, Output=F>,
{
    type Item = (Vec<ResultSet<Row, T>>, F);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.as_mut().unwrap().poll()) {
            Left(row) => {
                self.row_vec.push(row);
                self.poll()
            },
            Right(output) => {
                let set = mem::replace(&mut self.row_vec, Vec::new());
                match output.into_payload_or_output() {
                    Left(next_query_result) => {
                        let old_query_result = mem::replace(self.query_result.as_mut().unwrap(),
                                                            next_query_result);
                        self.vec.push(ResultSet(set, old_query_result));
                        self.poll()
                    },
                    Right(conn) => {
                        let query_result = self.query_result.take().unwrap();
                        self.vec.push(ResultSet(set, query_result));
                        let vec = mem::replace(&mut self.vec, Vec::new());
                        Ok(Ready((vec, conn)))
                    }
                }
            }
        }
    }
}
