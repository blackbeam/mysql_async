use conn::Conn;
use conn::futures::query_result::{
    QueryResult,
    QueryResultOutput,
};

use either::{
    Left,
    Right,
};

use errors::*;

use lib_futures::{
    Async,
    Future,
    Poll,
};
use lib_futures::Async::Ready;
use lib_futures::stream::Stream;

use proto::Row;

use std::mem;

use super::super::{
    MaybeRow,
    TextQueryResult,
    ResultSet,
    ResultSetNew,
};

pub struct CollectAllNew<T: QueryResult> {
    vec: Vec<ResultSetNew<Row, T>>,
    row_vec: Vec<Row>,
    query_result: Option<T>,
}

pub fn new_new<T: Sized>(query_result: T) -> CollectAllNew<T>
    where T: QueryResult
{
    CollectAllNew {
        vec: Vec::new(),
        row_vec: Vec::new(),
        query_result: Some(query_result),
    }
}

impl<T> Future for CollectAllNew<T>
where T: QueryResult,
      T::Output: QueryResultOutput<Payload=T>,
{
    type Item = (Vec<ResultSetNew<Row, T>>, Conn);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.as_mut().unwrap().poll()) {
            Left(row) => {
                self.row_vec.push(row);
                self.poll()
            },
            Right(output) => {
                let set = mem::replace(&mut self.row_vec, Vec::new());
                match output.into_next_or_conn() {
                    Left(next_query_result) => {
                        let old_query_result = mem::replace(self.query_result.as_mut().unwrap(),
                                                            next_query_result);
                        self.vec.push(ResultSetNew(set, old_query_result));
                        self.poll()
                    },
                    Right(conn) => {
                        let query_result = self.query_result.take().unwrap();
                        self.vec.push(ResultSetNew(set, query_result));
                        let vec = mem::replace(&mut self.vec, Vec::new());
                        Ok(Ready((vec, conn)))
                    }
                }
            }
        }
    }
}

pub struct CollectAll {
    vec: Vec<ResultSet<Row>>,
    row_vec: Vec<Row>,
    query_result: Option<TextQueryResult>,
}


pub fn new(query_result: TextQueryResult) -> CollectAll {
    CollectAll {
        vec: Vec::new(),
        row_vec: Vec::new(),
        query_result: Some(query_result),
    }
}

impl Future for CollectAll {
    type Item = (Vec<ResultSet<Row>>, Conn);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.as_mut().unwrap().poll()) {
            Some(MaybeRow::Row(row)) => {
                self.row_vec.push(row);
                self.poll()
            },
            Some(MaybeRow::End(next_result_set_or_row)) => {
                let set = mem::replace(&mut self.row_vec, Vec::new());
                match next_result_set_or_row {
                    Left(query_result) => {
                        let query_result = mem::replace(self.query_result.as_mut().unwrap(),
                                                        query_result);
                        self.vec.push(ResultSet(set, query_result));
                        self.poll()
                    },
                    Right(conn) => {
                        let query_result = self.query_result.take().unwrap();
                        self.vec.push(ResultSet(set, query_result));
                        let vec = mem::replace(&mut self.vec, Vec::new());
                        Ok(Async::Ready((vec, conn)))
                    },
                }
            },
            None => unreachable!(),
        }
    }
}
