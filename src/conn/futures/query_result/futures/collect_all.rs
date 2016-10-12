use conn::Conn;

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
use lib_futures::stream::Stream;

use proto::Row;

use std::mem;

use super::super::{
    MaybeRow,
    TextQueryResult,
    ResultSet,
};

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
