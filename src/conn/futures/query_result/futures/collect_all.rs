// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::futures::query_result::*;
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

impl<T> Future for CollectAll<T>
    where T: InnerQueryResult,
          T: UnconsumedQueryResult,
          T::Output: QueryResultOutput<Result = T>,
{
    type Item = (Vec<ResultSet<Row, T>>, <T::Output as QueryResultOutput>::Output);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.as_mut().unwrap().poll()) {
            Left(row) => {
                self.row_vec.push(row);
                self.poll()
            },
            Right(output) => {
                let set = mem::replace(&mut self.row_vec, Vec::new());
                let prev_result = self.query_result.take().unwrap();
                match output.into_next_or_output(prev_result) {
                    (prev_result, Left(next_result)) => {
                        self.query_result = Some(next_result);
                        self.vec.push(ResultSet(set, prev_result));
                        self.poll()
                    },
                    (prev_result, Right(out)) => {
                        self.vec.push(ResultSet(set, prev_result));
                        let vec = mem::replace(&mut self.vec, Vec::new());
                        Ok(Ready((vec, out)))
                    },
                }
            },
        }
    }
}
