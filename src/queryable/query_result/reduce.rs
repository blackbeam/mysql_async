// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use super::QueryResult;
use connection_like::ConnectionLike;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::{Future, Poll};
use queryable::Protocol;
use BoxFuture;
use Row;

pub struct Reduce<T, P, F, U> {
    fut: BoxFuture<(QueryResult<T, P>, Option<Row>)>,
    acc: Option<U>,
    fun: F,
}

impl<T, P, F, U> Reduce<T, P, F, U>
where
    F: FnMut(U, Row) -> U,
    P: Protocol + 'static,
    T: ConnectionLike + Sized + 'static,
{
    pub fn new(query_result: QueryResult<T, P>, init: U, fun: F) -> Reduce<T, P, F, U> {
        Reduce {
            fut: Box::new(query_result.get_row()),
            acc: Some(init),
            fun,
        }
    }
}

impl<T, P, F, U> Future for Reduce<T, P, F, U>
where
    F: FnMut(U, Row) -> U,
    P: Protocol + 'static,
    T: ConnectionLike + Sized + 'static,
{
    type Item = (QueryResult<T, P>, U);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let (query_result, row_opt) = try_ready!(self.fut.poll());
            match row_opt {
                Some(row) => {
                    let prev_acc_val = self.acc.take().unwrap();
                    let new_acc_value = (self.fun)(prev_acc_val, row);
                    self.acc = Some(new_acc_value);
                }
                None => {
                    return Ok(Ready((query_result, self.acc.take().unwrap())));
                }
            }
            self.fut = Box::new(query_result.get_row());
        }
    }
}
