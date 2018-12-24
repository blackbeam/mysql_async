// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use super::QueryResult;
use crate::{
    connection_like::ConnectionLike,
    error::*,
    lib_futures::{Async::Ready, Future, Poll},
    queryable::Protocol,
    BoxFuture, Row,
};

pub struct ForEach<T, P, F> {
    fut: BoxFuture<(QueryResult<T, P>, Option<Row>)>,
    fun: F,
}

impl<T, P, F> ForEach<T, P, F>
where
    F: FnMut(Row),
    P: Send + Protocol + 'static,
    T: ConnectionLike + Sized + 'static,
{
    pub fn new(query_result: QueryResult<T, P>, fun: F) -> ForEach<T, P, F> {
        ForEach {
            fut: Box::new(query_result.get_row()),
            fun,
        }
    }
}

impl<T, P, F> Future for ForEach<T, P, F>
where
    F: FnMut(Row),
    P: Protocol,
    P: Send + 'static,
    T: ConnectionLike + Sized + 'static,
{
    type Item = (QueryResult<T, P>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let (query_result, row_opt) = try_ready!(self.fut.poll());
            match row_opt {
                Some(row) => {
                    (self.fun)(row);
                }
                None => {
                    return Ok(Ready(query_result));
                }
            }
            self.fut = Box::new(query_result.get_row());
        }
    }
}
