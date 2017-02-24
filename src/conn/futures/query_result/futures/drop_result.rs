// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::futures::query_result::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;


/// Future that drops result and resolves to wrapped `Conn` or `Stmt`.
///
/// Note that it resolves to `Stmt` for result of `Conn::(prep|first)_exec` because it's result is
/// `BinQueryResult`. You can call `unwrap()` on `Stmt` to get wrapped `Conn`.
pub struct DropResult<T> {
    query_result: Option<T>,
}

pub fn new<T: Sized>(query_result: T) -> DropResult<T> {
    DropResult { query_result: Some(query_result) }
}

impl<T> Future for DropResult<T>
    where T: InnerQueryResult,
          T: UnconsumedQueryResult,
          T::Output: QueryResultOutput<Result = T>,
{
    type Item = <T::Output as QueryResultOutput>::Output;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.as_mut().unwrap().poll()) {
            Left(_) => self.poll(),
            Right(output) => {
                let prev_result = self.query_result.take().unwrap();
                match output.into_next_or_output(prev_result) {
                    (_, Left(next_result)) => {
                        self.query_result = Some(next_result);
                        self.poll()
                    },
                    (_, Right(stmt_or_conn)) => Ok(Ready(stmt_or_conn)),
                }
            },
        }
    }
}
