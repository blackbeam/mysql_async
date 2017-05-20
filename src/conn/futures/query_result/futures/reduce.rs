// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::futures::query_result::InnerQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use either::Left;
use either::Right;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::Row;


/// Future that calls `F: FnMut(A, Row) -> A` on each Row of a `QueryResult`.
///
/// It resolves to a pair of `A` and output of corresponding `ResultKind`.
pub struct Reduce<A, F, T> {
    query_result: T,
    accum: Option<A>,
    fun: F,
}

pub fn new_new<A, F, T>(query_result: T, init: A, fun: F) -> Reduce<A, F, T>
    where F: FnMut(A, Row) -> A,
{
    Reduce {
        query_result: query_result,
        accum: Some(init),
        fun: fun,
    }
}

impl<A, F, T> Future for Reduce<A, F, T>
    where F: FnMut(A, Row) -> A,
          T: InnerQueryResult,
          T: UnconsumedQueryResult,
{
    type Item = (A, T::Output);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match try_ready!(self.query_result.poll()) {
                Left(row) => {
                    let old_acc_val = self.accum.take().unwrap();
                    let new_acc_val = (self.fun)(old_acc_val, row);
                    self.accum = Some(new_acc_val);
                },
                Right(output) => {
                    let acc_val = self.accum.take().unwrap();
                    return Ok(Ready((acc_val, output)));
                },
            }
        }
    }
}
