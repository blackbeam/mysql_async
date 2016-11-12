// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::Query;
use conn::futures::query_result::futures::DropResult;
use conn::futures::query_result::TextQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use conn::transaction::Transaction;
use errors::*;
use lib_futures::AndThen;
use lib_futures::Future;
use lib_futures::Map;
use lib_futures::Poll;

type DropTextResultFn = fn(TextQueryResult) -> DropResult<TextQueryResult>;
type CleanConnFn = fn(Conn) -> Conn;
type DropTextResult = AndThen<Query, DropResult<TextQueryResult>, DropTextResultFn>;

pub struct Commit {
    fut: Map<DropTextResult, CleanConnFn>,
}

pub fn new(transaction: Transaction) -> Commit {
    let fut = transaction.conn.query("COMMIT")
        .and_then(UnconsumedQueryResult::drop_result as DropTextResultFn)
        .map(Transaction::clean_conn as CleanConnFn);

    Commit {
        fut: fut,
    }
}

impl Future for Commit {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.fut.poll()
    }
}
