// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::transaction::futures::StartTransaction as ConnStartTransaction;
use errors::*;
use IsolationLevel;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use super::GetConn;
use Transaction;

steps! {
    StartTransaction {
        GetConn(GetConn),
        StartTransaction(ConnStartTransaction),
    }
}

/// This future will resolve to `Transaction`
pub struct StartTransaction {
    step: Step,
    consistent_snapshot: bool,
    isolation_level: Option<IsolationLevel>,
    readonly: Option<bool>,
}

pub fn new(fut: GetConn,
           consistent_snapshot: bool,
           isolation_level: Option<IsolationLevel>,
           readonly: Option<bool>)
           -> StartTransaction {
    StartTransaction {
        step: Step::GetConn(fut),
        consistent_snapshot: consistent_snapshot,
        isolation_level: isolation_level,
        readonly: readonly,
    }
}

impl Future for StartTransaction {
    type Item = Transaction;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::GetConn(conn) => {
                self.step =
                    Step::StartTransaction(conn.start_transaction(self.consistent_snapshot,
                                                                  self.isolation_level,
                                                                  self.readonly));
                self.poll()
            },
            Out::StartTransaction(transaction) => Ok(Ready(transaction)),
        }
    }
}
