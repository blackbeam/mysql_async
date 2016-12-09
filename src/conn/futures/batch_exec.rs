// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::Prepare;
use conn::futures::query_result::BinQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use conn::futures::query_result::futures::DropResult;
use conn::stmt::futures::Execute;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use std::mem;
use value::Params;


steps! {
    BatchExec {
        Prepare(Prepare),
        Execute(Execute),
        DropResult(DropResult<BinQueryResult>),
    }
}

/// Future that prepares statement, performs batch execution and resolves to `Conn`.
///
/// All results will be dropped.
pub struct BatchExec {
    step: Step,
    params_vec: Vec<Params>,
    current: usize,
}

pub fn new<Q, P>(conn: Conn, query: Q, params_vec: Vec<P>) -> BatchExec
    where Q: AsRef<str>,
          P: Into<Params>,
{
    BatchExec {
        step: Step::Prepare(conn.prepare(query)),
        params_vec: params_vec.into_iter().map(Into::into).collect(),
        current: 0,
    }
}

impl Future for BatchExec {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::Prepare(stmt) |
            Out::DropResult(stmt) => {
                let current = self.current;
                self.current += 1;
                let params = match self.params_vec.get_mut(current) {
                    Some(params) => mem::replace(params, Params::Empty),
                    None => return Ok(Ready(stmt.unwrap())),
                };
                self.step = Step::Execute(stmt.execute(params));
                self.poll()
            },
            Out::Execute(query_result) => {
                self.step = Step::DropResult(query_result.drop_result());
                self.poll()
            },
        }
    }
}
