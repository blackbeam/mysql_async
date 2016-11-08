// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::Prepare;
use conn::stmt::futures::First;
use conn::stmt::Stmt;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use value::FromRow;
use value::Params;

enum Step<R> {
    Prepare(Prepare),
    First(First<R>),
}

enum Out<R> {
    Prepare(Stmt),
    First((Option<R>, Stmt)),
}

/// This future will execute statement, get first row of result and resolve to `Option<R>`.
///
/// It will call `from_row::<R>(row)` internally.
pub struct FirstExec<R> {
    step: Step<R>,
    params: Option<Params>,
}

impl<R: FromRow> FirstExec<R> {
    fn either_poll(&mut self) -> Result<Async<Out<R>>> {
        match self.step {
            Step::Prepare(ref mut fut) => Ok(Ready(Out::Prepare(try_ready!(fut.poll())))),
            Step::First(ref mut fut) => Ok(Ready(Out::First(try_ready!(fut.poll())))),
        }
    }
}

pub fn new<Q, P, R>(conn: Conn, query: Q, params: P) -> FirstExec<R>
    where Q: AsRef<str>,
          P: Into<Params>,
          R: FromRow,
{
    FirstExec {
        step: Step::Prepare(conn.prepare(query)),
        params: Some(params.into()),
    }
}

impl<R: FromRow> Future for FirstExec<R> {
    type Item = (Option<R>, Conn);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::Prepare(stmt) => {
                let params = self.params.take().unwrap();
                self.step = Step::First(stmt.first(params));
                self.poll()
            },
            Out::First((row, stmt)) => Ok(Ready((row, stmt.unwrap())))
        }
    }
}
