// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures::{try_ready, Async, Future, Poll};

use crate::{
    conn::{pool::Pool, Conn},
    error::*,
    MyFuture,
};

pub(crate) enum GetConnInner {
    New(Pool),
    Done(Option<Conn>),
    // TODO: one day this should be an existential
    // TODO: impl Drop?
    Connecting(Box<dyn MyFuture<Conn>>),
}

/// This future will take connection from a pool and resolve to `Conn`.
pub struct GetConn {
    pub(crate) inner: GetConnInner,
}

pub fn new(pool: &Pool) -> GetConn {
    GetConn {
        inner: GetConnInner::New(pool.clone()),
    }
}

impl Future for GetConn {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.inner {
                GetConnInner::New(ref mut pool) => match try_ready!(pool.poll_new_conn()) {
                    GetConn {
                        inner: GetConnInner::Done(Some(conn)),
                    } => {
                        self.inner = GetConnInner::Done(Some(conn));
                    }
                    GetConn {
                        inner: GetConnInner::Connecting(conn_fut),
                    } => {
                        self.inner = GetConnInner::Connecting(conn_fut);
                    }
                    GetConn {
                        inner: GetConnInner::Done(None),
                    } => unreachable!(
                        "Pool::poll_new_conn never gives out already-consumed GetConns"
                    ),
                    GetConn {
                        inner: GetConnInner::New(_),
                    } => unreachable!("Pool::poll_new_conn never gives out GetConnInner::New"),
                },
                GetConnInner::Done(ref mut c @ Some(_)) => {
                    return Ok(Async::Ready(c.take().unwrap()))
                }
                GetConnInner::Done(None) => {
                    unreachable!("GetConn::poll polled after returning Async::Ready");
                }
                GetConnInner::Connecting(ref mut f) => {
                    return f.poll();
                }
            }
        }
    }
}
