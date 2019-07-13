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

pub(crate) enum GetConnInner<T: crate::MyExecutor> {
    New,
    Done(Option<Conn<T>>),
    // TODO: one day this should be an existential
    // TODO: impl Drop?
    Connecting(Box<dyn MyFuture<Conn<T>>>),
}

/// This future will take connection from a pool and resolve to `Conn`.
pub struct GetConn<T: crate::MyExecutor> {
    pub(crate) pool: Option<Pool<T>>,
    pub(crate) inner: GetConnInner<T>,
}

pub fn new<T: crate::MyExecutor>(pool: &Pool<T>) -> GetConn<T> {
    GetConn {
        pool: Some(pool.clone()),
        inner: GetConnInner::New,
    }
}

impl<T: crate::MyExecutor> Future for GetConn<T> {
    type Item = Conn<T>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            match self.inner {
                GetConnInner::New => match try_ready!(self
                    .pool
                    .as_mut()
                    .expect("GetConn::poll polled after returning Async::Ready")
                    .poll_new_conn())
                .inner
                {
                    GetConnInner::Done(Some(conn)) => {
                        self.inner = GetConnInner::Done(Some(conn));
                    }
                    GetConnInner::Connecting(conn_fut) => {
                        self.inner = GetConnInner::Connecting(conn_fut);
                    }
                    GetConnInner::Done(None) => unreachable!(
                        "Pool::poll_new_conn never gives out already-consumed GetConns"
                    ),
                    GetConnInner::New => {
                        unreachable!("Pool::poll_new_conn never gives out GetConnInner::New")
                    }
                },
                GetConnInner::Done(ref mut c @ Some(_)) => {
                    let mut c = c.take().unwrap();
                    c.inner.pool = Some(
                        self.pool
                            .take()
                            .expect("GetConn::poll polled after returning Async::Ready"),
                    );
                    return Ok(Async::Ready(c));
                }
                GetConnInner::Done(None) => {
                    unreachable!("GetConn::poll polled after returning Async::Ready");
                }
                GetConnInner::Connecting(ref mut f) => {
                    let mut c = try_ready!(f.poll());
                    c.inner.pool = Some(
                        self.pool
                            .take()
                            .expect("GetConn::poll polled after returning Async::Ready"),
                    );
                    return Ok(Async::Ready(c));
                }
            }
        }
    }
}
