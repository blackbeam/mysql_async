// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::ready;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    conn::{pool::Pool, Conn},
    error::*,
    BoxFuture,
};

pub(crate) enum GetConnInner {
    New,
    Done(Option<Conn>),
    // TODO: one day this should be an existential
    Connecting(BoxFuture<Conn>),
}

impl GetConnInner {
    /// Take the value of the inner connection, resetting it to `New`.
    pub fn take(&mut self) -> GetConnInner {
        std::mem::replace(self, GetConnInner::New)
    }
}

/// This future will take connection from a pool and resolve to [`Conn`].
pub struct GetConn {
    pub(crate) pool: Option<Pool>,
    pub(crate) inner: GetConnInner,
}

pub fn new(pool: &Pool) -> GetConn {
    GetConn {
        pool: Some(pool.clone()),
        inner: GetConnInner::New,
    }
}

// this manual implementation of Future may seem stupid, but we sort
// of need it to get the dropping behavior we want.
impl Future for GetConn {
    type Output = Result<Conn>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.inner {
                GetConnInner::New => match ready!(Pin::new(
                    self.pool
                        .as_mut()
                        .expect("GetConn::poll polled after returning Async::Ready")
                )
                .poll_new_conn(cx))?
                .inner
                .take()
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
                    return Poll::Ready(Ok(c));
                }
                GetConnInner::Done(None) => {
                    unreachable!("GetConn::poll polled after returning Async::Ready");
                }
                GetConnInner::Connecting(ref mut f) => {
                    let result = ready!(Pin::new(f).poll(cx));
                    let pool = self
                        .pool
                        .take()
                        .expect("GetConn::poll polled after returning Async::Ready");

                    return match result {
                        Ok(mut c) => {
                            c.inner.pool = Some(pool);
                            Poll::Ready(Ok(c))
                        }
                        Err(e) => {
                            pool.cancel_connection();
                            Poll::Ready(Err(e))
                        }
                    };
                }
            }
        }
    }
}

impl Drop for GetConn {
    fn drop(&mut self) {
        // We drop a connection before it can be resolved, a.k.a. cancelling it.
        // Make sure we maintain the necessary invariants towards the pool.
        if let Some(pool) = self.pool.take() {
            if let GetConnInner::Connecting(..) = self.inner.take() {
                pool.cancel_connection();
            }
        }
    }
}
