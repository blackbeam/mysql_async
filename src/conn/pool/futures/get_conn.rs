// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::ready;

use crate::{
    conn::{pool::Pool, Conn},
    error::*,
};

/// States of the GetConn future.
pub(crate) enum GetConnInner {
    New,
    Done,
    // TODO: one day this should be an existential
    Connecting(crate::BoxFuture<'static, Conn>),
    /// This future will check, that idling connection is alive.
    Checking(crate::BoxFuture<'static, Conn>),
}

impl fmt::Debug for GetConnInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GetConnInner::New => f.debug_tuple("GetConnInner::New").finish(),
            GetConnInner::Done => f.debug_tuple("GetConnInner::Done").finish(),
            GetConnInner::Connecting(_) => f
                .debug_tuple("GetConnInner::Connecting")
                .field(&"<future>")
                .finish(),
            GetConnInner::Checking(_) => f
                .debug_tuple("GetConnInner::Checking")
                .field(&"<future>")
                .finish(),
        }
    }
}

impl GetConnInner {
    /// Take the value of the inner connection, resetting it to `New`.
    pub fn take(&mut self) -> GetConnInner {
        std::mem::replace(self, GetConnInner::New)
    }
}

/// This future will take connection from a pool and resolve to [`Conn`].
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct GetConn {
    pub(crate) pool: Option<Pool>,
    pub(crate) inner: GetConnInner,
}

impl GetConn {
    pub(crate) fn new(pool: &Pool) -> GetConn {
        GetConn {
            pool: Some(pool.clone()),
            inner: GetConnInner::New,
        }
    }

    fn pool_mut(&mut self) -> &mut Pool {
        self.pool
            .as_mut()
            .expect("GetConn::poll polled after returning Async::Ready")
    }

    fn pool_take(&mut self) -> Pool {
        self.pool
            .take()
            .expect("GetConn::poll polled after returning Async::Ready")
    }
}

// this manual implementation of Future may seem stupid, but we sort
// of need it to get the dropping behavior we want.
impl Future for GetConn {
    type Output = Result<Conn>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.inner {
                GetConnInner::New => match ready!(Pin::new(self.pool_mut()).poll_new_conn(cx))?
                    .inner
                    .take()
                {
                    GetConnInner::Connecting(conn_fut) => {
                        self.inner = GetConnInner::Connecting(conn_fut);
                    }
                    GetConnInner::Checking(conn_fut) => {
                        self.inner = GetConnInner::Checking(conn_fut);
                    }
                    GetConnInner::Done => unreachable!(
                        "Pool::poll_new_conn never gives out already-consumed GetConns"
                    ),
                    GetConnInner::New => {
                        unreachable!("Pool::poll_new_conn never gives out GetConnInner::New")
                    }
                },
                GetConnInner::Done => {
                    unreachable!("GetConn::poll polled after returning Async::Ready");
                }
                GetConnInner::Connecting(ref mut f) => {
                    let result = ready!(Pin::new(f).poll(cx));
                    let pool = self.pool_take();

                    self.inner = GetConnInner::Done;

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
                GetConnInner::Checking(ref mut f) => {
                    let result = ready!(Pin::new(f).poll(cx));
                    match result {
                        Ok(mut checked_conn) => {
                            self.inner = GetConnInner::Done;

                            let pool = self.pool_take();
                            checked_conn.inner.pool = Some(pool);
                            return Poll::Ready(Ok(checked_conn));
                        }
                        Err(_) => {
                            // Idling connection is broken. We'll drop it and try again.
                            self.inner = GetConnInner::New;

                            let pool = self.pool_mut();
                            pool.cancel_connection();
                            continue;
                        }
                    }
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
