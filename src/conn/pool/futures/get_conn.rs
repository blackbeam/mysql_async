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
    New,
    Done(Option<Conn>),
    // TODO: one day this should be an existential
    Connecting(Box<dyn MyFuture<Conn>>),
}

impl GetConnInner {
    /// Take the value of the inner connection, resetting it to `New`.
    pub fn take(&mut self) -> GetConnInner {
        std::mem::replace(self, GetConnInner::New)
    }
}

/// This future will take connection from a pool and resolve to `Conn`.
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

impl Future for GetConn {
    type Item = Conn;
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
                    return Ok(Async::Ready(c));
                }
                GetConnInner::Done(None) => {
                    unreachable!("GetConn::poll polled after returning Async::Ready");
                }
                GetConnInner::Connecting(ref mut f) => {
                    let result = match f.poll() {
                        Ok(Async::NotReady) => return Ok(Async::NotReady),
                        Ok(Async::Ready(c)) => Ok(c),
                        Err(e) => Err(e),
                    };

                    let pool = self
                        .pool
                        .take()
                        .expect("GetConn::poll polled after returning Async::Ready");

                    return match result {
                        Ok(mut c) => {
                            c.inner.pool = Some(pool);
                            Ok(Async::Ready(c))
                        }
                        Err(e) => {
                            pool.cancel_connection();
                            Err(e)
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
