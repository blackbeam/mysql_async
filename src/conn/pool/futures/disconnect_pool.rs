// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures::{task, Async, Future, Poll};

use crate::{
    conn::pool::{Inner, Pool},
    error::Error,
};

use std::sync::{atomic, Arc};

/// Future that disconnects this pool from server and resolves to `()`.
///
/// Active connections taken from this pool should be disconnected manually.
/// Also all pending and new `GetConn`'s will resolve to error.
pub struct DisconnectPool<T: crate::MyExecutor> {
    pool_inner: Arc<Inner<T>>,
}

pub fn new<T: crate::MyExecutor>(pool: Pool<T>) -> DisconnectPool<T> {
    DisconnectPool {
        pool_inner: pool.inner,
    }
}

impl<T: crate::MyExecutor> Future for DisconnectPool<T> {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.pool_inner.wake.push(task::current());

        if self.pool_inner.closed.load(atomic::Ordering::Acquire) {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}
