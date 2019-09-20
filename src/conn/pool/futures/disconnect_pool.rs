// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{
    conn::pool::{Inner, Pool},
    error::Error,
};

use std::sync::{atomic, Arc};

/// Future that disconnects this pool from server and resolves to `()`.
///
/// Active connections taken from this pool should be disconnected manually.
/// Also all pending and new `GetConn`'s will resolve to error.
pub struct DisconnectPool {
    pool_inner: Arc<Inner>,
}

pub fn new(pool: Pool) -> DisconnectPool {
    DisconnectPool {
        pool_inner: pool.inner,
    }
}

impl Future for DisconnectPool {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.pool_inner.wake.push(cx.waker().clone());

        if self.pool_inner.closed.load(atomic::Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
