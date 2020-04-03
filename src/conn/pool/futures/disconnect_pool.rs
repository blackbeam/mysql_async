// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    conn::pool::{Inner, Pool},
    error::Error,
};

use std::sync::{atomic, Arc};

/// Future that disconnects this pool from a server and resolves to `()`.
///
///
/// **Note:** This Future won't resolve until all active connections, taken from it,
/// are dropped or disonnected. Also all pending and new `GetConn`'s will resolve to error.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct DisconnectPool {
    pool_inner: Arc<Inner>,
}

impl DisconnectPool {
    pub(crate) fn new(pool: Pool) -> Self {
        Self {
            pool_inner: pool.inner,
        }
    }
}

impl Future for DisconnectPool {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut exchange = self.pool_inner.exchange.lock().unwrap();
        exchange.spawn_futures_if_needed(&self.pool_inner);
        exchange.waiting.push_back(cx.waker().clone());
        drop(exchange);

        if self.pool_inner.closed.load(atomic::Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
