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

use futures_core::ready;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    conn::pool::{Inner, Pool, QUEUE_END_ID},
    error::Error,
    Conn,
};

use std::sync::{atomic, Arc};

/// Future that disconnects this pool from a server and resolves to `()`.
///
/// **Note:** This Future won't resolve until all active connections, taken from it,
/// are dropped or disonnected. Also all pending and new `GetConn`'s will resolve to error.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct DisconnectPool {
    pool_inner: Arc<Inner>,
    drop: Option<UnboundedSender<Option<Conn>>>,
}

impl DisconnectPool {
    pub(crate) fn new(pool: Pool) -> Self {
        Self {
            pool_inner: pool.inner,
            drop: Some(pool.drop),
        }
    }
}

impl Future for DisconnectPool {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.pool_inner.close.store(true, atomic::Ordering::Release);
        let mut exchange = self.pool_inner.exchange.lock().unwrap();
        exchange.spawn_futures_if_needed(&self.pool_inner);
        exchange.waiting.push(cx.waker().clone(), QUEUE_END_ID);
        drop(exchange);

        if self.pool_inner.closed.load(atomic::Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            match self.drop.take() {
                Some(drop) => match drop.send(None) {
                    Ok(_) => {
                        // Recycler is alive. Waiting for it to finish.
                        ready!(Box::pin(drop.closed()).as_mut().poll(cx));
                        Poll::Ready(Ok(()))
                    }
                    Err(_) => {
                        // Recycler seem dead. No one will wake us.
                        Poll::Ready(Ok(()))
                    }
                },
                None => Poll::Pending,
            }
        }
    }
}
