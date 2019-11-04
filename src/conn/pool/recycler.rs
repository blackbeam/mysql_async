// Copyright (c) 2019 mysql_async contributors
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::stream::Stream;
use futures_util::stream::futures_unordered::FuturesUnordered;
use tokio::sync::mpsc;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{IdlingConn, Inner};
use crate::{BoxFuture, Conn, PoolOptions};
use tokio::sync::mpsc::UnboundedReceiver;

pub struct Recycler {
    inner: Arc<Inner>,
    discard: FuturesUnordered<BoxFuture<()>>,
    discarded: usize,
    cleaning: FuturesUnordered<BoxFuture<Conn>>,

    // Option<Conn> so that we have a way to send a "I didn't make a Conn after all" signal
    dropped: mpsc::UnboundedReceiver<Option<Conn>>,
    /// Pool options.
    pool_opts: PoolOptions,
    eof: bool,
}

impl Recycler {
    pub fn new(
        pool_opts: PoolOptions,
        inner: Arc<Inner>,
        dropped: UnboundedReceiver<Option<Conn>>,
    ) -> Self {
        Self {
            inner,
            discard: FuturesUnordered::new(),
            discarded: 0,
            cleaning: FuturesUnordered::new(),
            dropped,
            pool_opts,
            eof: false,
        }
    }
}

impl Future for Recycler {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut readied = 0;
        let mut close = self.inner.close.load(Ordering::Acquire);

        macro_rules! conn_decision {
            ($self:ident, $readied:ident, $conn:ident) => {
                if $conn.inner.stream.is_none() || $conn.inner.disconnected {
                    // drop unestablished connection
                    $self.discard.push(Box::pin(::futures_util::future::ok(())));
                } else if $conn.inner.in_transaction || $conn.inner.has_result.is_some() {
                    $self.cleaning.push(Box::pin($conn.cleanup()));
                } else if $conn.expired()
                    || close
                    || $self.inner.idle.len() >= $self.pool_opts.active_bound()
                {
                    $self.discard.push(Box::pin($conn.close()));
                } else {
                    $self.inner.push_to_idle($conn);
                    $readied += 1;
                }
            };
        }

        while !self.eof {
            // see if there are more connections for us to recycle
            match Pin::new(&mut self.dropped).poll_next(cx) {
                Poll::Ready(Some(Some(conn))) => {
                    conn_decision!(self, readied, conn);
                }
                Poll::Ready(Some(None)) => {
                    // someone signaled us that it's exit time
                    close = self.inner.close.load(Ordering::Acquire);
                    assert!(close);
                    continue;
                }
                Poll::Ready(None) => {
                    // no more connections are coming -- time to exit!
                    self.inner.close.store(true, Ordering::Release);
                    self.eof = true;
                    close = true;
                }
                Poll::Pending => {
                    // nope -- but let's still make progress on the ones we have
                    break;
                }
            }
        }

        // if we've been asked to close, reclaim any idle connections
        if close {
            while let Ok(IdlingConn { conn, .. }) = self.inner.idle.pop() {
                conn_decision!(self, readied, conn);
            }
        }

        // are any dirty connections ready for us to reclaim?
        loop {
            match Pin::new(&mut self.cleaning).poll_next(cx) {
                Poll::Pending | Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(conn))) => conn_decision!(self, readied, conn),
                Poll::Ready(Some(Err(e))) => {
                    // an error occurred while cleaning a connection.
                    // what do we do? replace it with a new connection?
                    self.discarded += 1;
                    // NOTE: we're discarding the error here
                    let _ = e;
                }
            }
        }

        // are there any torn-down connections for us to deal with?
        loop {
            match Pin::new(&mut self.discard).poll_next(cx) {
                Poll::Pending | Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(()))) => {
                    // yes! count it.
                    self.discarded += 1
                }
                Poll::Ready(Some(Err(e))) => {
                    // an error occurred while closing a connection.
                    // what do we do? we still replace it with a new connection..
                    self.discarded += 1;
                    // NOTE: we're discarding the error here
                    let _ = e;
                }
            }
        }

        if self.discarded != 0 {
            // we need to open up slots for new connctions to be established!
            self.inner.exist.fetch_sub(self.discarded, Ordering::AcqRel);
            readied += self.discarded;
            self.discarded = 0;
        }

        // NOTE: we are asserting here that no more connections will ever be returned to
        // us. see the explanation in Pool::poll_new_conn for why this is okay, even during
        // races on .exist
        let effectively_eof = close && self.inner.exist.load(Ordering::Acquire) == 0;

        if (self.eof || effectively_eof) && self.cleaning.is_empty() && self.discard.is_empty() {
            // we know that all Pool handles have been dropped (self.dropped.poll returned None).

            // if this assertion fails, where are the remaining connections?
            assert_eq!(self.inner.idle.len(), 0);
            assert_eq!(self.inner.exist.load(Ordering::Acquire), 0);

            // NOTE: it is _necessary_ that we set this _before_ we call .wake
            // otherwise, the following may happen to the DisconnectPool future:
            //
            //  - We wake all in .wake
            //  - DisconnectPool::poll adds to .wake
            //  - DisconnectPool::poll reads .closed == false
            //  - We set .closed = true
            //
            // At this point, DisconnectPool::poll will never be notified again.
            self.inner.closed.store(true, Ordering::Release);
        }

        self.inner.wake(readied);

        if self.inner.closed.load(Ordering::Acquire) {
            // `DisconnectPool` might still wait to be woken up.
            self.inner.wake(usize::max_value());

            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
