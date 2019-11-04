// Copyright (c) 2019 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_util::future::FutureExt;
use futures_util::stream::{StreamExt, StreamFuture};
use pin_project::pin_project;
use tokio::timer::Interval;

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::Inner;
use crate::prelude::Queryable;
use crate::PoolOptions;
use futures_core::task::Context;
use futures_core::Poll;
use std::pin::Pin;

/// Idling connections TTL check interval.
///
/// The purpose of this interval is to remove idling connections that both:
/// * overflows min bound of the pool;
/// * idles longer then `inactive_connection_ttl`.
#[pin_project]
pub struct TtlCheckInterval {
    inner: Arc<Inner>,
    #[pin]
    interval: StreamFuture<Interval>,
    pool_options: PoolOptions,
}

impl TtlCheckInterval {
    /// Creates new `TtlCheckInterval`.
    pub fn new(pool_options: PoolOptions, inner: Arc<Inner>) -> Self {
        let interval = Interval::new_interval(pool_options.ttl_check_interval()).into_future();
        Self {
            inner,
            interval,
            pool_options,
        }
    }

    /// Perform the check.
    pub fn check_ttl(&self) {
        let num_idling = self.inner.idle.len();
        let mut num_to_drop = num_idling.saturating_sub(self.pool_options.constraints().min());
        let mut num_returned = 0;

        for _ in 0..num_idling {
            match self.inner.idle.pop() {
                Ok(idling_conn) => {
                    if idling_conn.elapsed() > self.pool_options.inactive_connection_ttl() {
                        tokio::spawn(idling_conn.conn.disconnect().map(drop));
                        self.inner.exist.fetch_sub(1, Ordering::AcqRel);
                        num_to_drop -= 1;
                        if num_to_drop == 0 {
                            break;
                        }
                    } else {
                        self.inner.push_to_idle(idling_conn);
                        num_returned += 1;
                    }
                }
                Err(_) => break,
            }
        }

        if num_returned > 0 {
            self.inner.wake(num_returned);
        }
    }
}

impl Future for TtlCheckInterval {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let (_, interval) = futures_core::ready!(self.as_mut().project().interval.poll(cx));
            let close = self.inner.close.load(Ordering::Acquire);

            if !close {
                self.check_ttl();
                self.interval = interval.into_future();
            } else {
                return Poll::Ready(());
            }
        }
    }
}
