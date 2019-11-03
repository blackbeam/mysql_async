// Copyright (c) 2019 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_util::future::{ready, FutureExt};
use futures_util::stream::StreamExt;
use tokio::timer::Interval;

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::Inner;
use crate::prelude::Queryable;
use crate::PoolOptions;

/// Idling connections TTL check interval.
///
/// The purpose of this interval is to remove idling connections that both:
/// * overflows min bound of the pool;
/// * idles longer then `inactive_connection_ttl`.
pub struct TtlCheckInterval {
    inner: Arc<Inner>,
    pool_options: PoolOptions,
}

impl TtlCheckInterval {
    /// Creates new `TtlCheckInterval`.
    pub fn new(pool_options: PoolOptions, inner: Arc<Inner>) -> Self {
        Self {
            inner,
            pool_options,
        }
    }

    /// Returns future that will sequentially perform the check.
    pub fn into_future(self) -> impl Future<Output = ()> + 'static + Send {
        let pool_options = self.pool_options;
        let inner = self.inner;
        Interval::new_interval(pool_options.ttl_check_interval()).for_each(move |_| {
            let num_idling = inner.idle.len();
            let mut num_to_drop = num_idling.saturating_sub(pool_options.constraints().min());
            let mut num_returned = 0;

            for _ in 0..num_idling {
                match inner.idle.pop() {
                    Ok(idling_conn) => {
                        if idling_conn.elapsed() > pool_options.inactive_connection_ttl() {
                            tokio::spawn(idling_conn.conn.disconnect().map(drop));
                            inner.exist.fetch_sub(1, Ordering::AcqRel);
                            num_to_drop -= 1;
                            if num_to_drop == 0 {
                                break;
                            }
                        } else {
                            inner.push_to_idle(idling_conn);
                            num_returned += 1;
                        }
                    }
                    Err(_) => break,
                }
            }

            if num_returned > 0 {
                inner.wake(num_returned);
            }

            ready(())
        })
    }
}
