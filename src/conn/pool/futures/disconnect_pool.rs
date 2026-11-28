// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::sync::atomic;

use crate::{conn::pool::Pool, error::Error};

/// Disconnects this pool from the server and resolves to `()`.
///
/// **Note:** This future waits until all checked-out connections are dropped or disconnected,
/// and until any `get_conn()` already establishing a connection finishes setup or is dropped.
/// Parked and new `get_conn()` calls fail with [`crate::DriverError::PoolDisconnected`].
/// Callers that need bounded shutdown should cancel or time-limit outstanding `get_conn()` calls.
pub(crate) async fn disconnect_pool(pool: Pool) -> Result<(), Error> {
    let inner = pool.inner;
    let drop = pool.drop;

    // Mark the pool as closing and drain all parked waiters under one lock.
    let wakers = {
        let mut exchange = inner.exchange.lock().unwrap();
        inner.close.store(true, atomic::Ordering::Release);
        exchange.spawn_futures_if_needed(&inner);
        let mut wakers = Vec::new();
        exchange.waiting.drain_wakers(&mut wakers);
        wakers
    };

    for waker in wakers {
        waker.wake();
    }

    if !inner.closed.load(atomic::Ordering::Acquire) && drop.send(None).is_ok() {
        // Recycler is alive. Wait for it to finish.
        drop.closed().await;
    }

    Ok(())
}
