// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures::{
    Async::{NotReady, Ready},
    Future, Poll,
};

use crate::{conn::pool::Pool, error::*};

/// Future that disconnects this pool from server and resolves to `()`.
///
/// Active connections taken from this pool should be disconnected manually.
/// Also all pending and new `GetConn`'s will resolve to error.
pub struct DisconnectPool {
    pool: Pool,
}

pub fn new(pool: Pool) -> DisconnectPool {
    DisconnectPool { pool: pool }
}

impl Future for DisconnectPool {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.pool.handle_futures()?;

        let (new_len, queue_len) = self.pool.with_inner(|inner| {
            (
                inner.new.len(),
                inner.queue.len(),
            )
        });

        if (new_len, queue_len) == (0, 0) {
            Ok(Ready(()))
        } else {
            Ok(NotReady)
        }
    }
}
