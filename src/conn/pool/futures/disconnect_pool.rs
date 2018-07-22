// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::pool::Pool;
use errors::*;
use lib_futures::Async::NotReady;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;

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

        let new_len = self.pool.inner_ref().new.len();
        let dropping_len = self.pool.inner_ref().dropping.len();
        let disconnecting_len = self.pool.inner_ref().disconnecting.len();
        if (new_len, dropping_len, disconnecting_len) == (0, 0, 0) {
            Ok(Ready(()))
        } else {
            Ok(NotReady)
        }
    }
}
