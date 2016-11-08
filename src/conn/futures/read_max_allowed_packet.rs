// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use Conn;
use conn::futures::first::First;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;


/// Future that resolves to `Conn` with value of MySql's max_allowed_packet stored in it.
pub struct ReadMaxAllowedPacket {
    future: First<(u64,)>,
}

pub fn new(future: First<(u64,)>) -> ReadMaxAllowedPacket {
    ReadMaxAllowedPacket {
        future: future,
    }
}

impl Future for ReadMaxAllowedPacket {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.future.poll()) {
            (maybe_row, mut conn) => {
                conn.max_allowed_packet = maybe_row.map(|x| x.0).unwrap_or(1024 * 1024 * 2);
                Ok(Ready(conn))
            },
        }
    }
}
