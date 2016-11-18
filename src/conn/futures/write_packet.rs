// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use io;
use time::SteadyTime;


/// Futures that sends packet to a server and resolves to `Conn`.
pub struct WritePacket {
    conn: Option<Conn>,
    future: io::futures::WritePacket,
}

pub fn new(conn: Conn, future: io::futures::WritePacket) -> WritePacket {
    WritePacket {
        conn: Some(conn),
        future: future,
    }
}

impl Future for WritePacket {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.future.poll()) {
            (stream, seq_id) => {
                let mut conn = self.conn.take().unwrap();
                conn.seq_id = seq_id;
                conn.stream = Some(stream);
                conn.last_io = SteadyTime::now();
                Ok(Ready(conn))
            }
        }
    }
}
