// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::{
    connection_like::{streamless::Streamless, ConnectionLike},
    errors::*,
    io,
    lib_futures::{Async::Ready, Future, Poll},
};

pub struct WritePacket<T> {
    conn_like: Option<Streamless<T>>,
    fut: io::futures::WritePacket,
}

impl<T: ConnectionLike> WritePacket<T> {
    pub fn new<U: Into<Vec<u8>>>(conn_like: T, data: U) -> WritePacket<T> {
        let seq_id = conn_like.get_seq_id();
        let (incomplete_conn, stream) = conn_like.take_stream();
        WritePacket {
            conn_like: Some(incomplete_conn),
            fut: stream.write_packet(data.into(), seq_id),
        }
    }
}

impl<T: ConnectionLike> Future for WritePacket<T> {
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (stream, seq_id) = try_ready!(self.fut.poll());
        let mut conn_like = self.conn_like.take().unwrap().return_stream(stream);
        conn_like.set_seq_id(seq_id);
        conn_like.touch();
        Ok(Ready(conn_like))
    }
}
