// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures::{
    stream::{Stream, StreamFuture},
    try_ready,
    Async::Ready,
    Future, Poll,
};
use mysql_common::packets::{parse_err_packet, parse_ok_packet, RawPacket};

use crate::{
    connection_like::{streamless::Streamless, ConnectionLike},
    error::*,
    io,
};

pub struct ReadPacket<T> {
    conn_like: Option<Streamless<T>>,
    fut: StreamFuture<io::Stream>,
}

impl<T: ConnectionLike> ReadPacket<T> {
    pub fn new(conn_like: T) -> Self {
        let (incomplete_conn, stream) = conn_like.take_stream();
        ReadPacket {
            conn_like: Some(incomplete_conn),
            fut: stream.into_future(),
        }
    }
}

impl<T: ConnectionLike> Future for ReadPacket<T> {
    type Item = (T, RawPacket);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (packet_opt, stream) = try_ready!(self.fut.poll());
        let mut conn_like = self.conn_like.take().unwrap().return_stream(stream);
        match packet_opt {
            Some((packet, seq_id)) => {
                if let Ok(ok_packet) = parse_ok_packet(&*packet.0, conn_like.get_capabilities()) {
                    conn_like.set_affected_rows(ok_packet.affected_rows());
                    conn_like.set_last_insert_id(ok_packet.last_insert_id().unwrap_or(0));
                    conn_like.set_status(ok_packet.status_flags());
                    conn_like.set_warnings(ok_packet.warnings());
                } else if let Ok(err_packet) =
                    parse_err_packet(&*packet.0, conn_like.get_capabilities())
                {
                    return Err(err_packet.into());
                }

                conn_like.touch();
                conn_like.set_seq_id(seq_id.wrapping_add(1));
                Ok(Ready((conn_like, packet)))
            }
            None => return Err(DriverError::ConnectionClosed.into()),
        }
    }
}
