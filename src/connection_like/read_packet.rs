// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::ready;
use futures_util::stream::{StreamExt, StreamFuture};
use mysql_common::packets::{parse_err_packet, parse_ok_packet};
use pin_project::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    connection_like::{streamless::Streamless, ConnectionLike},
    consts::StatusFlags,
    error::*,
    io,
};

#[pin_project]
pub struct ReadPacket<T> {
    conn_like: Option<Streamless<T>>,
    #[pin]
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
    type Output = Result<(T, Vec<u8>)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let (packet_opt, stream) = ready!(this.fut.poll(cx));
        let packet_opt = packet_opt.transpose()?;
        let mut conn_like = this.conn_like.take().unwrap().return_stream(stream);
        match packet_opt {
            Some(packet) => {
                if let Ok(ok_packet) = parse_ok_packet(&*packet, conn_like.get_capabilities()) {
                    conn_like.set_status(ok_packet.status_flags());
                    conn_like.set_last_ok_packet(Some(ok_packet.into_owned()));
                } else if let Ok(err_packet) =
                    parse_err_packet(&*packet, conn_like.get_capabilities())
                {
                    conn_like.set_status(StatusFlags::empty());
                    conn_like.set_last_ok_packet(None);
                    return Err(err_packet.into()).into();
                }

                conn_like.touch();
                Poll::Ready(Ok((conn_like, packet)))
            }
            None => Poll::Ready(Err(DriverError::ConnectionClosed.into())),
        }
    }
}
