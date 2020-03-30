// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::{ready, stream::Stream};
use mysql_common::packets::{parse_err_packet, parse_ok_packet, OkPacketKind};

use std::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{connection_like::ConnectionLike, consts::StatusFlags, error::*};

/// Reads some number of packets.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadPackets<'a, T: ?Sized> {
    conn_like: &'a mut T,
    n: usize,
    packets: Vec<Vec<u8>>,
}

impl<'a, T: ?Sized> ReadPackets<'a, T> {
    pub fn new(conn_like: &'a mut T, n: usize) -> Self {
        Self {
            conn_like,
            n,
            packets: Vec::with_capacity(n),
        }
    }
}

impl<'a, T: ConnectionLike> Future for ReadPackets<'a, T> {
    type Output = Result<Vec<Vec<u8>>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if self.n > 0 {
                let packet_opt =
                    ready!(Pin::new(self.conn_like.stream_mut()).poll_next(cx)).transpose()?;
                match packet_opt {
                    Some(packet) => {
                        let kind = if self.conn_like.get_pending_result().is_some() {
                            OkPacketKind::ResultSetTerminator
                        } else {
                            OkPacketKind::Other
                        };

                        if let Ok(ok_packet) =
                            parse_ok_packet(&*packet, self.conn_like.get_capabilities(), kind)
                        {
                            self.conn_like.set_status(ok_packet.status_flags());
                            self.conn_like
                                .set_last_ok_packet(Some(ok_packet.into_owned()));
                        } else if let Ok(err_packet) =
                            parse_err_packet(&*packet, self.conn_like.get_capabilities())
                        {
                            self.conn_like.set_status(StatusFlags::empty());
                            self.conn_like.set_last_ok_packet(None);
                            return Err(err_packet.into()).into();
                        }

                        self.conn_like.touch();
                        self.packets.push(packet);
                        self.n -= 1;
                        continue;
                    }
                    None => {
                        return Poll::Ready(Err(DriverError::ConnectionClosed.into()));
                    }
                }
            } else {
                return Poll::Ready(Ok(mem::replace(&mut self.packets, Vec::new())));
            }
        }
    }
}

pub struct ReadPacket<'a, T: ?Sized> {
    inner: ReadPackets<'a, T>,
}

impl<'a, T: ?Sized> ReadPacket<'a, T> {
    pub fn new(conn_like: &'a mut T) -> Self {
        Self {
            inner: ReadPackets::new(conn_like, 1),
        }
    }
}

impl<'a, T: ConnectionLike> Future for ReadPacket<'a, T> {
    type Output = Result<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut packets = ready!(Pin::new(&mut self.inner).poll(cx))?;
        Poll::Ready(Ok(packets.pop().unwrap()))
    }
}
