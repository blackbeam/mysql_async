// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::{ready, stream::Stream};

use std::{
    future::Future,
    io::{Error, ErrorKind},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{connection_like::ConnectionLike, error::IoError};

/// Reads a packet.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadPacket<'a, T: ?Sized> {
    conn_like: &'a mut T,
}

impl<'a, T: ?Sized> ReadPacket<'a, T> {
    pub(crate) fn new(conn_like: &'a mut T) -> Self {
        Self { conn_like }
    }
}

impl<'a, T: ConnectionLike> Future for ReadPacket<'a, T> {
    type Output = std::result::Result<Vec<u8>, IoError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let packet_opt =
            ready!(Pin::new(self.conn_like.conn_mut().stream_mut()).poll_next(cx)).transpose()?;

        match packet_opt {
            Some(packet) => {
                self.conn_like.conn_mut().touch();
                return Poll::Ready(Ok(packet));
            }
            None => {
                return Poll::Ready(Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "connection closed",
                )
                .into()));
            }
        }
    }
}
