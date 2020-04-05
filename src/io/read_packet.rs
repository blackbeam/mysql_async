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

use crate::{
    connection_like::{Connection, ConnectionLike},
    error::IoError,
};

/// Reads a packet.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadPacket<'a>(Connection<'a>);

impl<'a> ReadPacket<'a> {
    pub(crate) fn new<T: Into<Connection<'a>>>(conn: T) -> Self {
        Self(conn.into())
    }
}

impl<'a> Future for ReadPacket<'a> {
    type Output = std::result::Result<Vec<u8>, IoError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let packet_opt =
            ready!(Pin::new(self.0.conn_mut().stream_mut()).poll_next(cx)).transpose()?;

        match packet_opt {
            Some(packet) => {
                self.0.conn_mut().touch();
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
