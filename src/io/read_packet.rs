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

use crate::{buffer_pool::PooledBuf, connection_like::Connection, error::IoError};

/// Reads a packet.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadPacket<'a, 't> {
    pub(crate) conn: Connection<'a, 't>,
    metadata_limit: Option<usize>,
}

impl<'a, 't> ReadPacket<'a, 't> {
    pub(crate) fn new<T: Into<Connection<'a, 't>>>(conn: T) -> Self {
        Self {
            conn: conn.into(),
            metadata_limit: None,
        }
    }

    pub(crate) fn with_metadata_limit(mut self, limit: Option<usize>) -> Self {
        self.metadata_limit = limit;
        self
    }

    #[cfg(feature = "binlog")]
    pub(crate) fn conn_ref(&self) -> &crate::Conn {
        &self.conn
    }
}

impl Future for ReadPacket<'_, '_> {
    type Output = std::result::Result<PooledBuf, IoError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let limit = self.metadata_limit;
        let packet_opt = match self.conn.as_mut().stream_mut() {
            Ok(stream) => {
                let previous_limit = stream.set_metadata_limit(limit);
                let result = Pin::new(&mut *stream).poll_next(cx);
                // Restore even on Pending: cancelling this future must not leak its limit.
                stream.set_metadata_limit(previous_limit);
                ready!(result).transpose()?
            }
            // `ConnectionClosed` error.
            Err(_) => None,
        };

        match packet_opt {
            Some(packet) => {
                self.conn.as_mut().touch();
                Poll::Ready(Ok(packet))
            }
            None => Poll::Ready(Err(Error::new(
                ErrorKind::UnexpectedEof,
                "connection closed",
            )
            .into())),
        }
    }
}
