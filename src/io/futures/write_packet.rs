// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::ready;
use futures_sink::Sink;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{error::*, io::Stream};

/// Future that writes packet to a [`Stream`] and resolves to a pair of [`Stream`] and MySql's sequence
/// id.
pub struct WritePacket {
    data: Option<Vec<u8>>,
    stream: Option<Stream>,
}

pub fn new(stream: Stream, data: Vec<u8>) -> WritePacket {
    WritePacket {
        data: Some(data),
        stream: Some(stream),
    }
}

impl Future for WritePacket {
    type Output = Result<Stream>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.data.is_some() {
            ready!(Pin::new(self.stream.as_mut().unwrap().codec.as_mut().unwrap()).poll_ready(cx))?;
        }

        if let Some(data) = self.data.take() {
            // to get here, stream must be ready
            Pin::new(self.stream.as_mut().unwrap().codec.as_mut().unwrap()).start_send(data)?;
        }

        ready!(Pin::new(self.stream.as_mut().unwrap().codec.as_mut().unwrap()).poll_flush(cx))
            .map_err(Error::from)?;

        Poll::Ready(Ok(self.stream.take().unwrap()))
    }
}
