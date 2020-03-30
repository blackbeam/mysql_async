// Copyright (c) 2017 Anatoly Ikorsky
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

use crate::{connection_like::ConnectionLike, error::*};

pub struct WritePacket2<'a, T: ?Sized> {
    conn_like: &'a mut T,
    data: Option<Vec<u8>>,
}

impl<'a, T: ?Sized> WritePacket2<'a, T> {
    pub(crate) fn new(conn_like: &'a mut T, data: Vec<u8>) -> WritePacket2<'a, T> {
        Self {
            conn_like,
            data: Some(data),
        }
    }
}

impl<'a, T> Future for WritePacket2<'a, T>
where
    T: ConnectionLike,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.data.is_some() {
            let codec = Pin::new(
                self.conn_like
                    .stream_mut()
                    .codec
                    .as_mut()
                    .expect("must be here"),
            );
            ready!(codec.poll_ready(cx))?;
        }

        if let Some(data) = self.data.take() {
            let codec = Pin::new(
                self.conn_like
                    .stream_mut()
                    .codec
                    .as_mut()
                    .expect("must be here"),
            );
            // to get here, stream must be ready
            codec.start_send(data)?;
        }

        let codec = Pin::new(
            self.conn_like
                .stream_mut()
                .codec
                .as_mut()
                .expect("must be here"),
        );

        ready!(codec.poll_flush(cx)).map_err(Error::from)?;

        Poll::Ready(Ok(()))
    }
}
