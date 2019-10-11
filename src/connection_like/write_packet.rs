// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::ready;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{
    connection_like::{streamless::Streamless, ConnectionLike},
    error::*,
    io,
};

#[pin_project]
pub struct WritePacket<T> {
    conn_like: Option<Streamless<T>>,
    #[pin]
    fut: io::futures::WritePacket,
}

impl<T: ConnectionLike> WritePacket<T> {
    pub fn new<U: Into<Vec<u8>>>(conn_like: T, data: U) -> WritePacket<T> {
        let (incomplete_conn, stream) = conn_like.take_stream();
        WritePacket {
            conn_like: Some(incomplete_conn),
            fut: stream.write_packet(data.into()),
        }
    }
}

impl<T: ConnectionLike> Future for WritePacket<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let stream = ready!(this.fut.poll(cx))?;
        let mut conn_like = this.conn_like.take().unwrap().return_stream(stream);
        conn_like.touch();
        Poll::Ready(Ok(conn_like))
    }
}
