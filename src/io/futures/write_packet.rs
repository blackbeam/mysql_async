// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::ready;
use futures_sink::Sink;
use mysql_common::packets::RawPacket;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{consts::MAX_PAYLOAD_LEN, error::*, io::Stream};

/// Future that writes packet to a `Stream` and resolves to a pair of `Stream` and MySql's sequence
/// id.
pub struct WritePacket {
    data: Option<RawPacket>,
    stream: Option<Stream>,
    seq_id: u8,
    resulting_seq_id: u8,
}

pub fn new(stream: Stream, data: Vec<u8>, seq_id: u8) -> WritePacket {
    // at least one packet will be written
    let resulting_seq_id = seq_id.wrapping_add(1);

    // each new packet after 2²⁴−1 will add to the resulting sequence id
    let mut resulting_seq_id =
        resulting_seq_id.wrapping_add(((data.len() / MAX_PAYLOAD_LEN) % 256) as u8);

    // empty tail packet will also add to the resulting sequence id
    if !data.is_empty() && data.len() % MAX_PAYLOAD_LEN == 0 {
        resulting_seq_id = resulting_seq_id.wrapping_add(1);
    }

    WritePacket {
        data: Some(RawPacket(data)),
        stream: Some(stream),
        seq_id,
        resulting_seq_id,
    }
}

impl Future for WritePacket {
    type Output = Result<(Stream, u8)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.data.is_some() {
            ready!(Pin::new(self.stream.as_mut().unwrap().codec.as_mut().unwrap()).poll_ready(cx))?;
        }

        if let Some(data) = self.data.take() {
            // to get here, stream must be ready
            let id = self.seq_id;
            Pin::new(self.stream.as_mut().unwrap().codec.as_mut().unwrap())
                .start_send((data, id))?;
        }

        ready!(Pin::new(self.stream.as_mut().unwrap().codec.as_mut().unwrap()).poll_flush(cx))
            .map_err(Error::from)?;
        Poll::Ready(Ok((self.stream.take().unwrap(), self.resulting_seq_id)))
    }
}
