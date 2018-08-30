// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use consts::MAX_PAYLOAD_LEN;
use errors::*;
use io::Stream;
use lib_futures::Async;
use lib_futures::AsyncSink;
use lib_futures::Future;
use lib_futures::Poll;
use lib_futures::Sink;
use myc::packets::RawPacket;

/// Future that writes packet to a `Stream` and resolves to a pair of `Stream` and MySql's sequence
/// id.
pub struct WritePacket {
    data: Option<RawPacket>,
    stream: Option<Stream>,
    seq_id: u8,
    out_seq_id: u8,
}

pub fn new(stream: Stream, data: Vec<u8>, seq_id: u8) -> WritePacket {
    let mut out_seq_id = ((data.len() / MAX_PAYLOAD_LEN) % 256) as u8;

    if data.len() % MAX_PAYLOAD_LEN == 0 {
        out_seq_id = out_seq_id.wrapping_add(1);
    }

    WritePacket {
        data: Some(RawPacket(data)),
        stream: Some(stream),
        seq_id,
        out_seq_id,
    }
}

impl Future for WritePacket {
    type Item = (Stream, u8);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.data.take() {
            Some(data) => match self
                .stream
                .as_mut()
                .unwrap()
                .codec
                .start_send((data, self.seq_id))?
            {
                AsyncSink::Ready => (),
                AsyncSink::NotReady(data) => {
                    self.data = Some(data.0);
                    return Ok(Async::NotReady);
                }
            },
            None => (),
        }

        try_ready!(
            self.stream
                .as_mut()
                .unwrap()
                .codec
                .poll_complete()
                .map_err(Error::from)
        );
        Ok(Async::Ready((self.stream.take().unwrap(), self.out_seq_id)))
    }
}
