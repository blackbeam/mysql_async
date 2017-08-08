// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use byteorder::LittleEndian as LE;
use byteorder::WriteBytesExt;
use consts;
use errors::*;
use io::Stream;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use tokio_io::io::write_all;
use tokio_io::io::WriteAll;


/// Future that writes packet to a `Stream` and resolves to a pair of `Stream` and MySql's sequence
/// id.
pub struct WritePacket {
    future: WriteAll<Stream, Vec<u8>>,
    seq_id: u8,
}

pub fn new(stream: Stream, data: Vec<u8>, mut seq_id: u8) -> WritePacket {
    let data = {
        if data.len() == 0 {
            let out = vec![0, 0, 0, seq_id];
            seq_id = seq_id.wrapping_add(1);
            out
        } else {
            let mut last_was_max = false;
            let capacity = data.len() + 4 * (data.len() / consts::MAX_PAYLOAD_LEN + 2);
            let mut out = Vec::with_capacity(capacity);
            for chunk in data.chunks(consts::MAX_PAYLOAD_LEN) {
                out.write_uint::<LE>(chunk.len() as u64, 3).unwrap();
                out.write_u8(seq_id).unwrap();
                out.extend_from_slice(chunk);
                seq_id = seq_id.wrapping_add(1);
                last_was_max = chunk.len() == consts::MAX_PAYLOAD_LEN;
            }
            if last_was_max {
                out.extend_from_slice(&[0, 0, 0, seq_id][..]);
                seq_id = seq_id.wrapping_add(1);
            }
            out
        }
    };

    WritePacket { future: write_all(stream, data), seq_id: seq_id }
}

impl Future for WritePacket {
    type Item = (Stream, u8);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.future.poll()) {
            (stream, _) => Ok(Ready((stream, self.seq_id))),
        }
    }
}
