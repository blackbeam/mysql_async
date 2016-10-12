use byteorder::LittleEndian as LE;
use byteorder::{WriteBytesExt};

use consts;

use errors::*;

use lib_futures::{
    Future,
    Poll,
    Async,
};

use super::super::Stream;

use tokio::io::{
    write_all,
    WriteAll,
};

pub struct WritePacket {
    future: WriteAll<Stream, Vec<u8>>,
    seq_id: u8,
}

pub fn new(stream: Stream, data: Vec<u8>, mut seq_id: u8) -> WritePacket {
    let data = {
        let mut last_was_max = false;
        let capacity = data.len() + 4 * (data.len() / consts::MAX_PAYLOAD_LEN + 2);
        let mut out = Vec::with_capacity(capacity);
        for chunk in data.chunks(consts::MAX_PAYLOAD_LEN) {
            out.write_uint::<LE>(chunk.len() as u64, 3).unwrap();
            out.write_u8(seq_id).unwrap();
            out.extend_from_slice(chunk);
            seq_id += 1;
            last_was_max = chunk.len() == consts::MAX_PAYLOAD_LEN;
        }
        if last_was_max {
            out.extend_from_slice(&[0, 0, 0, seq_id][..]);
            seq_id += 1;
        }
        out
    };
    WritePacket {
        future: write_all(stream, data),
        seq_id: seq_id,
    }
}

impl Future for WritePacket {
    type Item = (Stream, u8);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.future.poll()) {
            (stream, _) => Ok(Async::Ready((stream, self.seq_id))),
        }
    }
}