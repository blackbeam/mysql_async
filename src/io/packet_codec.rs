// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use mysql_common::{constants::MAX_PAYLOAD_LEN, packets::RawPacket};
use tokio_codec::{Decoder, Encoder};

use std::io;

#[derive(Debug)]
pub struct PacketCodec {
    chunk_len: isize,
    packet_data: BytesMut,
    seq_id: u8,
}

impl PacketCodec {
    pub fn new() -> PacketCodec {
        PacketCodec {
            chunk_len: -1,
            packet_data: BytesMut::with_capacity(256),
            seq_id: 0,
        }
    }
}

impl Decoder for PacketCodec {
    type Item = (RawPacket, u8);
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<(RawPacket, u8)>, io::Error> {
        if self.chunk_len >= 0 {
            if self.chunk_len as usize <= buf.len() {
                let chunk_len = self.chunk_len as usize;
                let chunk = buf.split_to(chunk_len);

                self.chunk_len = -1;
                self.packet_data.extend_from_slice(&chunk[..]);

                if chunk_len == MAX_PAYLOAD_LEN {
                    Ok(None)
                } else {
                    let packet_data: Vec<u8> = self.packet_data.as_ref().into();
                    self.packet_data.clear();
                    Ok(Some((RawPacket(packet_data), self.seq_id)))
                }
            } else {
                Ok(None)
            }
        } else if buf.len() < 4 {
            Ok(None)
        } else {
            let header = buf.split_to(4);
            self.chunk_len = LittleEndian::read_uint(&header[..], 3) as isize;
            self.seq_id = header[3];
            self.decode(buf)
        }
    }
}

impl Encoder for PacketCodec {
    type Item = (RawPacket, u8);
    type Error = io::Error;

    fn encode(
        &mut self,
        (RawPacket(packet), mut seq_id): Self::Item,
        buf: &mut BytesMut,
    ) -> Result<(), io::Error> {
        let empty_chunk_required = packet.len() % MAX_PAYLOAD_LEN == 0;

        buf.reserve(
            packet.len()
                + (packet.len() / MAX_PAYLOAD_LEN) * 4
                + ((!empty_chunk_required as usize) << 2)
                + ((empty_chunk_required as usize) << 2),
        );

        for chunk in packet.chunks(MAX_PAYLOAD_LEN) {
            buf.put_u32_le(chunk.len() as u32 | (u32::from(seq_id) << 24));
            buf.put(chunk);
            seq_id = seq_id.wrapping_add(1);
        }

        if empty_chunk_required {
            buf.put_u32_be(u32::from(seq_id));
            seq_id = seq_id.wrapping_add(1);
        }

        self.seq_id = seq_id;
        Ok(())
    }
}
