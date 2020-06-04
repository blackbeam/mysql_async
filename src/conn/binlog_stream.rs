// Copyright (c) 2020 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::ready;
use mysql_common::{
    binlog::{events::TableMapEvent, BinlogVersion::Version4, Event, EventStreamReader},
    packets::{ComBinlogDump, ComBinlogDumpGtid, OkPacketKind, SidBlock},
};

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{error::DriverError, io::ReadPacket, BinlogDumpFlags, Conn, Result};

/// Binlog event stream.
///
/// Stream initialization is lazy, i.e. binlog won't be requested until this stream is polled.
pub struct BinlogStream {
    read_packet: ReadPacket<'static, 'static>,
    esr: EventStreamReader,
}

impl BinlogStream {
    /// `conn` is a `Conn` with `request_binlog` executed on it.
    pub(super) fn new(conn: Conn) -> Self {
        BinlogStream {
            read_packet: ReadPacket::new(conn),
            esr: EventStreamReader::new(Version4),
        }
    }

    /// Returns a table map event for the given table id.
    pub fn get_tme(table_id: u64) -> Option<&TableMapEvent<'static>> {
        self.esr.get_tme(table_id)
    }
}

impl futures_core::stream::Stream for BinlogStream {
    type Item = Result<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.read_packet).poll(cx)) {
            Ok(packet) if packet.get(0) == Some(&0) => {
                let event_data = &packet[1..];
                match self.esr.read(event_data) {
                    Ok(event) => {
                        return Poll::Ready(Some(Ok(event)));
                    }
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                }
            }
            Ok(packet) if packet.get(0) == Some(&254) => {
                let eof_packet = mysql_common::packets::parse_ok_packet(
                    &packet,
                    self.read_packet.conn_ref().capabilities(),
                    OkPacketKind::NetworkStreamTerminator,
                );
                match eof_packet {
                    Ok(_) => return Poll::Ready(None),
                    Err(_) => {
                        return Poll::Ready(Some(Err(DriverError::UnexpectedPacket {
                            payload: packet,
                        }
                        .into())))
                    }
                }
            }
            Ok(packet) => {
                return Poll::Ready(Some(Err(
                    DriverError::UnexpectedPacket { payload: packet }.into()
                )))
            }
            Err(err) => return Poll::Ready(Some(Err(err.into()))),
        }
    }
}

/// Binlog request representation. Please consult MySql documentation.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct BinlogRequest {
    /// Server id of a slave.
    server_id: u32,
    /// If true, then `COM_BINLOG_DUMP_GTID` will be used.
    use_gtid: bool,
    /// If `use_gtid` is `false`, then all flags except `BINLOG_DUMP_NON_BLOCK` will be truncated.
    flags: BinlogDumpFlags,
    /// Filename of the binlog on the master.
    filename: Vec<u8>,
    /// Position in the binlog-file to start the stream with.
    ///
    /// If `use_gtid` is `false`, then the value will be truncated to u32.
    pos: u64,
    /// SID blocks. If `use_gtid` is `false`, then this value is ignored.
    sid_blocks: Vec<SidBlock>,
}

impl BinlogRequest {
    /// Creates new request with the given slave server id.
    pub fn new(server_id: u32) -> Self {
        Self {
            server_id,
            use_gtid: false,
            flags: BinlogDumpFlags::empty(),
            filename: Vec::new(),
            pos: 4,
            sid_blocks: vec![],
        }
    }

    /// Server id of a slave.
    pub fn server_id(&self) -> u32 {
        self.server_id
    }

    /// If true, then `COM_BINLOG_DUMP_GTID` will be used (defaults to `false`).
    pub fn use_gtid(&self) -> bool {
        self.use_gtid
    }

    /// If `use_gtid` is `false`, then all flags except `BINLOG_DUMP_NON_BLOCK` will be truncated
    /// (defaults to empty).
    pub fn flags(&self) -> BinlogDumpFlags {
        self.flags
    }

    /// Filename of the binlog on the master (defaults to an empty string).
    pub fn filename(&self) -> &[u8] {
        &self.filename
    }

    /// Position in the binlog-file to start the stream with (defaults to `4`).
    ///
    /// If `use_gtid` is `false`, then the value will be truncated to u32.
    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// If `use_gtid` is `false`, then this value will be ignored (defaults to an empty vector).
    pub fn sid_blocks(&self) -> &[SidBlock] {
        &self.sid_blocks
    }

    /// Returns modified `self` with the given value of the `server_id` field.
    pub fn with_server_id(mut self, server_id: u32) -> Self {
        self.server_id = server_id;
        self
    }

    /// Returns modified `self` with the given value of the `use_gtid` field.
    pub fn with_use_gtid(mut self, use_gtid: bool) -> Self {
        self.use_gtid = use_gtid;
        self
    }

    /// Returns modified `self` with the given value of the `flags` field.
    pub fn with_flags(mut self, flags: BinlogDumpFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Returns modified `self` with the given value of the `filename` field.
    pub fn with_filename<T: Into<Vec<u8>>>(mut self, filename: T) -> Self {
        self.filename = filename.into();
        self
    }

    /// Returns modified `self` with the given value of the `pos` field.
    pub fn with_pos<T: Into<u64>>(mut self, pos: T) -> Self {
        self.pos = pos.into();
        self
    }

    /// Returns modified `self` with the given value of the `sid_blocks` field.
    pub fn with_sid_blocks<T>(mut self, sid_blocks: T) -> Self
    where
        T: IntoIterator<Item = SidBlock>,
    {
        self.sid_blocks = sid_blocks.into_iter().collect();
        self
    }

    pub(super) fn serialize_cmd(self) -> Vec<u8> {
        macro_rules! to_buf {
            ($cmd:ident) => {{
                let mut buf = vec![0_u8; $cmd.len()];
                $cmd.write(&mut buf[..]).expect("unreachable");
                buf
            }};
        }

        if self.use_gtid() {
            let cmd = ComBinlogDumpGtid::new(self.server_id)
                .with_pos(self.pos)
                .with_flags(self.flags)
                .with_filename(self.filename)
                .with_sid_blocks(self.sid_blocks);
            to_buf!(cmd)
        } else {
            let cmd = ComBinlogDump::new(self.server_id)
                .with_pos(self.pos as u32)
                .with_filename(self.filename)
                .with_flags(self.flags & BinlogDumpFlags::BINLOG_DUMP_NON_BLOCK);
            to_buf!(cmd)
        }
    }
}
