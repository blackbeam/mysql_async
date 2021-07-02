// Copyright (c) 2020 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::ready;
use mysql_common::{
    binlog::{
        consts::BinlogVersion::Version4,
        events::{Event, TableMapEvent},
        EventStreamReader,
    },
    io::ParseBuf,
    packets::{ErrPacket, NetworkStreamTerminator, OkPacketDeserializer},
};

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{error::DriverError, io::ReadPacket, Conn, Result};

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
    pub fn get_tme(&self, table_id: u64) -> Option<&TableMapEvent<'static>> {
        self.esr.get_tme(table_id)
    }
}

impl futures_core::stream::Stream for BinlogStream {
    type Item = Result<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let packet = match ready!(Pin::new(&mut self.read_packet).poll(cx)) {
            Ok(packet) => packet,
            Err(err) => return Poll::Ready(Some(Err(err.into()))),
        };

        let first_byte = packet.get(0).copied();

        if first_byte == Some(255) {
            if let Ok(ErrPacket::Error(err)) =
                ParseBuf(&*packet).parse(self.read_packet.conn_ref().capabilities())
            {
                return Poll::Ready(Some(Err(From::from(err))));
            }
        }

        if first_byte == Some(254) && packet.len() < 8 {
            if ParseBuf(&*packet)
                .parse::<OkPacketDeserializer<NetworkStreamTerminator>>(
                    self.read_packet.conn_ref().capabilities(),
                )
                .is_ok()
            {
                return Poll::Ready(None);
            }
        }

        if first_byte == Some(0) {
            let event_data = &packet[1..];
            match self.esr.read(event_data) {
                Ok(event) => {
                    return Poll::Ready(Some(Ok(event)));
                }
                Err(err) => return Poll::Ready(Some(Err(err.into()))),
            }
        } else {
            return Poll::Ready(Some(Err(DriverError::UnexpectedPacket {
                payload: packet.to_vec(),
            }
            .into())));
        }
    }
}
