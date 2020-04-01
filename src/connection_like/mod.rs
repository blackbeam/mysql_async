// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::{
    connection_like::{
        read_packet::{ReadPacket, ReadPackets},
        write_packet::WritePacket,
    },
    consts::Command,
};

pub mod read_packet;
pub mod write_packet;

pub trait ConnectionLike: Send + Sized {
    fn conn_ref(&self) -> &crate::Conn;
    fn conn_mut(&mut self) -> &mut crate::Conn;

    fn read_packet<'a>(&'a mut self) -> ReadPacket<'a, Self> {
        ReadPacket::new(self)
    }

    /// Returns future that reads packets from a server.
    fn read_packets<'a>(&'a mut self, n: usize) -> ReadPackets<'a, Self> {
        ReadPackets::new(self, n)
    }

    fn write_packet<T>(&mut self, data: T) -> WritePacket<'_, Self>
    where
        T: Into<Vec<u8>>,
    {
        WritePacket::new(self, data.into())
    }

    /// Returns future that sends full command body to a server.
    fn write_command_raw<'a>(&'a mut self, body: Vec<u8>) -> WritePacket<'a, Self> {
        debug_assert!(body.len() > 0);
        self.conn_mut().reset_seq_id();
        self.write_packet(body)
    }

    /// Returns future that writes command to a server.
    fn write_command_data<T>(&mut self, cmd: Command, cmd_data: T) -> WritePacket<'_, Self>
    where
        T: AsRef<[u8]>,
    {
        let cmd_data = cmd_data.as_ref();
        let mut body = Vec::with_capacity(1 + cmd_data.len());
        body.push(cmd as u8);
        body.extend_from_slice(cmd_data);
        self.write_command_raw(body)
    }
}
