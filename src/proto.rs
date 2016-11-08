// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use byteorder::LittleEndian as LE;
use byteorder::{ReadBytesExt, WriteBytesExt};

use consts::{
    self,
    CapabilityFlags,
    ColumnType,
    StatusFlags,
};

use errors::*;

use regex::Regex;

use scramble::scramble;

use std::borrow::Cow;
use std::cmp;
use std::fmt;
use std::io::{
    self,
    Write,
};
use std::ops::Index;
use std::str;
use std::sync::Arc;

use value::{
    from_value,
    FromValue,
    Value,
};
use value::Value::*;

lazy_static! {
    static ref VERSION_RE: Regex = {
        Regex::new(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})(.*)").unwrap()
    };
}

pub fn lenenc_le(mut data: &[u8]) -> Option<(u64, u8)> {
    let head_byte = match data.read_u8() {
        Ok(x) => x,
        _ => return None,
    };
    let length = match head_byte {
        0xfc => 2,
        0xfd => 3,
        0xfe => 8,
        x => return Some((x as u64, 1)),
    };
    data.read_uint::<LE>(length).ok().map(|x| (x, length as u8))
}

pub fn u32_le(data: &[u8]) -> Option<u32> {
    if data.len() >= 4 {
        Some(
            ((data[3] as u32) << 24) +
            ((data[2] as u32) << 16) +
            ((data[1] as u32) << 8) +
            data[0] as u32
        )
    } else {
        None
    }
}

pub fn u24_le(data: &[u8]) -> Option<u32> {
    if data.len() >= 3 {
        Some(((data[2] as u32) << 16) + ((data[1] as u32) << 8) + data[0] as u32)
    } else {
        None
    }
}

pub fn u16_le(data: &[u8]) -> Option<u16> {
    if data.len() >= 2 {
        Some(((data[1] as u16) << 8) + data[0] as u16)
    } else {
        None
    }
}

pub fn write_lenenc_int<W>(writer: &mut W, x: u64) -> Result<()>
where W: Write,
{
    if x < 251 {
        try!(writer.write_u8(x as u8));
    } else if x < 65_536 {
        try!(writer.write_u8(0xFC));
        try!(writer.write_uint::<LE>(x, 2))
    } else if x < 16_777_216 {
        try!(writer.write_u8(0xFD));
        try!(writer.write_uint::<LE>(x, 3))
    } else {
        try!(writer.write_u8(0xFE));
        try!(writer.write_uint::<LE>(x, 8))
    }
    Ok(())
}

pub fn write_lenenc_bytes<W>(writer: &mut W, bytes: &[u8]) -> Result<()>
where W: Write,
{
    try!(write_lenenc_int(writer, bytes.len() as u64));
    try!(writer.write_all(bytes));
    Ok(())
}

pub fn read_lenenc_int<R>(reader: &mut R) -> Result<u64>
where R: io::Read,
{
    let head_byte = try!(reader.read_u8());
    let length = match head_byte {
        0xfc => 2,
        0xfd => 3,
        0xfe => 8,
        x => return Ok(x as u64),
    };
    let out = try!(reader.read_uint::<LE>(length));
    Ok(out)
}

pub fn read_lenenc_bytes<R>(reader: &mut R) -> Result<Vec<u8>>
where R: io::Read,
{
    let len = try!(read_lenenc_int(reader));
    let mut out = vec![0u8; len as usize];
    try!(reader.read_exact(&mut *out));
    Ok(out)
}



pub fn read_bin_value<R>(reader: &mut R,
                         col_type: ColumnType,
                         unsigned: bool,
                         num_flag: bool) -> Result<Value>
    where R: io::Read,
{
    match col_type {
        ColumnType::MYSQL_TYPE_STRING |
        ColumnType::MYSQL_TYPE_VAR_STRING |
        ColumnType::MYSQL_TYPE_BLOB |
        ColumnType::MYSQL_TYPE_TINY_BLOB |
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB |
        ColumnType::MYSQL_TYPE_LONG_BLOB |
        ColumnType::MYSQL_TYPE_SET |
        ColumnType::MYSQL_TYPE_ENUM |
        ColumnType::MYSQL_TYPE_DECIMAL |
        ColumnType::MYSQL_TYPE_VARCHAR |
        ColumnType::MYSQL_TYPE_BIT |
        ColumnType::MYSQL_TYPE_NEWDECIMAL |
        ColumnType::MYSQL_TYPE_GEOMETRY => {
            if num_flag {
                Ok(Int(try!(reader.read_i64::<LE>())))
            } else {
                Ok(Bytes(try!(read_lenenc_bytes(reader))))
            }
        },
        ColumnType::MYSQL_TYPE_TINY => {
            if unsigned {
                Ok(Int(try!(reader.read_u8()) as i64))
            } else {
                Ok(Int(try!(reader.read_i8()) as i64))
            }
        },
        ColumnType::MYSQL_TYPE_SHORT |
        ColumnType::MYSQL_TYPE_YEAR => {
            if unsigned {
                Ok(Int(try!(reader.read_u16::<LE>()) as i64))
            } else {
                Ok(Int(try!(reader.read_i16::<LE>()) as i64))
            }
        },
        ColumnType::MYSQL_TYPE_LONG |
        ColumnType::MYSQL_TYPE_INT24 => {
            if unsigned {
                Ok(Int(try!(reader.read_u32::<LE>()) as i64))
            } else {
                Ok(Int(try!(reader.read_i32::<LE>()) as i64))
            }
        },
        ColumnType::MYSQL_TYPE_LONGLONG => {
            if unsigned {
                Ok(UInt(try!(reader.read_u64::<LE>())))
            } else {
                Ok(Int(try!(reader.read_i64::<LE>())))
            }
        },
        ColumnType::MYSQL_TYPE_FLOAT => {
            Ok(Float(try!(reader.read_f32::<LE>()) as f64))
        },
        ColumnType::MYSQL_TYPE_DOUBLE => {
            Ok(Float(try!(reader.read_f64::<LE>())))
        },
        ColumnType::MYSQL_TYPE_TIMESTAMP |
        ColumnType::MYSQL_TYPE_DATE |
        ColumnType::MYSQL_TYPE_DATETIME => {
            let len = try!(reader.read_u8());
            let mut year = 0u16;
            let mut month = 0u8;
            let mut day = 0u8;
            let mut hour = 0u8;
            let mut minute = 0u8;
            let mut second = 0u8;
            let mut micro_second = 0u32;
            if len >= 4u8 {
                year = try!(reader.read_u16::<LE>());
                month = try!(reader.read_u8());
                day = try!(reader.read_u8());
            }
            if len >= 7u8 {
                hour = try!(reader.read_u8());
                minute = try!(reader.read_u8());
                second = try!(reader.read_u8());
            }
            if len == 11u8 {
                micro_second = try!(reader.read_u32::<LE>());
            }
            Ok(Date(year, month, day, hour, minute, second, micro_second))
        },
        ColumnType::MYSQL_TYPE_TIME => {
            let len = try!(reader.read_u8());
            let mut is_negative = false;
            let mut days = 0u32;
            let mut hours = 0u8;
            let mut minutes = 0u8;
            let mut seconds = 0u8;
            let mut micro_seconds = 0u32;
            if len >= 8u8 {
                is_negative = try!(reader.read_u8()) == 1u8;
                days = try!(reader.read_u32::<LE>());
                hours = try!(reader.read_u8());
                minutes = try!(reader.read_u8());
                seconds = try!(reader.read_u8());
            }
            if len == 12u8 {
                micro_seconds = try!(reader.read_u32::<LE>());
            }
            Ok(Time(is_negative, days, hours, minutes, seconds, micro_seconds))
        },
        _ => Ok(NULL),
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum PacketType {
    Ok,
    Eof,
    Err,
}

#[derive(Debug)]
pub struct Packet {
    payload: Vec<u8>,
}

impl Packet {
    pub fn is(&self, type_: PacketType) -> bool {
        match self.payload[0] {
            0x00 if type_ == PacketType::Ok && self.payload.len() >= 7 => true,
            0xfe if self.payload.len() <= 9 && type_ == PacketType::Eof => true,
            0xff if type_ == PacketType::Err => true,
            _ => false,
        }
    }

    pub fn as_ref(&self) -> &[u8] {
        &self.payload[..]
    }
}

#[derive(Debug)]
pub enum ParseResult {
    NeedHeader(NewPacket, usize),
    Incomplete(NewPacket, usize),
    Done(Packet, u8),
}

#[derive(Debug)]
pub struct NewPacket {
    data: Vec<u8>,
    length: usize,
    header: Vec<u8>,
    last_seq_id: u8,
}

impl NewPacket {
    pub fn empty() -> NewPacket {
        NewPacket {
            data: Vec::new(),
            length: 0,
            header: Vec::with_capacity(4),
            last_seq_id: 0,
        }
    }

    pub fn push(&mut self, byte: u8) {
        self.data.push(byte);
    }

    pub fn push_header(&mut self, byte: u8) {
        assert!(self.header.len() < 4);
        self.header.push(byte);
    }

    pub fn parse(mut self) -> ParseResult {
        let last_packet_part = self.data.len() % consts::MAX_PAYLOAD_LEN;
        if last_packet_part == 0 {
            if self.header.len() != 4 {
                let needed = 4 - self.header.len();
                return ParseResult::NeedHeader(self, needed);
            } else {
                let length = u24_le(&*self.header).unwrap();
                self.last_seq_id = self.header[3];
                debug!("Last seq id {}", self.last_seq_id);
                self.header.clear();
                if length == 0 {
                    return ParseResult::Done(Packet { payload: self.data }, self.last_seq_id);
                } else {
                    self.length = length as usize;
                    return ParseResult::Incomplete(self, length as usize);
                }
            }
        } else {
            if last_packet_part == self.length {
                return ParseResult::Done(Packet { payload: self.data }, self.last_seq_id);
            } else {
                let length = self.length;
                return ParseResult::Incomplete(self, length - last_packet_part);
            }
        }
    }
}

#[derive(Debug)]
pub struct HandshakePacket {
    packet: Packet,
    srv_ver_len: usize,
    more_data: bool,
}

impl HandshakePacket {
    pub fn new(packet: Packet) -> HandshakePacket {
        let (srv_ver_len, more_data) = {
            let mut payload = &packet.payload[..];
            let mut srv_ver_len = 0;
            for (i, byte) in payload.iter().enumerate() {
                if *byte == 0 {
                    srv_ver_len = i - 1;
                    break;
                }
            }

            payload = &payload[(1 + srv_ver_len + 1 + 4 + 8 + 1)..];
            let more_data = payload.len() > 0;

            (srv_ver_len, more_data)
        };

        HandshakePacket {
            packet: packet,
            srv_ver_len: srv_ver_len,
            more_data: more_data,
        }
    }

    pub fn srv_ver_bytes(&self) -> &[u8] {
        &self.packet.payload[1..(4 + 1 + self.srv_ver_len)]
    }

    pub fn srv_ver(&self) -> Cow<str> {
        String::from_utf8_lossy(self.srv_ver_bytes())
    }

    pub fn srv_ver_parsed(&self) -> Result<(u16, u16, u16)> {
        let ver_str = self.srv_ver().into_owned();
        VERSION_RE.captures(&ver_str[..])
            .and_then(|capts| {
                Some((
                    (capts.at(1).unwrap().parse::<u16>()).unwrap_or(0),
                    (capts.at(2).unwrap().parse::<u16>()).unwrap_or(0),
                    (capts.at(3).unwrap().parse::<u16>()).unwrap_or(0),
                ))
            }).and_then(|version| {
            if version == (0, 0, 0) {
                None
            } else {
                Some(version)
            }
        }).ok_or(ErrorKind::CantParseVersion(ver_str).into())
    }

    pub fn conn_id(&self) -> u32 {
        u32_le(&self.packet.payload[(1 + self.srv_ver_len + 1)..]).expect("should be here")
    }

    pub fn auth_plug_data_len(&self) -> Option<u8> {
        let offset = 1 + self.srv_ver_len + 1 + 4 + 8 + 1 + 2 + 1 + 2 + 2;
        if self.more_data && self.capabilities().contains(consts::CLIENT_PLUGIN_AUTH) {
            Some(cmp::max(13, self.packet.payload[offset] - 8))
        } else {
            None
        }
    }

    pub fn auth_plug_data_1(&self) -> &[u8] {
        let offset = 1 + self.srv_ver_len + 1 + 4;
        &self.packet.payload[offset..(offset + 8)]
    }

    pub fn auth_plug_data_2(&self) -> Option<&[u8]> {
        let offset = 1 + self.srv_ver_len + 1 + 4 + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10;
        match self.auth_plug_data_len() {
            Some(length) => {
                let length = length as usize;
                let mut auth_plug_data_2 = &self.packet.payload[offset..offset+length];
                if auth_plug_data_2[auth_plug_data_2.len() - 1] == 0 {
                    auth_plug_data_2 = &auth_plug_data_2[..auth_plug_data_2.len() - 1];
                }
                Some(auth_plug_data_2)
            },
            None => None,
        }
    }

    pub fn capabilities(&self) -> CapabilityFlags {
        let offset1 = 1 + self.srv_ver_len + 1 + 4 + 8 + 1;
        let offset2 = offset1 + 2 + 1 + 2;
        let lower = u16_le(&self.packet.payload[offset1..]).expect("should be here");
        let mut capabilityes = CapabilityFlags::from_bits_truncate(lower as u32);
        if self.more_data {
            let upper = u16_le(&self.packet.payload[offset2..]).expect("should be here");
            capabilityes.insert(CapabilityFlags::from_bits_truncate((upper as u32) << 16));
        }
        capabilityes
    }

    pub fn status_flags(&self) -> Option<StatusFlags> {
        let offset = 1 + self.srv_ver_len + 1 + 4 + 8 + 1 + 2 + 1;
        if self.more_data {
            let value = u16_le(&self.packet.payload[offset..]).expect("should be here");
            Some(StatusFlags::from_bits_truncate(value))
        } else {
            None
        }
    }

    pub fn charset(&self) -> Option<u8> {
        let offset = 1 + self.srv_ver_len + 1 + 4 + 8 + 1 + 2;
        if self.more_data {
            Some(self.packet.payload[offset])
        } else {
            None
        }
    }

    pub fn auth_plugin_name_bytes(&self) -> Option<&[u8]> {
        self.auth_plug_data_len().map(|length| {
            let length = length as usize;
            let offset = 1 + self.srv_ver_len + 1 + 4 + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10 + length;
            let mut auth_plugin_name = &self.packet.payload[offset..];
            if auth_plugin_name[auth_plugin_name.len() - 1] == 0 {
                auth_plugin_name = &auth_plugin_name[..auth_plugin_name.len() - 1];
            }
            auth_plugin_name
        })
    }

    pub fn auth_plugin_name(&self) -> Option<Cow<str>> {
        self.auth_plugin_name_bytes().map(|bytes| {
            String::from_utf8_lossy(bytes)
        })
    }
}

#[derive(Debug)]
pub struct HandshakeResponse {
    data: Vec<u8>,
}

impl HandshakeResponse {
    pub fn new<U, P, D>(handshake: &HandshakePacket,
                        user: Option<U>,
                        passwd: Option<P>,
                        database: Option<D>) -> HandshakeResponse
        where U: AsRef<[u8]> + Clone,
              P: AsRef<[u8]>,
              D: AsRef<[u8]> + Clone,
    {
        let mut client_flags = consts::CLIENT_PROTOCOL_41 |
                               consts::CLIENT_SECURE_CONNECTION |
                               consts::CLIENT_LONG_PASSWORD |
                               consts::CLIENT_TRANSACTIONS |
                               consts::CLIENT_LOCAL_FILES |
                               consts::CLIENT_MULTI_STATEMENTS |
                               consts::CLIENT_MULTI_RESULTS |
                               consts::CLIENT_PS_MULTI_RESULTS;
        if let Some(ref database) = database {
            if database.as_ref().len() > 0 {
                client_flags.insert(consts::CLIENT_CONNECT_WITH_DB);
            }
        }

        let scramble_buf = passwd.and_then(|passwd| {
            scramble(handshake.auth_plug_data_1(), handshake.auth_plug_data_2(), passwd.as_ref())
        });

        let user_len = user.clone().map(|user| user.as_ref().len()).unwrap_or(0);
        let database_len = database.clone().map(|database| database.as_ref().len()).unwrap_or(0);
        let scramble_len = scramble_buf.map(|scramble_buf| scramble_buf.len()).unwrap_or(0);

        let mut payload_len = 4 + 4 + 1 + 23 + user_len + 1 + 1 + scramble_len;
        if database_len > 0 {
            payload_len += database_len + 1;
        }

        let mut data = vec![0u8; payload_len];
        {
            let mut writer = &mut *data;
            writer.write_u32::<LE>(client_flags.bits()).unwrap();
            writer.write_all(&[0u8; 4]).unwrap();
            let mut collation = consts::UTF8_GENERAL_CI;
            if let Ok(version) = handshake.srv_ver_parsed() {
                if version >= (5, 5, 3) {
                    collation = consts::UTF8MB4_GENERAL_CI;
                }
            }
            writer.write_u8(collation).unwrap();
            writer.write_all(&[0u8; 23]).unwrap();
            if let Some(user) = user {
                writer.write_all(user.as_ref()).unwrap();
            }
            writer.write_u8(0).unwrap();
            writer.write_u8(scramble_len as u8).unwrap();
            if let Some(scramble_buf) = scramble_buf {
                writer.write_all(&scramble_buf[..]).unwrap();
            }
            if let Some(database) = database {
                if database_len > 0 {
                    writer.write_all(database.as_ref()).unwrap();
                    writer.write_u8(0).unwrap();
                }
            }
        }

        HandshakeResponse {
            data: data,
        }
    }

    pub fn as_ref(&self) -> &[u8] {
        &self.data[..]
    }
}

#[derive(Debug, Clone)]
pub struct OkPacket {
    data: Vec<u8>,
    affected_rows: u64,
    last_insert_id: u64,
    status_flags: StatusFlags,
    warnings: u16,
    info: (usize, usize),
    session_state_changes: Option<(usize, usize)>,
}

impl OkPacket {
    pub fn new(packet: Packet, capabilities: CapabilityFlags) -> Option<OkPacket> {
        if packet.is(PacketType::Ok) {
            let mut offset = 1;

            let (affected_rows, affected_rows_len) = lenenc_le(&packet.payload[offset..]).expect("should be here 5");
            offset += affected_rows_len as usize;

            let (last_insert_id, last_insert_id_len) = lenenc_le(&packet.payload[offset..]).expect("should be here 6");
            offset += last_insert_id_len as usize;

            let status_flags = u16_le(&packet.payload[offset..]).expect("should be here 7");
            let status_flags = StatusFlags::from_bits_truncate(status_flags);
            offset += 2;

            let warnings = u16_le(&packet.payload[offset..]).expect("should be here 8");
            offset += 2;

            let info;
            let mut session_state_changes = None;
            if capabilities.contains(consts::CLIENT_SESSION_TRACK) {
                let (info_len, info_len_len) = lenenc_le(&packet.payload[offset..]).expect("should be here 9");
                offset += info_len_len as usize;
                info = (offset, offset + info_len as usize);
                if status_flags.contains(consts::SERVER_SESSION_STATE_CHANGED) {
                    let (sess_state_changes_len, sess_state_changes_len_len) =
                        lenenc_le(&packet.payload[offset..]).expect("should be here 10");
                    offset += sess_state_changes_len_len as usize;
                    session_state_changes = Some((offset, offset + sess_state_changes_len as usize));
                }
            } else {
                info = (offset, packet.payload.len());
            }

            Some(OkPacket {
                data: packet.payload,
                affected_rows: affected_rows,
                last_insert_id: last_insert_id,
                status_flags: status_flags,
                warnings: warnings,
                info: info,
                session_state_changes: session_state_changes,
            })
        } else {
            None
        }
    }

    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    pub fn status_flags(&self) -> StatusFlags {
        self.status_flags
    }

    pub fn warnings(&self) -> u16 {
        self.warnings
    }

    pub fn info_bytes(&self) -> &[u8] {
        &self.data[self.info.0..self.info.1]
    }

    pub fn info(&self) -> Cow<str> {
        String::from_utf8_lossy(self.info_bytes())
    }

    pub fn session_state_changes_bytes(&self) -> Option<&[u8]> {
        if let Some((start, end)) = self.session_state_changes {
            Some(&self.data[start..end])
        } else {
            None
        }
    }

    pub fn session_state_changes(&self) -> Option<Cow<str>> {
        self.session_state_changes_bytes().map(String::from_utf8_lossy)
    }

    pub fn unwrap(self) -> Packet {
        Packet { payload: self.data }
    }
}

/// MySql error packet.
///
/// `Debug` implementation will renders something similar to console mysql client error message.
pub struct ErrPacket {
    data: Vec<u8>,
}

impl ErrPacket {
    #[doc(hidden)]
    pub fn new(packet: Packet) -> Option<ErrPacket> {
        if packet.is(PacketType::Err) {
            Some(ErrPacket {
                data: packet.payload,
            })
        } else {
            None
        }
    }

    pub fn error_code(&self) -> u16 {
        let offset = 1;
        u16_le(&self.data[offset..]).expect("should be here")
    }

    pub fn state_bytes(&self) -> &[u8] {
        let offset = 1 + 2 + 1;
        if self.data[3] == b'#' {
            &self.data[offset..offset + 5]
        } else {
            &b""[..]
        }
    }

    pub fn state(&self) -> Cow<str> {
        String::from_utf8_lossy(self.state_bytes())
    }

    pub fn message_bytes(&self) -> &[u8] {
        let offset = if self.data[3] == b'#' {
            1 + 2 + 1 + 5
        } else {
            1 + 2
        };
        &self.data[offset..]
    }

    pub fn message(&self) -> Cow<str> {
        String::from_utf8_lossy(self.message_bytes())
    }
}

impl fmt::Debug for ErrPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ERROR {} ({}): {}", self.error_code(), self.state(), self.message())
    }
}

pub struct EofPacket {
    data: Vec<u8>,
}

impl EofPacket {
    pub fn new(packet: Packet) -> Option<EofPacket> {
        if packet.is(PacketType::Eof) {
            Some(EofPacket {
                data: packet.payload,
            })
        } else {
            None
        }
    }

    pub fn warnings(&self) -> u16 {
        let offset = 1;
        u16_le(&self.data[offset..]).expect("should be here")
    }

    pub fn status_flags(&self) -> StatusFlags {
        let offset = 1 + 2;
        let status_flags_bytes = u16_le(&self.data[offset..]).expect("should be here");
        StatusFlags::from_bits_truncate(status_flags_bytes)
    }

    pub fn unwrap(self) -> Packet {
        Packet { payload: self.data }
    }
}

/// Row of a result set.
///
/// Row could be indexed by numeric column index or by column name.
#[derive(Clone, PartialEq, Debug)]
pub struct Row {
    values: Vec<Option<Value>>,
    columns: Arc<Vec<Column>>
}

impl Row {
    /// Creates instance of `Row` from raw row representation
    #[doc(hidden)]
    pub fn new(raw_row: Vec<Value>, columns: Arc<Vec<Column>>) -> Row {
        Row {
            values: raw_row.into_iter().map(|value| Some(value)).collect(),
            columns: columns
        }
    }

    /// Returns length of a row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns reference to the value of a column with index `index` if it exists and wasn't taken
    /// by `Row::take` method.
    ///
    /// Non panicking version of `row[usize]`.
    pub fn as_ref(&self, index: usize) -> Option<&Value> {
        self.values.get(index).and_then(|x| x.as_ref())
    }

    /// Will copy value at index `index` if it was not taken by `Row::take` earlier,
    /// then will convert it to `T`.
    pub fn get<T, I>(&mut self, index: I) -> Option<T>
        where T: FromValue,
              I: ColumnIndex {
        index.idx(&*self.columns).and_then(|idx| {
            self.values.get(idx).and_then(|x| x.as_ref()).map(|x| from_value::<T>(x.clone()))
        })
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will converts it to `T`.
    pub fn take<T, I>(&mut self, index: I) -> Option<T>
        where T: FromValue,
              I: ColumnIndex {
        index.idx(&*self.columns).and_then(|idx| {
            self.values.get_mut(idx).and_then(|x| x.take()).map(from_value::<T>)
        })
    }

    /// Unwraps values of a row.
    ///
    /// # Panics
    ///
    /// Panics if any of columns was taken by `take` method.
    pub fn unwrap(self) -> Vec<Value> {
        self.values.into_iter()
            .map(|x| x.expect("Can't unwrap row if some of columns was taken"))
            .collect()
    }

    #[doc(hidden)]
    pub fn place(&mut self, index: usize, value: Value) {
        self.values[index] = Some(value);
    }
}

impl Index<usize> for Row {
    type Output = Value;

    fn index<'a>(&'a self, index: usize) -> &'a Value {
        self.values[index].as_ref().unwrap()
    }
}

impl<'a> Index<&'a str> for Row {
    type Output = Value;

    fn index<'r>(&'r self, index: &'a str) -> &'r Value {
        for (i, column) in self.columns.iter().enumerate() {
            if column.name() == index.as_bytes() {
                return self.values[i].as_ref().unwrap();
            }
        }
        panic!("No such column: `{}`", index);
    }
}

pub trait ColumnIndex {
    fn idx(&self, columns: &Vec<Column>) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn idx(&self, columns: &Vec<Column>) -> Option<usize> {
        if *self >= columns.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl<'a> ColumnIndex for &'a str {
    fn idx(&self, columns: &Vec<Column>) -> Option<usize> {
        for (i, c) in columns.iter().enumerate() {
            if c.name() == self.as_bytes() {
                return Some(i);
            }
        }
        None
    }
}

/// Mysql column.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Column {
    payload: Vec<u8>,
    schema: (usize, usize),
    table: (usize, usize),
    org_table: (usize, usize),
    name: (usize, usize),
    org_name: (usize, usize),
    default_values: Option<(usize, usize)>,
    pub column_length: u32,
    pub character_set: u16,
    pub flags: consts::ColumnFlags,
    pub column_type: consts::ColumnType,
    pub decimals: u8,
}

impl Column {
    #[doc(hidden)]
    pub fn new(packet: Packet, last_command: consts::Command) -> Column {
        let (schema, table, org_table, name, org_name, defaults, column_len, charset, type_, flags, decimals) = {
            let mut reader = packet.as_ref();
            let mut offset = 0;

            let (len, size) = lenenc_le(&mut reader).map(|(a, b)| (a as usize, b as usize)).expect("1");
            // skip catalog
            offset += size + len;
            reader = &reader[size + len..];

            let (len, size) = lenenc_le(&mut reader).map(|(a, b)| (a as usize, b as usize)).expect("2");
            let schema = (offset + size, len);
            offset += size + len;
            reader = &reader[size + len..];

            let (len, size) = lenenc_le(&mut reader).map(|(a, b)| (a as usize, b as usize)).expect("3");
            let table = (offset + size, len);
            offset += size + len;
            reader = &reader[size + len..];

            let (len, size) = lenenc_le(&mut reader).map(|(a, b)| (a as usize, b as usize)).expect("4");
            let org_table = (offset + size, len);
            offset += size + len;
            reader = &reader[size + len..];

            let (len, size) = lenenc_le(&mut reader).map(|(a, b)| (a as usize, b as usize)).expect("5");
            let name = (offset + size, len);
            offset += size + len;
            reader = &reader[size + len..];

            let (len, size) = lenenc_le(&mut reader).map(|(a, b)| (a as usize, b as usize)).expect("6");
            let org_name = (offset + size, len);
            offset += size + len;
            reader = &reader[size + len..];

            offset += 1;
            reader = &reader[1..];

            let charset = u16_le(reader).expect("7");
            offset += 2;
            reader = &reader[2..];

            let column_len = u32_le(reader).expect("8");
            offset += 4;
            reader = &reader[4..];

            let type_: consts::ColumnType = reader[0].into();
            offset += 1;
            reader = &reader[1..];

            let flags = u16_le(reader).expect("9");
            offset += 2;
            reader = &reader[2..];

            let decimals = reader[0];
            offset += 1;
            reader = &reader[1..];

            offset += 2;

            let defaults = if last_command == consts::Command::COM_FIELD_LIST {
                let (len, size) = lenenc_le(&mut reader).map(|(a, b)| (a as usize, b as usize)).expect("10");
                Some((offset + size, len))
            } else {
                None
            };
            (schema, table, org_table, name, org_name, defaults, column_len, charset, type_, flags, decimals)
        };

        Column {
            payload: packet.payload,
            schema: schema,
            table: table,
            org_table: org_table,
            name: name,
            org_name: org_name,
            default_values: defaults,
            column_length: column_len,
            character_set: charset,
            flags: consts::ColumnFlags::from_bits_truncate(flags),
            column_type: type_,
            decimals: decimals,
        }
    }

    /// Schema name (see [`Column`](http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition)).
    pub fn schema<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.schema.0..self.schema.0+self.schema.1]
    }

    /// Virtual table name (see [`Column`](http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition)).
    pub fn table<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.table.0..self.table.0+self.table.1]
    }

    /// Physical table name (see [`Column`](http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition)).
    pub fn org_table<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.org_table.0..self.org_table.0+self.org_table.1]
    }

    /// Virtual column name (see [`Column`](http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition)).
    pub fn name<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.name.0..self.name.0+self.name.1]
    }

    /// Physical column name (see [`Column`](http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition)).
    pub fn org_name<'a>(&'a self) -> &'a [u8] {
        &self.payload[self.org_name.0..self.org_name.0+self.org_name.1]
    }

    /// Default values (see [`Column`](http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition)).
    pub fn default_values<'a>(&'a self) -> Option<&'a [u8]> {
        self.default_values.map(|(offset, len)| {
            &self.payload[offset..offset + len]
        })
    }
}
