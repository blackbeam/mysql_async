// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use errors::*;
use std::fmt;
use lib_futures::Async::NotReady;
use lib_futures::Async::Ready;
use lib_futures::Poll;
use lib_futures::stream;
use io::futures::ConnectingStream;
use io::futures::new_connecting_stream;
use io::futures::new_write_packet;
use io::futures::WritePacket;
use proto::NewPacket;
use proto::Packet;
use proto::ParseResult;
use std::cmp;
use std::collections::vec_deque::VecDeque;
use std::io;
use std::io::Read;
use std::net::ToSocketAddrs;
use tokio::net::TcpStream;
use tokio::reactor::Handle;


pub mod futures;


/// Stream connected to MySql server.
pub struct Stream {
    endpoint: Option<TcpStream>,
    closed: bool,
    next_packet: Option<ParseResult>,
    buf: Option<VecDeque<u8>>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Stream (endpoint={:?})", self.endpoint)
    }
}

impl Stream {
    pub fn connect<S>(addr: S, handle: &Handle) -> ConnectingStream
        where S: ToSocketAddrs,
    {
        new_connecting_stream(addr, handle)
    }

    pub fn write_packet(self, data: Vec<u8>, seq_id: u8) -> WritePacket {
        new_write_packet(self, data, seq_id)
    }

    pub fn set_keepalive_ms(&self, ms: Option<u32>) -> Result<()> {
        Ok(self.endpoint.as_ref().expect("Should be here").set_keepalive_ms(ms)?)
    }
}

impl io::Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.endpoint.as_mut().unwrap().read(buf)
    }
}

impl io::Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.endpoint.as_mut().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.endpoint.as_mut().unwrap().flush()
    }
}

impl stream::Stream for Stream {
    type Item = (Packet, u8);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<(Packet, u8)>, Error> {
        // should read everything from self.endpoint
        let mut would_block = false;
        if !self.closed {
            let mut buf = [0u8; 4096];
            loop {
                match self.endpoint.as_mut().unwrap().read(&mut buf[..]) {
                    Ok(0) => {
                        break;
                    },
                    Ok(size) => {
                        let buf_handle = self.buf.as_mut().unwrap();
                        buf_handle.extend(&buf[..size]);
                    },
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        would_block = true;
                        break;
                    },
                    Err(error) => {
                        self.closed = true;
                        return Err(Error::from(error));
                    },
                };
            }
        } else {
            return Ok(Ready(None));
        }

        // need to call again if there is a data in self.buf
        // or data was written to packet parser
        let mut should_poll = false;

        let next_packet = self.next_packet.take().expect("Stream.next_packet should not be None");
        let next_packet = match next_packet {
            ParseResult::Done(packet, seq_id) => {
                self.next_packet = Some(NewPacket::empty().parse());
                return Ok(Ready(Some((packet, seq_id))));
            },
            ParseResult::NeedHeader(mut new_packet, needed) => {
                let buf_handle = self.buf.as_mut().unwrap();
                let buf_len = buf_handle.len();
                for byte in buf_handle.drain(..cmp::min(needed, buf_len)) {
                    new_packet.push_header(byte);
                }
                if buf_len != 0 {
                    should_poll = true;
                }

                new_packet
            },
            ParseResult::Incomplete(mut new_packet, needed) => {
                let buf_handle = self.buf.as_mut().unwrap();
                let buf_len = buf_handle.len();
                for byte in buf_handle.drain(..cmp::min(needed, buf_len)) {
                    new_packet.push(byte);
                }
                if buf_len != 0 {
                    should_poll = true;
                }

                new_packet
            },
        };

        self.next_packet = Some(next_packet.parse());

        if should_poll || !would_block {
            self.poll()
        } else {
            Ok(NotReady)
        }
    }
}
