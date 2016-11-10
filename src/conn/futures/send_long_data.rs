// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use byteorder::WriteBytesExt;
use byteorder::LittleEndian as LE;
use conn::Conn;
use conn::futures::write_packet::WritePacket;
use conn::stmt::InnerStmt;
use consts::Command;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Done;
use lib_futures::done;
use lib_futures::Future;
use lib_futures::Poll;
use std::io::Write;
use std::mem;
use value::Value;
use value::Value::*;


steps! {
    SendLongData {
        Init(Done<Conn, Error>),
        WritePacket(WritePacket),
    }
}

/// Future that sends part of statement parametes via MySql's `COM_STMT_SEND_LONG_DATA`.
///
/// It resolves to `Conn`, `InnerStmt` and statement parameters.
pub struct SendLongData {
    step: Step,
    inner_stmt: Option<InnerStmt>,
    params: Vec<Value>,
    ids: Vec<u16>,
    next_chunk: usize,
}

impl SendLongData {
    fn next_chunk(&mut self, max_allowed_packet: usize) -> Option<Vec<u8>> {
        let chunk_meta = match self.ids.pop() {
            Some(id) => match self.params[id as usize] {
                Bytes(ref x) => {
                    let stmt_id = self.inner_stmt.as_ref().unwrap().statement_id;
                    let mut chunks = x.chunks(max_allowed_packet - 8);
                    match chunks.nth(self.next_chunk) {
                        Some(chunk) => {
                            let mut buf = Vec::with_capacity(chunk.len() + 7);
                            buf.write_u32::<LE>(stmt_id).unwrap();
                            buf.write_u16::<LE>(id).unwrap();
                            buf.write_all(chunk).unwrap();
                            if chunk.len() < max_allowed_packet - 8 {
                                Some((buf, 0, None))
                            } else {
                                Some((buf, 1, Some(id)))
                            }
                        },
                        None => {
                            Some((Vec::new(), 0, None))
                        }
                    }
                },
                _ => unreachable!(),
            },
            None => None,
        };

        match chunk_meta {
            Some((data, next_chunk, maybe_id)) => {
                if let Some(id) = maybe_id {
                    self.ids.push(id);
                }
                self.next_chunk = next_chunk;
                if data.len() == 0 {
                    self.next_chunk(max_allowed_packet)
                } else {
                    Some(data)
                }
            },
            None => None,
        }
    }
}

pub fn new(conn: Conn, inner_stmt: InnerStmt, params: Vec<Value>, ids: Vec<u16>) -> SendLongData {
    SendLongData {
        step: Step::Init(done(Ok(conn))),
        inner_stmt: Some(inner_stmt),
        params: params,
        ids: ids,
        next_chunk: 0
    }
}

impl Future for SendLongData {
    type Item = (Conn, InnerStmt, Vec<Value>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::Init(conn) | Out::WritePacket(conn) => {
                match self.next_chunk(conn.max_allowed_packet as usize) {
                    Some(data) => {
                        let future = conn.write_command_data(Command::COM_STMT_SEND_LONG_DATA, data);
                        self.step = Step::WritePacket(future);
                        self.poll()
                    },
                    None => {
                        let stmt = self.inner_stmt.take().unwrap();
                        let params = mem::replace(&mut self.params, vec![]);
                        Ok(Ready((conn, stmt, params)))
                    }
                }
            },
        }
    }
}
