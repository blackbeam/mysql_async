use byteorder::{
    WriteBytesExt,
    LittleEndian as LE,
};

use Conn;
use conn::stmt::InnerStmt;
use consts::Command;

use errors::*;


use lib_futures::{
    Async,
    Future,
    Poll,
};

use std::io::Write;
use std::mem;

use super::WritePacket;

use Value;
use Value::*;

enum Step {
    WritePacket(WritePacket),
    Done(Option<Conn>),
}

enum Out {
    WritePacket(Conn),
    Done(Conn, InnerStmt),
}

pub struct SendLongData {
    step: Step,
    stmt: Option<InnerStmt>,
    params: Vec<Value>,
    ids: Vec<u16>,
    next_chunk: usize,
}

impl SendLongData {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::WritePacket(ref mut fut) => {
                Ok(Async::Ready(Out::WritePacket(try_ready!(fut.poll()))))
            },
            Step::Done(ref mut conn) => {
                let conn = conn.take().unwrap();
                let stmt = self.stmt.take().unwrap();
                Ok(Async::Ready(Out::Done(conn, stmt)))
            }
        }
    }
}

pub fn new(conn: Conn, stmt: InnerStmt, params: Vec<Value>, mut ids: Vec<u16>) -> SendLongData {
    let data = match ids.pop() {
        Some(id) => match params[id as usize] {
            Bytes(ref x) => {
                let max_allowed_packet = conn.max_allowed_packet as usize;
                let mut chunks = x.chunks(max_allowed_packet - 8);
                match chunks.next() {
                    Some(chunk) => {
                        let mut buf = Vec::with_capacity(chunk.len() + 7);
                        buf.write_u32::<LE>(stmt.statement_id).unwrap();
                        buf.write_u16::<LE>(id).unwrap();
                        buf.write_all(chunk).unwrap();
                        if chunk.len() < max_allowed_packet - 8 {
                            Some((buf, 0))
                        } else {
                            ids.push(id);
                            Some((buf, 1))
                        }
                    },
                    None => Some((vec![], 0)),
                }
            },
            _ => unreachable!(),
        },
        None => None,
    };
    match data {
        Some((data, next_chunk)) => {
            let future = conn.write_command_data(Command::COM_STMT_SEND_LONG_DATA, data);
            SendLongData {
                step: Step::WritePacket(future),
                stmt: Some(stmt),
                params: params,
                ids: ids,
                next_chunk: next_chunk,
            }
        },
        None => {
            SendLongData {
                step: Step::Done(Some(conn)),
                stmt: Some(stmt),
                params: vec![],
                ids: vec![],
                next_chunk: 0
            }
        }
    }
}

impl Future for SendLongData {
    type Item = (Conn, InnerStmt, Vec<Value>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::WritePacket(conn) => {
                let data = match self.ids.pop() {
                    Some(id) => match self.params[id as usize] {
                        Bytes(ref x) => {
                            let max_allowed_packet = conn.max_allowed_packet as usize;
                            let stmt_id = self.stmt.as_ref().unwrap().statement_id;
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
                                    Some((vec![], 0, None))
                                }
                            }
                        },
                        _ => unreachable!(),
                    },
                    None => None,
                };
                match data {
                    Some((data, next_chunk, id)) => {
                        if let Some(id) = id {
                            self.ids.push(id);
                        }
                        self.next_chunk = next_chunk;
                        let future = conn.write_command_data(Command::COM_STMT_SEND_LONG_DATA, data);
                        self.step = Step::WritePacket(future);
                        self.poll()
                    },
                    None => {
                        self.step = Step::Done(Some(conn));
                        self.poll()
                    }
                }
            },
            Out::Done(conn, stmt) => {
                let params = mem::replace(&mut self.params, vec![]);
                Ok(Async::Ready((conn, stmt, params)))
            },
        }
    }
}
