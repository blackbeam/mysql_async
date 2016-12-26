// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use Column;
use Conn;
use conn::stmt::InnerStmt;
use conn::futures::Columns;
use conn::futures::HandleLocalInfile;
use conn::futures::new_handle_local_infile;
use conn::futures::query_result::Protocol;
use conn::futures::query_result::ResultKind;
use conn::futures::query_result::RawQueryResult;
use conn::futures::query_result::new_raw as new_raw_query_result;
use conn::futures::ReadPacket;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::OkPacket;
use proto::Packet;
use proto::PacketType;
use proto::read_lenenc_int;
use std::marker::PhantomData;


enum Step {
    ReadPacket(ReadPacket),
    ReadColumns(Columns),
    HandleLocalInfile(HandleLocalInfile),
}

enum Out {
    ReadPacket((Conn, Packet)),
    ReadColumns((Conn, Vec<Column>)),
    HandleLocalInfile(Conn),
}

/// Future that resolves to `RawQueryResult`.
///
/// It could be a part of query or statement execution.
pub struct NewRawQueryResult<K: ResultKind + ?Sized> {
    step: Step,
    inner_stmt: Option<InnerStmt>,
    _phantom: PhantomData<K>,
}

pub fn new<K: ResultKind + ?Sized>(read_packet: ReadPacket,
                                   inner_stmt: Option<InnerStmt>)
                                   -> NewRawQueryResult<K> {
    NewRawQueryResult {
        step: Step::ReadPacket(read_packet),
        inner_stmt: inner_stmt,
        _phantom: PhantomData,
    }
}

impl<K: ResultKind + ?Sized> NewRawQueryResult<K> {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::ReadPacket(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadPacket(val)))
            },
            Step::HandleLocalInfile(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::HandleLocalInfile(val)))
            },
            Step::ReadColumns(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadColumns(val)))
            },
        }
    }
}

impl<K: ResultKind + ?Sized> Future for NewRawQueryResult<K> {
    type Item = RawQueryResult<K>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::ReadPacket((conn, packet)) => {
                if packet.is(PacketType::Ok) {
                    let ok_packet = OkPacket::new(packet, conn.capabilities);
                    let query_result = new_raw_query_result::<K, _>(conn,
                                                                    vec![],
                                                                    ok_packet,
                                                                    self.inner_stmt.clone());
                    Ok(Ready(query_result))
                } else {
                    if K::protocol() == Protocol::Text && packet.as_ref()[0] == 0xfb {
                        let filename = &packet.as_ref()[1..];
                        let handler = conn.opts.get_local_infile_handler();
                        self.step = Step::HandleLocalInfile(new_handle_local_infile(conn,
                                                                                    filename,
                                                                                    handler));
                        self.poll()
                    } else {
                        let column_count = read_lenenc_int(&mut packet.as_ref())?;
                        self.step = Step::ReadColumns(conn.read_result_set_columns(column_count));
                        self.poll()
                    }
                }
            },
            Out::HandleLocalInfile(conn) => {
                self.step = Step::ReadPacket(conn.read_packet());
                self.poll()
            },
            Out::ReadColumns((conn, columns)) => {
                let query_result =
                    new_raw_query_result::<K, _>(conn, columns, None, self.inner_stmt.clone());
                Ok(Ready(query_result))
            },
        }
    }
}
