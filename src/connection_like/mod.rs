// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use BoxFuture;
use Opts;
use conn::named_params::parse_named_params;
use consts::{CapabilityFlags, Command, StatusFlags};
use errors::*;
use io;
use lib_futures::future::{Future, IntoFuture, Loop, err, ok, loop_fn};
use lib_futures::future::Either::*;
use local_infile_handler::LocalInfileHandler;
use proto::{Column, LocalInfilePacket, Packet, read_lenenc_int};
use queryable::Protocol;
use queryable::query_result::{self, QueryResult};
use queryable::stmt::InnerStmt;
use self::read_packet::ReadPacket;
use self::streamless::Streamless;
use self::write_packet::WritePacket;
use std::sync::Arc;
use conn::stmt_cache::StmtCache;
use tokio_io::io::read;

pub mod read_packet;
pub mod streamless {
    use io::Stream;
    use super::ConnectionLike;

    pub struct Streamless<T>(T);

    impl<T: ConnectionLike> Streamless<T> {
        pub fn new(x: T) -> Streamless<T> {
            Streamless(x)
        }

        pub fn return_stream(mut self, stream: Stream) -> T {
            self.0.return_stream(stream);
            self.0
        }
    }
}
pub mod write_packet;

#[derive(Debug)]
pub enum StmtCacheResult {
    Cached,
    NotCached(u32),
}

pub trait ConnectionLikeWrapper {
    type ConnLike: ConnectionLike;

    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized;
    fn return_stream(&mut self, stream: io::Stream) -> ();
    fn conn_like_ref(&self) -> &Self::ConnLike;

    fn conn_like_mut(&mut self) -> &mut Self::ConnLike;
}

impl<T, U> ConnectionLike for T
where
    T: ConnectionLikeWrapper<ConnLike = U>,
    U: ConnectionLike + 'static,
{
    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized,
    {
        <Self as ConnectionLikeWrapper>::take_stream(self)
    }

    fn return_stream(&mut self, stream: io::Stream) -> () {
        <Self as ConnectionLikeWrapper>::return_stream(self, stream)
    }

    fn stmt_cache_ref(&self) -> &StmtCache {
        self.conn_like_ref().stmt_cache_ref()
    }

    fn stmt_cache_mut(&mut self) -> &mut StmtCache {
        self.conn_like_mut().stmt_cache_mut()
    }

    fn get_affected_rows(&self) -> u64 {
        self.conn_like_ref().get_affected_rows()
    }

    fn get_capabilities(&self) -> CapabilityFlags {
        self.conn_like_ref().get_capabilities()
    }

    fn get_in_transaction(&self) -> bool {
        self.conn_like_ref().get_in_transaction()
    }

    fn get_last_command(&self) -> Command {
        self.conn_like_ref().get_last_command()
    }

    fn get_last_insert_id(&self) -> Option<u64> {
        self.conn_like_ref().get_last_insert_id()
    }

    fn get_local_infile_handler(&self) -> Option<Arc<LocalInfileHandler>> {
        self.conn_like_ref().get_local_infile_handler()
    }

    fn get_max_allowed_packet(&self) -> u64 {
        self.conn_like_ref().get_max_allowed_packet()
    }

    fn get_opts(&self) -> &Opts {
        self.conn_like_ref().get_opts()
    }

    fn get_pending_result(&self) -> Option<&(Arc<Vec<Column>>, Option<StmtCacheResult>)> {
        self.conn_like_ref().get_pending_result()
    }

    fn get_server_version(&self) -> (u16, u16, u16) {
        self.conn_like_ref().get_server_version()
    }

    fn get_status(&self) -> StatusFlags {
        self.conn_like_ref().get_status()
    }

    fn get_seq_id(&self) -> u8 {
        self.conn_like_ref().get_seq_id()
    }

    fn set_affected_rows(&mut self, affected_rows: u64) {
        self.conn_like_mut().set_affected_rows(affected_rows);
    }

    fn set_in_transaction(&mut self, in_transaction: bool) {
        self.conn_like_mut().set_in_transaction(in_transaction);
    }

    fn set_last_command(&mut self, last_command: Command) -> () {
        self.conn_like_mut().set_last_command(last_command);
    }

    fn set_last_insert_id(&mut self, last_insert_id: u64) -> () {
        self.conn_like_mut().set_last_insert_id(last_insert_id);
    }

    fn set_pending_result(&mut self, meta: Option<(Arc<Vec<Column>>, Option<StmtCacheResult>)>) {
        self.conn_like_mut().set_pending_result(meta);
    }

    fn set_status(&mut self, status: StatusFlags) -> () {
        self.conn_like_mut().set_status(status);
    }

    fn set_warnings(&mut self, warnings: u16) -> () {
        self.conn_like_mut().set_warnings(warnings);
    }

    fn set_seq_id(&mut self, seq_id: u8) -> () {
        self.conn_like_mut().set_seq_id(seq_id);
    }

    fn touch(&mut self) -> () {
        self.conn_like_mut().touch();
    }

    fn on_disconnect(&mut self) {
        self.conn_like_mut().on_disconnect();
    }
}

pub trait ConnectionLike {
    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized;
    fn return_stream(&mut self, stream: io::Stream) -> ();
    fn stmt_cache_ref(&self) -> &StmtCache;
    fn stmt_cache_mut(&mut self) -> &mut StmtCache;
    fn get_affected_rows(&self) -> u64;
    fn get_capabilities(&self) -> CapabilityFlags;
    fn get_in_transaction(&self) -> bool;
    fn get_last_command(&self) -> Command;
    fn get_last_insert_id(&self) -> Option<u64>;
    fn get_local_infile_handler(&self) -> Option<Arc<LocalInfileHandler>>; // TODO: Switch to Rc?
    fn get_max_allowed_packet(&self) -> u64;
    fn get_opts(&self) -> &Opts;
    fn get_pending_result(&self) -> Option<&(Arc<Vec<Column>>, Option<StmtCacheResult>)>;
    fn get_server_version(&self) -> (u16, u16, u16);
    fn get_status(&self) -> StatusFlags;
    fn get_seq_id(&self) -> u8;
    fn set_affected_rows(&mut self, affected_rows: u64);
    fn set_in_transaction(&mut self, in_transaction: bool);
    fn set_last_command(&mut self, last_command: Command);
    fn set_last_insert_id(&mut self, last_insert_id: u64);
    fn set_pending_result(&mut self, meta: Option<(Arc<Vec<Column>>, Option<StmtCacheResult>)>);
    fn set_status(&mut self, status: StatusFlags);
    fn set_warnings(&mut self, warnings: u16);
    fn set_seq_id(&mut self, seq_id: u8);
    fn touch(&mut self) -> ();
    fn on_disconnect(&mut self);

    fn cache_stmt(mut self, query: String, stmt: &InnerStmt) -> BoxFuture<(Self, StmtCacheResult)>
    where
        Self: Sized + 'static,
    {
        let fut = if self.get_opts().get_stmt_cache_size() > 0 {
            if let Some(old_stmt) = self.stmt_cache_mut().put(query, stmt.clone()) {
                A(self.close_stmt(old_stmt.statement_id).map(|this| {
                    (this, StmtCacheResult::Cached)
                }))
            } else {
                B(ok((self, StmtCacheResult::Cached)))
            }
        } else {
            B(ok((self, StmtCacheResult::NotCached(stmt.statement_id))))
        };
        Box::new(fut)
    }

    fn get_cached_stmt(&mut self, query: &String) -> Option<&InnerStmt> {
        self.stmt_cache_mut().get(query)
    }

    /// Returns future that reads packet from a server end resolves to `(Self, Packet)`.
    fn read_packet(self) -> ReadPacket<Self>
    where
        Self: Sized,
        Self: 'static,
    {
        ReadPacket::new(self)
    }

    /// Returns future that reads packets from a server and resolves to `(Self, Vec<Packet>)`.
    fn read_packets(self, n: usize) -> BoxFuture<(Self, Vec<Packet>)>
    where
        Self: Sized + 'static,
    {
        if n == 0 {
            return Box::new(ok((self, Vec::new())));
        }
        let fut = loop_fn((Vec::new(), n - 1, self), |(mut acc, num, conn_like)| {
            conn_like.read_packet().and_then(
                move |(conn_like, packet)| {
                    acc.push(packet);
                    if num == 0 {
                        Ok(Loop::Break((conn_like, acc)))
                    } else {
                        Ok(Loop::Continue((acc, num - 1, conn_like)))
                    }
                },
            )
        });
        Box::new(fut)
    }

    fn prepare_stmt<Q>(mut self, query: Q) -> BoxFuture<(Self, InnerStmt, StmtCacheResult)>
    where
        Q: AsRef<str>,
        Self: Sized + 'static,
    {
        match parse_named_params(query.as_ref()) {
            Ok((named_params, query)) => {
                let query = query.into_owned();
                if let Some(mut inner_stmt) = self.get_cached_stmt(&query).map(Clone::clone) {
                    inner_stmt.named_params = named_params.clone();
                    Box::new(ok((self, inner_stmt, StmtCacheResult::Cached)))
                } else {
                    let fut = self.write_command_data(Command::COM_STMT_PREPARE, &*query)
                        .and_then(|this| this.read_packet())
                        .and_then(|(this, packet)| {
                            InnerStmt::new(packet.as_ref(), named_params)
                                .into_future()
                                .map(|inner_stmt| (this, inner_stmt))
                        })
                        .and_then(|(this, mut inner_stmt)| {
                            this.read_packets(inner_stmt.num_params as usize)
                                .and_then(|(this, packets)| {
                                    let params = if packets.len() > 0 {
                                        let params = packets
                                            .into_iter()
                                            .map(|packet| Column::new(packet).map_err(Error::from))
                                            .collect::<Result<Vec<Column>>>();
                                        Some(params)
                                    } else {
                                        None
                                    };
                                    match params {
                                        Some(Err(error)) => return A(err(error)),
                                        Some(Ok(params)) => inner_stmt.params = Some(params),
                                        _ => (),
                                    }
                                    B(ok((this, inner_stmt)))
                                })
                                .and_then(|(this, inner_stmt)| if inner_stmt.num_params > 0 {
                                    if this
                                        .get_capabilities()
                                        .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
                                        A(ok((this, inner_stmt)))
                                    } else {
                                        B(this.read_packet().map(|(this, _)| (this, inner_stmt)))
                                    }
                                } else {
                                    A(ok((this, inner_stmt)))
                                })
                        })
                        .and_then(|(this, mut inner_stmt)| {
                            this.read_packets(inner_stmt.num_columns as usize)
                                .and_then(|(this, packets)| {
                                    let columns = if packets.len() > 0 {
                                        let columns = packets
                                            .into_iter()
                                            .map(|packet| Column::new(packet).map_err(Error::from))
                                            .collect::<Result<Vec<Column>>>();
                                        Some(columns)
                                    } else {
                                        None
                                    };
                                    match columns {
                                        Some(Err(error)) => return A(err(error)),
                                        Some(Ok(columns)) => inner_stmt.columns = Some(columns),
                                        _ => (),
                                    }
                                    B(ok((this, inner_stmt)))
                                })
                                .and_then(|(this, inner_stmt)| if inner_stmt.num_columns > 0 {
                                    if this
                                        .get_capabilities()
                                        .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
                                        A(ok((this, inner_stmt)))
                                    } else {
                                        B(this.read_packet().map(|(this, _)| (this, inner_stmt)))
                                    }
                                } else {
                                    A(ok((this, inner_stmt)))
                                })
                        })
                        .and_then(|(this, inner_stmt)| {
                            this.cache_stmt(query, &inner_stmt).map(|(this,
                              stmt_cache_result)| {
                                (this, inner_stmt, stmt_cache_result)
                            })
                        });
                    Box::new(fut)
                }
            }
            Err(err) => Box::new(Err(err).into_future()),
        }
    }

    fn close_stmt(self, statement_id: u32) -> WritePacket<Self>
    where
        Self: Sized + 'static,
    {
        let stmt_id = [
            (statement_id & 0xFF) as u8,
            (statement_id >> 8 & 0xFF) as u8,
            (statement_id >> 16 & 0xFF) as u8,
            (statement_id >> 24 & 0xFF) as u8,
        ];
        self.write_command_data(Command::COM_STMT_CLOSE, &stmt_id[..])
    }

    /// Returns future that reads result set from a server and resolves to `QueryResult`.
    fn read_result_set<P>(self, cached: Option<StmtCacheResult>) -> BoxFuture<QueryResult<Self, P>>
    where
        P: Protocol + 'static,
        Self: Sized + 'static,
    {
        let fut = self.read_packet().and_then(
            |(this, packet)| match packet.as_ref()
                [0] {
                0x00 => A(A(ok(query_result::new(this, None, cached)))),
                0xFB => {
                    let fut = LocalInfilePacket::new(&packet)
                        .ok_or(ErrorKind::UnexpectedPacket.into())
                        .and_then(|local_infile| match this.get_local_infile_handler() {
                            Some(handler) => handler.handle(local_infile.file_name()),
                            None => Err(ErrorKind::NoLocalInfileHandler.into()),
                        })
                        .into_future()
                        .and_then(|reader| {
                            let mut buf = Vec::with_capacity(4096);
                            unsafe {
                                buf.set_len(4096);
                            }
                            loop_fn((this, buf, reader), |(this, buf, reader)| {
                                read(reader, buf)
                                    .map_err(Into::into)
                                    .and_then(|(reader, mut buf, count)| {
                                        unsafe {
                                            buf.set_len(count);
                                        }
                                        (
                                            this.write_packet(&buf[..count]),
                                            ok(buf),
                                            ok(reader),
                                            ok(count),
                                        )
                                    })
                                    .map(|(this, buf, reader, count)| if count > 0 {
                                        Loop::Continue((this, buf, reader))
                                    } else {
                                        Loop::Break(this)
                                    })
                            }).and_then(|this| this.read_packet())
                                .map(|(this, _)| query_result::new(this, None, cached))
                        });
                    A(B(fut))
                }
                _ => {
                    let fut = read_lenenc_int(&mut packet.as_ref())
                        .into_future()
                        .and_then(|column_count| this.read_packets(column_count as usize))
                        .and_then(|(this, packets)| {
                            packets
                                .into_iter()
                                .map(|packet| Column::new(packet).map_err(Error::from))
                                .collect::<Result<Vec<Column>>>()
                                .into_future()
                                .and_then(|columns| if this.get_capabilities().contains(
                                    CapabilityFlags::CLIENT_DEPRECATE_EOF,
                                )
                                {
                                    A(ok((this, columns)))
                                } else {
                                    B(this.read_packet().map(|(this, _)| (this, columns)))
                                })
                        })
                        .map(|(mut this, columns)| {
                            let columns = Arc::new(columns);
                            this.set_pending_result(Some((Clone::clone(&columns), None)));
                            query_result::new(this, Some(columns), cached)
                        });
                    B(fut)
                }
            },
        );
        Box::new(fut)
    }

    /// Returns future that writes packet to a server end resolves to `Self`.
    fn write_packet<T>(self, data: T) -> WritePacket<Self>
    where
        T: Into<Vec<u8>>, // TODO: Switch to `AsRef<u8> + 'static`?
        Self: Sized + 'static,
    {
        WritePacket::new(self, data)
    }

    /// Returns future that writes command to a server end resolves to `Self`.
    fn write_command_data<T>(mut self, cmd: Command, cmd_data: T) -> WritePacket<Self>
    where
        Self: Sized + 'static,
        T: AsRef<[u8]>,
    {
        let mut data = Vec::with_capacity(1 + cmd_data.as_ref().len());
        data.push(cmd as u8);
        data.extend_from_slice(cmd_data.as_ref());
        self.set_seq_id(0);
        self.write_packet(data)
    }
}
