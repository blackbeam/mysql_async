// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use BoxFuture;
use Opts;
use byteorder::LittleEndian as LE;
use byteorder::{ReadBytesExt, WriteBytesExt};
use conn::stmt_cache::StmtCache;
use connection_like::{ConnectionLike, StmtCacheResult};
use connection_like::streamless::Streamless;
use consts::{CapabilityFlags, ColumnType, Command, StatusFlags};
use errors::*;
use io;
use lib_futures::future::{Either, Future, IntoFuture, Loop, err, loop_fn, ok};
use lib_futures::future::Either::*;
use local_infile_handler::LocalInfileHandler;
use prelude::FromRow;
use proto::{Column, OkPacket, Row};
use queryable::BinaryProtocol;
use queryable::query_result::QueryResult;
use std::io::Write;
use std::sync::Arc;
use value::{Params, Value};
use value::Value::*;

/// Inner statement representation.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct InnerStmt {
    /// Positions and names of named parameters
    pub named_params: Option<Vec<String>>,
    pub params: Option<Vec<Column>>,
    pub columns: Option<Vec<Column>>,
    pub statement_id: u32,
    pub num_columns: u16,
    pub num_params: u16,
    pub warning_count: u16,
}

impl InnerStmt {
    // TODO: Consume payload?
    pub fn new(pld: &[u8], named_params: Option<Vec<String>>) -> Result<InnerStmt> {
        let mut reader = &pld[1..];
        let statement_id = reader.read_u32::<LE>()?;
        let num_columns = reader.read_u16::<LE>()?;
        let num_params = reader.read_u16::<LE>()?;
        let warning_count = reader.read_u16::<LE>()?;
        Ok(InnerStmt {
            named_params: named_params,
            statement_id: statement_id,
            num_columns: num_columns,
            num_params: num_params,
            warning_count: warning_count,
            params: None,
            columns: None,
        })
    }
}

pub struct Stmt<T> {
    conn_like: Option<Either<T, Streamless<T>>>,
    inner: InnerStmt,
    /// None => In use elsewhere
    /// Some(Cached) => Should not be closed
    /// Some(NotCached(_)) => Should be closed
    cached: Option<StmtCacheResult>,
}

pub fn new<T>(conn_like: T, inner: InnerStmt, cached: StmtCacheResult) -> Stmt<T>
    where T: ConnectionLike + Sized + 'static
{
    Stmt::new(conn_like, inner, cached)
}

impl<T> Stmt<T>
    where T: ConnectionLike + Sized + 'static
{
    fn new(conn_like: T, inner: InnerStmt, cached: StmtCacheResult) -> Stmt<T> {
        Stmt {
            conn_like: Some(A(conn_like)),
            inner,
            cached: Some(cached),
        }
    }

    fn conn_like_ref(&self) -> &T {
        match self.conn_like {
            Some(A(ref conn_like)) => {
                conn_like
            },
            _ => unreachable!(),
        }
    }

    fn conn_like_mut(&mut self) -> &mut T {
        match self.conn_like {
            Some(A(ref mut conn_like)) => {
                conn_like
            },
            _ => unreachable!(),
        }
    }

    fn send_long_data(self,
                      params: Vec<Value>,
                      large_ids: Vec<u16>)
                      -> BoxFuture<(Self, Vec<Value>)>
    {
        let fut = loop_fn(
            (self, params, large_ids, 0),
            |(this, params, mut large_ids, next_chunk)| {
                let data_cap = this.get_max_allowed_packet() as usize - 8;
                match large_ids.pop() {
                    Some(id) => {
                        let buf = match params[id as usize] {
                            Bytes(ref x) => {
                                let statement_id = this.inner.statement_id;
                                let mut chunks = x.chunks(data_cap);
                                match chunks.nth(next_chunk) {
                                    Some(chunk) => {
                                        let mut buf = Vec::with_capacity(chunk.len() + 7);
                                        buf.write_u32::<LE>(statement_id).unwrap();
                                        buf.write_u16::<LE>(id).unwrap();
                                        buf.write_all(chunk).unwrap();
                                        Some(buf)
                                    },
                                    None => None,
                                }
                            },
                            _ => unreachable!(),
                        };
                        match buf {
                            Some(buf) => {
                                let chunk_len = buf.len() - 7;
                                let fut = this
                                    .write_command_data(Command::COM_STMT_SEND_LONG_DATA, buf)
                                    .map(move |this| {
                                        let next = if chunk_len < data_cap {
                                            (this, params, large_ids, 0)
                                        } else {
                                            large_ids.push(id);
                                            (this, params, large_ids, next_chunk + 1)
                                        };
                                        Loop::Continue(next)
                                    });
                                A(A(fut))
                            },
                            None => A(B(ok(Loop::Continue((this, params, large_ids, 0)))))
                        }
                    },
                    None => B(ok(Loop::Break((this, params)))),
                }
            });
        Box::new(fut)
    }

    /// See `Queriable::execute`
    pub fn execute<P>(self, params: P) -> BoxFuture<QueryResult<Self, BinaryProtocol>>
        where P: Into<Params>
    {
        let params = params.into();
        let fut = match params {
            Params::Positional(params) => {
                if self.inner.num_params as usize != params.len() {
                    let error = ErrorKind::MismatchedStmtParams(self.inner.num_params,
                                                                params.len() as u16).into();
                    return Box::new(err(error));
                }

                let fut = self.inner.params.as_ref().ok_or_else(|| unreachable!())
                    .and_then(|params_def| {
                        Value::to_bin_payload(&*params_def,
                                              &*params,
                                              self.get_max_allowed_packet() as usize)
                    })
                    .into_future()
                    .and_then(|bin_payload| match bin_payload {
                        (bitmap, row_data, Some(large_ids)) => {
                            let fut = self.send_long_data(params, large_ids.clone())
                                .and_then(|(this, params)| {
                                    let mut data = Vec::new();
                                    write_data(&mut data,
                                               this.inner.statement_id,
                                               bitmap,
                                               row_data,
                                               params,
                                               this.inner.params.as_ref().unwrap(),
                                               large_ids);
                                    this.write_command_data(Command::COM_STMT_EXECUTE, data)
                                });
                            A(fut)
                        },
                        (bitmap, row_data, None) => {
                            let mut data = Vec::new();
                            write_data(&mut data,
                                       self.inner.statement_id,
                                       bitmap,
                                       row_data,
                                       params,
                                       self.inner.params.as_ref().unwrap(),
                                       Vec::new());
                            B(self.write_command_data(Command::COM_STMT_EXECUTE, data))
                        },
                    });
                A(fut)
            },
            Params::Named(_) => {
                if self.inner.named_params.is_none() {
                    let error = ErrorKind::NamedParamsForPositionalQuery.into();
                    return Box::new(err(error));
                }

                let positional_params =
                    match params.into_positional(self.inner.named_params.as_ref().unwrap()) {
                        Ok(positional_params) => positional_params,
                        Err(error) => {
                            return Box::new(err(error));
                        }
                    };
                return self.execute(positional_params);
            },
            Params::Empty => {
                if self.inner.num_params > 0 {
                    let error = ErrorKind::MismatchedStmtParams(self.inner.num_params, 0).into();
                    return Box::new(err(error));
                }

                let mut data = Vec::with_capacity(4 + 1 + 4);
                data.write_u32::<LE>(self.inner.statement_id).unwrap();
                data.write_u8(0u8).unwrap();
                data.write_u32::<LE>(1u32).unwrap();

                B(self.write_command_data(Command::COM_STMT_EXECUTE, data))
            }
        };
        Box::new(fut.and_then(|this| this.read_result_set(None)))
    }

    /// See `Queriable::first`
    pub fn first<P, R>(self, params: P) -> BoxFuture<(Self, Option<R>)>
        where P: Into<Params>,
              R: FromRow
    {
        let fut = self.execute(params)
            .and_then(|result| result.collect_and_drop::<Row>())
            .map(|(this, mut rows)| if rows.len() > 1 {
                (this, Some(FromRow::from_row(rows.swap_remove(0))))
            } else {
                (this, rows.pop().map(FromRow::from_row))
            });
        Box::new(fut)
    }

    /// See `Queriable::batch`
    pub fn batch<I, P>(self, params_iter: I) -> BoxFuture<Self>
        where I: IntoIterator<Item=P> + 'static,
              Params: From<P>,
              P: 'static
    {
        let params_iter = params_iter.into_iter().map(Params::from);
        let fut = loop_fn((self, params_iter), |(this, mut params_iter)| {
            match params_iter.next() {
                Some(params) => {
                    let fut = this.execute(params)
                        .and_then(|result| result.drop_result())
                        .map(|this| Loop::Continue((this, params_iter)));
                    A(fut)
                }
                None => B(ok(Loop::Break(this)))
            }
        });
        Box::new(fut)
    }

    /// This will close statement (it it's not in the cache) and resolve to a wrapped queryable.
    pub fn close(mut self) -> BoxFuture<T> {
        let cached = self.cached.take();
        let fut = match self.conn_like {
            Some(A(conn_like)) => {
                if let Some(StmtCacheResult::NotCached(stmt_id)) = cached {
                    A(conn_like.close_stmt(stmt_id))
                } else {
                    B(ok(conn_like))
                }
            },
            _ => unreachable!(),
        };
        Box::new(fut)
    }

    pub (crate) fn unwrap(mut self) -> (T, Option<StmtCacheResult>) {
        match self.conn_like {
            Some(A(conn_like)) => (conn_like, self.cached.take()),
            _ => unreachable!(),
        }
    }
}

impl<T: ConnectionLike + 'static> ConnectionLike for Stmt<T> {
    fn take_stream(self) -> (Streamless<Self>, io::Stream) where Self: Sized {
        let Stmt { conn_like, inner, cached } = self;
        match conn_like {
            Some(A(conn_like)) => {
                let (streamless, stream) = conn_like.take_stream();
                let this = Stmt {
                    conn_like: Some(B(streamless)),
                    inner,
                    cached,
                };
                (Streamless::new(this), stream)
            },
            _ => unreachable!(),
        }
    }

    fn return_stream(&mut self, stream: io::Stream) -> () {
        let conn_like = self.conn_like.take().unwrap();
        match conn_like {
            B(streamless) => {
                self.conn_like = Some(A(streamless.return_stream(stream)));
            },
            _ => unreachable!(),
        }
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

    fn get_pending_result(&self) -> Option<&(Arc<Vec<Column>>, Option<OkPacket>, Option<StmtCacheResult>)> {
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

    fn set_pending_result(&mut self, meta: Option<(Arc<Vec<Column>>, Option<OkPacket>, Option<StmtCacheResult>)>) {
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

fn write_data(writer: &mut Vec<u8>,
              stmt_id: u32,
              bitmap: Vec<u8>,
              row_data: Vec<u8>,
              params: Vec<Value>,
              params_def: &Vec<Column>,
              large_ids: Vec<u16>) {
    let capacity = 9 + bitmap.len() + 1 + params.len() * 2 + row_data.len();
    writer.reserve(capacity);
    writer.write_u32::<LE>(stmt_id).unwrap();
    writer.write_u8(0u8).unwrap();
    writer.write_u32::<LE>(1u32).unwrap();
    writer.write_all(bitmap.as_ref()).unwrap();
    writer.write_u8(1u8).unwrap();
    for i in 0..params.len() {
        if large_ids.contains(&(i as u16)) {
            continue;
        }
        let result = match params[i] {
            NULL => writer.write_all(&[params_def[i].column_type as u8, 0u8]),
            Bytes(..) => writer.write_all(&[ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0u8]),
            Int(..) => writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8, 0u8]),
            UInt(..) => writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8, 128u8]),
            Float(..) => writer.write_all(&[ColumnType::MYSQL_TYPE_DOUBLE as u8, 0u8]),
            Date(..) => writer.write_all(&[ColumnType::MYSQL_TYPE_DATETIME as u8, 0u8]),
            Time(..) => writer.write_all(&[ColumnType::MYSQL_TYPE_TIME as u8, 0u8]),
        };
        result.unwrap();
    }
    writer.write_all(row_data.as_ref()).unwrap();
}
