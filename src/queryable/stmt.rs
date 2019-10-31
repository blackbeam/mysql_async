// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bit_vec::BitVec;
use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use futures_util::future::Either;
use mysql_common::value::serialize_bin_many;

use std::io::Write;

use crate::{
    connection_like::{
        streamless::Streamless, ConnectionLike, ConnectionLikeWrapper, StmtCacheResult,
    },
    consts::{ColumnType, Command},
    error::*,
    io,
    prelude::FromRow,
    queryable::{query_result::QueryResult, BinaryProtocol},
    Column, Params, Row,
    Value::{self, *},
};

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
    pub fn new(payload: &[u8], named_params: Option<Vec<String>>) -> Result<InnerStmt> {
        let packet: mysql_common::packets::StmtPacket =
            mysql_common::packets::parse_stmt_packet(payload)?;

        Ok(InnerStmt {
            named_params,
            statement_id: packet.statement_id(),
            num_columns: packet.num_columns(),
            num_params: packet.num_params(),
            warning_count: packet.warning_count(),
            params: None,
            columns: None,
        })
    }
}

/// Prepared statement
pub struct Stmt<T> {
    conn_like: Option<Either<T, Streamless<T>>>,
    inner: InnerStmt,
    /// None => In use elsewhere
    /// Some(Cached) => Should not be closed
    /// Some(NotCached(_)) => Should be closed
    cached: Option<StmtCacheResult>,
}

pub fn new<T>(conn_like: T, inner: InnerStmt, cached: StmtCacheResult) -> Stmt<T>
where
    T: ConnectionLike + Sized + 'static,
{
    Stmt::new(conn_like, inner, cached)
}

impl<T> Stmt<T>
where
    T: ConnectionLike + Sized + 'static,
{
    fn new(conn_like: T, inner: InnerStmt, cached: StmtCacheResult) -> Stmt<T> {
        Stmt {
            conn_like: Some(Either::Left(conn_like)),
            inner,
            cached: Some(cached),
        }
    }

    async fn send_long_data_for_index(
        self,
        params: Vec<Value>,
        index: usize,
    ) -> Result<(Self, Vec<Value>)> {
        let mut this = self;
        for chunk in 0.. {
            let data_cap = crate::consts::MAX_PAYLOAD_LEN - 10;
            let buf = match params[index] {
                Bytes(ref x) => {
                    let statement_id = this.inner.statement_id;
                    let mut chunks = x.chunks(data_cap);
                    match chunks.nth(chunk) {
                        Some(chunk) => {
                            let mut buf = Vec::with_capacity(chunk.len() + 6);
                            buf.write_u32::<LE>(statement_id).unwrap();
                            buf.write_u16::<LE>(index as u16).unwrap();
                            buf.write_all(chunk).unwrap();
                            Some(buf)
                        }
                        _ => None,
                    }
                }
                _ => unreachable!(),
            };
            match buf {
                Some(buf) => {
                    let chunk_len = buf.len() - 6;
                    this = this
                        .write_command_data(Command::COM_STMT_SEND_LONG_DATA, buf)
                        .await?;
                    if chunk_len < data_cap {
                        break;
                    }
                }
                None => break,
            }
        }
        Ok((this, params))
    }

    async fn send_long_data(
        self,
        mut params: Vec<Value>,
        large_bitmap: BitVec<u8>,
    ) -> Result<(Self, Vec<Value>)> {
        let mut bits = large_bitmap.into_iter().enumerate();
        let mut this = self;
        loop {
            match bits.next() {
                Some((index, true)) => {
                    let (this_, params_) = this.send_long_data_for_index(params, index).await?;
                    this = this_;
                    params = params_;
                }
                Some((_, false)) => {}
                None => return Ok((this, params)),
            }
        }
    }

    async fn execute_positional<U>(self, params: U) -> Result<QueryResult<Self, BinaryProtocol>>
    where
        U: ::std::ops::Deref<Target = [Value]>,
        U: IntoIterator<Item = Value>,
        U: Send + 'static,
    {
        if self.inner.num_params as usize != params.len() {
            let error = DriverError::StmtParamsMismatch {
                required: self.inner.num_params,
                supplied: params.len() as u16,
            }
            .into();
            return Err(error);
        }

        let params_def = self.inner.params.as_ref().unwrap();
        let bin_payload = serialize_bin_many(&*params_def, &*params)?;
        let (row_data, null_bitmap, large_bitmap) = bin_payload;
        let (this, params) = self
            .send_long_data(params.into_iter().collect(), large_bitmap.clone())
            .await?;

        let mut data = Vec::new();
        write_data(
            &mut data,
            this.inner.statement_id,
            row_data,
            params,
            this.inner.params.as_ref().unwrap(),
            null_bitmap,
        );
        let this = this
            .write_command_data(Command::COM_STMT_EXECUTE, data)
            .await?;
        let this = this.read_result_set(None).await?;
        Ok(this)
    }

    async fn execute_named(self, params: Params) -> Result<QueryResult<Self, BinaryProtocol>> {
        if self.inner.named_params.is_none() {
            let error = DriverError::NamedParamsForPositionalQuery.into();
            return Err(error);
        }

        let positional_params =
            match params.into_positional(self.inner.named_params.as_ref().unwrap()) {
                Ok(positional_params) => positional_params,
                Err(error) => return Err(error.into()),
            };

        match positional_params {
            Params::Positional(params) => self.execute_positional(params).await,
            _ => unreachable!(),
        }
    }

    async fn execute_empty(self) -> Result<QueryResult<Self, BinaryProtocol>> {
        if self.inner.num_params > 0 {
            let error = DriverError::StmtParamsMismatch {
                required: self.inner.num_params,
                supplied: 0,
            }
            .into();
            return Err(error);
        }

        let mut data = Vec::with_capacity(4 + 1 + 4);
        data.write_u32::<LE>(self.inner.statement_id).unwrap();
        data.write_u8(0u8).unwrap();
        data.write_u32::<LE>(1u32).unwrap();

        let this = self
            .write_command_data(Command::COM_STMT_EXECUTE, data)
            .await?;
        this.read_result_set(None).await
    }

    /// See `Queryable::execute`
    pub async fn execute<P>(self, params: P) -> Result<QueryResult<Self, BinaryProtocol>>
    where
        P: Into<Params>,
    {
        let params = params.into();
        match params {
            Params::Positional(params) => self.execute_positional(params).await,
            Params::Named(_) => self.execute_named(params).await,
            Params::Empty => self.execute_empty().await,
        }
    }

    /// See `Queryable::first`
    pub async fn first<P, R>(self, params: P) -> Result<(Self, Option<R>)>
    where
        P: Into<Params> + 'static,
        R: FromRow,
    {
        let result = self.execute(params).await?;
        let (this, mut rows) = result.collect_and_drop::<Row>().await?;
        if rows.len() > 1 {
            Ok((this, Some(FromRow::from_row(rows.swap_remove(0)))))
        } else {
            Ok((this, rows.pop().map(FromRow::from_row)))
        }
    }

    /// See `Queryable::batch`
    pub async fn batch<I, P>(self, params_iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = P>,
        I::IntoIter: Send + 'static,
        Params: From<P>,
        P: 'static,
    {
        let mut params_iter = params_iter.into_iter().map(Params::from);
        let mut this = self;
        loop {
            match params_iter.next() {
                Some(params) => {
                    this = this.execute(params).await?.drop_result().await?;
                }
                None => break Ok(this),
            }
        }
    }

    /// This will close statement (if it's not in the cache) and resolve to a wrapped queryable.
    pub async fn close(mut self) -> Result<T> {
        let cached = self.cached.take();
        match self.conn_like {
            Some(Either::Left(conn_like)) => {
                if let Some(StmtCacheResult::NotCached(stmt_id)) = cached {
                    conn_like.close_stmt(stmt_id).await
                } else {
                    Ok(conn_like)
                }
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn unwrap(mut self) -> (T, Option<StmtCacheResult>) {
        match self.conn_like {
            Some(Either::Left(conn_like)) => (conn_like, self.cached.take()),
            _ => unreachable!(),
        }
    }
}

impl<T: ConnectionLike + 'static> ConnectionLikeWrapper for Stmt<T> {
    type ConnLike = T;

    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized,
    {
        let Stmt {
            conn_like,
            inner,
            cached,
        } = self;
        match conn_like {
            Some(Either::Left(conn_like)) => {
                let (streamless, stream) = conn_like.take_stream();
                let this = Stmt {
                    conn_like: Some(Either::Right(streamless)),
                    inner,
                    cached,
                };
                (Streamless::new(this), stream)
            }
            _ => unreachable!(),
        }
    }

    fn return_stream(&mut self, stream: io::Stream) {
        let conn_like = self.conn_like.take().unwrap();
        match conn_like {
            Either::Right(streamless) => {
                self.conn_like = Some(Either::Left(streamless.return_stream(stream)));
            }
            _ => unreachable!(),
        }
    }

    fn conn_like_ref(&self) -> &Self::ConnLike {
        match self.conn_like {
            Some(Either::Left(ref conn_like)) => conn_like,
            _ => unreachable!(),
        }
    }

    fn conn_like_mut(&mut self) -> &mut Self::ConnLike {
        match self.conn_like {
            Some(Either::Left(ref mut conn_like)) => conn_like,
            _ => unreachable!(),
        }
    }
}

fn write_data(
    writer: &mut Vec<u8>,
    stmt_id: u32,
    row_data: Vec<u8>,
    params: Vec<Value>,
    params_def: &[Column],
    null_bitmap: BitVec<u8>,
) {
    let capacity = 9 + null_bitmap.storage().len() + 1 + params.len() * 2 + row_data.len();
    writer.reserve(capacity);
    writer.write_u32::<LE>(stmt_id).unwrap();
    writer.write_u8(0u8).unwrap();
    writer.write_u32::<LE>(1u32).unwrap();
    writer.write_all(null_bitmap.storage().as_ref()).unwrap();
    writer.write_u8(1u8).unwrap();
    for i in 0..params.len() {
        let result = match params[i] {
            NULL => writer.write_all(&[params_def[i].column_type() as u8, 0u8]),
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
