// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use futures_util::future::Either;

use crate::{
    connection_like::{
        streamless::Streamless, ConnectionLike, ConnectionLikeWrapper, StmtCacheResult,
    },
    consts::Command,
    error::*,
    io,
    prelude::FromRow,
    queryable::{query_result::QueryResult, BinaryProtocol},
    Column, Params, Row,
    Value::{self},
};
use mysql_common::constants::MAX_PAYLOAD_LEN;
use mysql_common::packets::{ComStmtExecuteRequestBuilder, ComStmtSendLongData};

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
            named_params,
            statement_id,
            num_columns,
            num_params,
            warning_count,
            params: None,
            columns: None,
        })
    }
}

/// Prepared statement
#[derive(Debug)]
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

    async fn send_long_data(self, params: Vec<Value>) -> Result<Self> {
        let mut this = self;

        for (i, value) in params.into_iter().enumerate() {
            if let Value::Bytes(bytes) = value {
                let chunks = bytes.chunks(MAX_PAYLOAD_LEN - 6);
                let chunks = chunks.chain(if bytes.is_empty() {
                    Some(&[][..])
                } else {
                    None
                });
                for chunk in chunks {
                    let com = ComStmtSendLongData::new(this.inner.statement_id, i, chunk);
                    this = this.write_command_raw(com.into()).await?;
                }
            }
        }

        Ok(this)
    }

    async fn execute_positional<U>(self, params: U) -> Result<QueryResult<Self, BinaryProtocol>>
    where
        U: ::std::ops::Deref<Target = [Value]>,
        U: IntoIterator<Item = Value>,
        U: Send + 'static,
    {
        if self.inner.num_params as usize != params.len() {
            Err(DriverError::StmtParamsMismatch {
                required: self.inner.num_params,
                supplied: params.len() as u16,
            })?
        }

        let params = params.into_iter().collect::<Vec<_>>();

        let (body, as_long_data) =
            ComStmtExecuteRequestBuilder::new(self.inner.statement_id).build(&*params);

        let this = if as_long_data {
            self.send_long_data(params).await?
        } else {
            self
        };

        this.write_command_raw(body).await?.read_result_set(None).await
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
