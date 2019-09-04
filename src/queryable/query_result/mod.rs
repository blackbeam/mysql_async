// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_util::future::Either;
use mysql_common::packets::RawPacket;
use mysql_common::row::convert::FromRowError;

use std::marker::PhantomData;
use std::result::Result as StdResult;
use std::sync::Arc;

use self::QueryResultInner::*;
use crate::{
    connection_like::{
        streamless::Streamless, ConnectionLike, ConnectionLikeWrapper, StmtCacheResult,
    },
    consts::StatusFlags,
    error::*,
    io,
    prelude::FromRow,
    queryable::Protocol,
    Column, MyFuture, Row,
};

pub fn new<T, P>(
    conn_like: T,
    columns: Option<Arc<Vec<Column>>>,
    cached: Option<StmtCacheResult>,
) -> QueryResult<T, P>
where
    T: ConnectionLike + Sized + 'static,
    P: Protocol,
    P: Send + 'static,
{
    QueryResult::new(conn_like, columns, cached)
}

pub fn disassemble<T, P>(
    query_result: QueryResult<T, P>,
) -> (T, Option<Arc<Vec<Column>>>, Option<StmtCacheResult>) {
    match query_result {
        QueryResult(Empty(Some(Either::Left(conn_like)), cached, _)) => (conn_like, None, cached),
        QueryResult(WithRows(Some(Either::Left(conn_like)), columns, cached, _)) => {
            (conn_like, Some(columns), cached)
        }
        _ => unreachable!(),
    }
}

pub fn assemble<T, P>(
    conn_like: T,
    columns: Option<Arc<Vec<Column>>>,
    cached: Option<StmtCacheResult>,
) -> QueryResult<T, P>
where
    T: ConnectionLike + Sized + 'static,
    P: Protocol + 'static,
{
    match columns {
        Some(columns) => QueryResult(WithRows(
            Some(Either::Left(conn_like)),
            columns,
            cached,
            PhantomData,
        )),
        None => QueryResult(Empty(Some(Either::Left(conn_like)), cached, PhantomData)),
    }
}

enum QueryResultInner<T, P> {
    Empty(
        Option<Either<T, Streamless<T>>>,
        Option<StmtCacheResult>,
        PhantomData<P>,
    ),
    WithRows(
        Option<Either<T, Streamless<T>>>,
        Arc<Vec<Column>>,
        Option<StmtCacheResult>,
        PhantomData<P>,
    ),
}

/// Result of a query or statement execution.
pub struct QueryResult<T, P>(QueryResultInner<T, P>);

impl<T, P> QueryResult<T, P>
where
    P: Protocol,
    P: Send + 'static,
    T: ConnectionLike,
    T: Sized + Send + 'static,
{
    fn into_empty(mut self) -> Self {
        self.set_pending_result(None);
        match self {
            QueryResult(WithRows(conn_like, _, cached, _)) => {
                QueryResult(Empty(conn_like, cached, PhantomData))
            }
            x => x,
        }
    }

    fn into_inner(self) -> (T, Option<StmtCacheResult>) {
        match self {
            QueryResult(Empty(conn_like, cached, _))
            | QueryResult(WithRows(conn_like, _, cached, _)) => match conn_like {
                Some(Either::Left(conn_like)) => (conn_like, cached),
                _ => unreachable!(),
            },
        }
    }

    async fn get_row_raw(self) -> Result<(Self, Option<RawPacket>)> {
        if self.is_empty() {
            return Ok((self, None));
        }

        let (this, packet) = self.read_packet().await?;
        if P::is_last_result_set_packet(&this, &packet) {
            if this
                .get_status()
                .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
            {
                let (inner, cached) = this.into_inner();
                let this = inner.read_result_set(cached).await?;
                Ok((this, None))
            } else {
                Ok((this.into_empty(), None))
            }
        } else {
            Ok((this, Some(packet)))
        }
    }

    async fn get_row(self) -> Result<(Self, Option<Row>)> {
        let (this, packet) = self.get_row_raw().await?;
        if let Some(packet) = packet {
            if let QueryResult(WithRows(_, ref columns, ..)) = this {
                let row = P::read_result_set_row(&packet, columns.clone())?;
                Ok((this, Some(row)))
            } else {
                unreachable!()
            }
        } else {
            Ok((this, None))
        }
    }

    fn new(
        conn_like: T,
        columns: Option<Arc<Vec<Column>>>,
        cached: Option<StmtCacheResult>,
    ) -> QueryResult<T, P> {
        match columns {
            Some(columns) => QueryResult(WithRows(
                Some(Either::Left(conn_like)),
                columns,
                cached,
                PhantomData,
            )),
            None => QueryResult(Empty(Some(Either::Left(conn_like)), cached, PhantomData)),
        }
    }

    /// Last insert id (if not 0).
    pub fn last_insert_id(&self) -> Option<u64> {
        self.get_last_insert_id()
    }

    /// Value of `affected_rows` returned from a server.
    pub fn affected_rows(&self) -> u64 {
        self.get_affected_rows()
    }

    /// `true` if there is no more rows nor result sets in this query.
    ///
    /// One could use it to check if there is more than one result set in this query result.
    pub fn is_empty(&self) -> bool {
        match *self {
            QueryResult(Empty(..)) => !self.more_results_exists(),
            _ => false,
        }
    }

    /// Returns `true` if the SERVER_MORE_RESULTS_EXISTS flag is contained in status flags
    /// of the connection.
    fn more_results_exists(&self) -> bool {
        self.get_status()
            .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
    }

    /// `true` if rows may exists for this query result.
    ///
    /// If `false` then there is no rows possible (for example UPDATE query).
    fn has_rows(&self) -> bool {
        match *self {
            QueryResult(Empty(..)) => false,
            _ => true,
        }
    }

    /// Returns future that collects result set of this query result.
    ///
    /// It is parametrized by `R` and internally calls `R::from_row(Row)` on each row.
    ///
    /// It will stop collecting on result set boundary. This means that you should call `collect`
    /// as many times as result sets in your query result. For example query
    /// `SELECT 'foo'; SELECT 'foo', 'bar';` will produce `QueryResult` with two result sets in it.
    /// One can use `QueryResult::is_empty` to make sure that there is no more result sets.
    ///
    /// # Panic
    ///
    /// It'll panic if any row isn't convertible to `R` (i.e. programmer error or unknown schema).
    /// * In case of programmer error see `FromRow` docs;
    /// * In case of unknown schema use [`QueryResult::try_collect`].
    pub fn collect<R>(self) -> impl MyFuture<(Self, Vec<R>)>
    where
        R: FromRow,
        R: Send + 'static,
    {
        self.reduce(Vec::new(), |mut acc, row| {
            acc.push(FromRow::from_row(row));
            acc
        })
    }

    /// Returns future that collects result set of this query.
    ///
    /// It works the same way as [`QueryResult::collect`] but won't panic
    /// if row isn't convertible to `R`.
    pub fn try_collect<R>(self) -> impl MyFuture<(Self, Vec<StdResult<R, FromRowError>>)>
    where
        R: FromRow,
        R: Send + 'static,
    {
        self.reduce(Vec::new(), |mut acc, row| {
            acc.push(FromRow::from_row_opt(row));
            acc
        })
    }

    /// Returns future that collects result set of a query result and drops everything else.
    /// It will resolve to a pair of wrapped `Queryable` and collected result set.
    ///
    /// # Panic
    ///
    /// It'll panic if any row isn't convertible to `R` (i.e. programmer error or unknown schema).
    /// * In case of programmer error see `FromRow` docs;
    /// * In case of unknown schema use [`QueryResult::try_collect`].
    pub async fn collect_and_drop<R>(self) -> Result<(T, Vec<R>)>
    where
        R: FromRow,
        R: Send + 'static,
    {
        let (this, output) = self.collect().await?;
        let conn = this.drop_result().await?;
        Ok((conn, output))
    }

    /// Returns future that collects result set of a query result and drops everything else.
    /// It will resolve to a pair of wrapped `Queryable` and collected result set.
    ///
    /// It works the same way as [`QueryResult::collect_and_drop`] but won't panic
    /// if row isn't convertible to `R`.
    pub async fn try_collect_and_drop<R>(self) -> Result<(T, Vec<StdResult<R, FromRowError>>)>
    where
        R: FromRow,
        R: Send + 'static,
    {
        let (this, output) = self.try_collect().await?;
        let conn = this.drop_result().await?;
        Ok((conn, output))
    }

    /// Returns future that will execute `fun` on every row of current result set.
    ///
    /// It will stop on result set boundary (see `QueryResult::collect` docs).
    pub async fn for_each<F>(self, mut fun: F) -> Result<Self>
    where
        F: FnMut(Row),
    {
        if self.is_empty() {
            Ok(self)
        } else {
            let mut qr = self;
            loop {
                let (qr_, row) = qr.get_row().await?;
                qr = qr_;
                if let Some(row) = row {
                    fun(row);
                } else {
                    break Ok(qr);
                }
            }
        }
    }

    /// Returns future that will execute `fun` on every row of current result set and drop
    /// everything else. It will resolve to a wrapped `Queryable`.
    pub async fn for_each_and_drop<F>(self, fun: F) -> Result<T>
    where
        F: FnMut(Row),
    {
        self.for_each(fun).await?.drop_result().await
    }

    /// Returns future that will map every row of current result set to `U` using `fun`.
    ///
    /// It will stop on result set boundary (see `QueryResult::collect` docs).
    pub async fn map<F, U>(self, mut fun: F) -> Result<(Self, Vec<U>)>
    where
        F: FnMut(Row) -> U,
    {
        if self.is_empty() {
            Ok((self, Vec::new()))
        } else {
            let mut qr = self;
            let mut rows = Vec::new();
            loop {
                let (qr_, row) = qr.get_row().await?;
                qr = qr_;
                if let Some(row) = row {
                    rows.push(fun(row));
                } else {
                    break Ok((qr, rows));
                }
            }
        }
    }

    /// Returns future that will map every row of current result set to `U` using `fun` and drop
    /// everything else. It will resolve to a pair of wrapped `Queryable` and mapped result set.
    pub async fn map_and_drop<F, U>(self, fun: F) -> Result<(T, Vec<U>)>
    where
        F: FnMut(Row) -> U,
    {
        let (this, rows) = self.map(fun).await?;
        let this = this.drop_result().await?;
        Ok((this, rows))
    }

    /// Returns future that will reduce rows of current result set to `U` using `fun`.
    ///
    /// It will stop on result set boundary (see `QueryResult::collect` docs).
    pub async fn reduce<F, U>(self, init: U, mut fun: F) -> Result<(Self, U)>
    where
        F: FnMut(U, Row) -> U,
    {
        if self.is_empty() {
            Ok((self, init))
        } else {
            let mut qr = self;
            let mut acc = init;
            loop {
                let (qr_, row) = qr.get_row().await?;
                qr = qr_;
                if let Some(row) = row {
                    acc = fun(acc, row);
                } else {
                    break Ok((qr, acc));
                }
            }
        }
    }

    /// Returns future that will reduce rows of current result set to `U` using `fun` and drop
    /// everything else. It will resolve to a pair of wrapped `Queryable` and `U`.
    pub async fn reduce_and_drop<F, U>(self, init: U, fun: F) -> Result<(T, U)>
    where
        F: FnMut(U, Row) -> U,
    {
        let (this, acc) = self.reduce(init, fun).await?;
        let this = this.drop_result().await?;
        Ok((this, acc))
    }

    /// Returns future that will drop this query result end resolve to a wrapped `Queryable`.
    pub async fn drop_result(self) -> Result<T> {
        let mut this = self;
        let (conn_like, cached) = loop {
            if !this.has_rows() {
                if this.more_results_exists() {
                    let (inner, cached) = this.into_inner();
                    this = inner.read_result_set(cached).await?;
                } else {
                    break this.into_inner();
                }
            } else {
                let (this_, _) = this.get_row_raw().await?;
                this = this_;
            }
        };

        if let Some(StmtCacheResult::NotCached(statement_id)) = cached {
            conn_like.close_stmt(statement_id).await
        } else {
            Ok(conn_like)
        }
    }

    /// Returns reference to columns in this query result.
    pub fn columns_ref(&self) -> &[Column] {
        match self.0 {
            QueryResultInner::Empty(..) => {
                static EMPTY: &'static [Column] = &[];
                EMPTY
            }
            QueryResultInner::WithRows(_, ref columns, ..) => &**columns,
        }
    }

    /// Returns copy of columns of this query result.
    pub fn columns(&self) -> Option<Arc<Vec<Column>>> {
        match self.0 {
            QueryResultInner::Empty(..) => None,
            QueryResultInner::WithRows(_, ref columns, ..) => Some(columns.clone()),
        }
    }
}

impl<T: ConnectionLike + 'static, P: Protocol> ConnectionLikeWrapper for QueryResult<T, P> {
    type ConnLike = T;

    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized,
    {
        match self {
            QueryResult(Empty(conn_like, cached, _)) => match conn_like {
                Some(Either::Left(conn_like)) => {
                    let (streamless, stream) = conn_like.take_stream();
                    let self_streamless = Streamless::new(QueryResult(Empty(
                        Some(Either::Right(streamless)),
                        cached,
                        PhantomData,
                    )));
                    (self_streamless, stream)
                }
                Some(Either::Right(..)) => panic!("Logic error: stream taken"),
                None => unreachable!(),
            },
            QueryResult(WithRows(conn_like, columns, cached, _)) => match conn_like {
                Some(Either::Left(conn_like)) => {
                    let (streamless, stream) = conn_like.take_stream();
                    let self_streamless = Streamless::new(QueryResult(WithRows(
                        Some(Either::Right(streamless)),
                        columns,
                        cached,
                        PhantomData,
                    )));
                    (self_streamless, stream)
                }
                Some(Either::Right(..)) => panic!("Logic error: stream taken"),
                None => unreachable!(),
            },
        }
    }

    fn return_stream(&mut self, stream: io::Stream) {
        match *self {
            QueryResult(Empty(ref mut conn_like, ..))
            | QueryResult(WithRows(ref mut conn_like, ..)) => match conn_like.take() {
                Some(Either::Left(..)) => panic!("Logic error: stream exists"),
                Some(Either::Right(streamless)) => {
                    *conn_like = Some(Either::Left(streamless.return_stream(stream)));
                }
                None => unreachable!(),
            },
        }
    }

    fn conn_like_ref(&self) -> &Self::ConnLike {
        match *self {
            QueryResult(Empty(ref conn_like, ..)) | QueryResult(WithRows(ref conn_like, ..)) => {
                match *conn_like {
                    Some(Either::Left(ref conn_like)) => conn_like,
                    _ => unreachable!(),
                }
            }
        }
    }

    fn conn_like_mut(&mut self) -> &mut Self::ConnLike {
        match *self {
            QueryResult(Empty(ref mut conn_like, ..))
            | QueryResult(WithRows(ref mut conn_like, ..)) => match *conn_like {
                Some(Either::Left(ref mut conn_like)) => conn_like,
                _ => unreachable!(),
            },
        }
    }
}
