// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::row::convert::FromRowError;

use std::{borrow::Cow, marker::PhantomData, result::Result as StdResult, sync::Arc};

use self::QueryResultInner::*;
use crate::{
    consts::StatusFlags,
    error::*,
    prelude::{ConnectionLike, FromRow, Protocol},
    Column, Row,
};

enum QueryResultInner {
    Empty,
    WithRows(Arc<Vec<Column>>),
}

impl QueryResultInner {
    fn new(columns: Option<Arc<Vec<Column>>>) -> Self {
        match columns {
            Some(columns) => WithRows(columns),
            None => Empty,
        }
    }

    fn columns(&self) -> Option<&Arc<Vec<Column>>> {
        match self {
            WithRows(columns) => Some(columns),
            Empty => None,
        }
    }

    fn make_empty(&mut self) {
        *self = Empty
    }
}

/// Result of a query or statement execution.
pub struct QueryResult<'a, T: ?Sized, P> {
    conn_like: &'a mut T,
    inner: QueryResultInner,
    __phantom: PhantomData<P>,
}

impl<'a, T: ?Sized, P> QueryResult<'a, T, P>
where
    P: Protocol,
    T: ConnectionLike,
{
    pub(crate) fn new(
        conn_like: &'a mut T,
        columns: Option<Arc<Vec<Column>>>,
    ) -> QueryResult<'a, T, P> {
        QueryResult {
            conn_like,
            inner: QueryResultInner::new(columns),
            __phantom: PhantomData,
        }
    }

    pub(crate) fn disassemble(self) -> (&'a mut T, Option<Arc<Vec<Column>>>) {
        match self.inner {
            WithRows(columns) => (self.conn_like, Some(columns)),
            Empty => (self.conn_like, None),
        }
    }

    fn make_empty(&mut self) {
        self.conn_like.set_pending_result(None);
        self.inner.make_empty();
    }

    async fn get_row_raw(&mut self) -> Result<Option<Vec<u8>>> {
        if self.is_empty() {
            return Ok(None);
        }

        let packet: Vec<u8> = self.conn_like.read_packet().await?;

        if P::is_last_result_set_packet(&*self.conn_like, &packet) {
            if self.more_results_exists() {
                self.conn_like.sync_seq_id();
                let next_set = self.conn_like.read_result_set::<P>().await?;
                self.inner = next_set.inner;
                Ok(None)
            } else {
                self.make_empty();
                Ok(None)
            }
        } else {
            Ok(Some(packet))
        }
    }

    /// Returns next row, if any.
    ///
    /// Requires that `self.inner` matches `WithRows(..)`.
    pub(crate) async fn get_row(&mut self) -> Result<Option<Row>> {
        let packet = self.get_row_raw().await?;
        if let Some(packet) = packet {
            let columns = self.inner.columns().expect("must be here");
            let row = P::read_result_set_row(&packet, columns.clone())?;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    /// Last insert id, if any.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.conn_like.get_last_insert_id()
    }

    /// Number of affected rows, as reported by the server, or `0`.
    pub fn affected_rows(&self) -> u64 {
        self.conn_like.get_affected_rows()
    }

    /// Text information, as reported by the server, or an empty string.
    pub fn info(&self) -> Cow<'_, str> {
        self.conn_like.get_info()
    }

    /// Number of warnings, as reported by the server, or `0`.
    pub fn warnings(&self) -> u16 {
        self.conn_like.get_warnings()
    }

    /// `true` if there is no more rows nor result sets in this query.
    ///
    /// One could use it to check if there is more than one result set in this query result.
    pub fn is_empty(&self) -> bool {
        !self.has_rows()
    }

    /// Returns `true` if the `SERVER_MORE_RESULTS_EXISTS` flag is contained in status flags
    /// of the connection.
    fn more_results_exists(&self) -> bool {
        self.conn_like
            .get_status()
            .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
    }

    /// Returns `true` if this query result may contain rows.
    ///
    /// If `false` then there is no rows possible (for example UPDATE query).
    fn has_rows(&self) -> bool {
        matches!(self.inner, WithRows(..))
    }

    /// Returns a future that collects result set of this query result.
    ///
    /// It is parametrized by `R` and internally calls `R::from_row(Row)` on each row.
    ///
    /// It will collect rows up to a neares result set boundary. This means that you should call
    /// `collect` as many times as result sets in your query result. For example query
    /// `SELECT 'foo'; SELECT 'foo', 'bar';` will produce `QueryResult` with two result sets in it.
    /// One can use `QueryResult::is_empty` to make sure that there is no more result sets.
    ///
    /// # Panic
    ///
    /// It'll panic if any row isn't convertible to `R` (i.e. programmer error or unknown schema).
    /// * In case of programmer error see [`FromRow`] docs;
    /// * In case of unknown schema use [`QueryResult::try_collect`].
    pub async fn collect<'b, R>(&mut self) -> Result<Vec<R>>
    where
        R: FromRow + Send + 'static,
    {
        self.reduce(Vec::new(), |mut acc, row| {
            acc.push(FromRow::from_row(row));
            acc
        })
        .await
    }

    /// Returns a future that collects result set of this query result.
    ///
    /// It works the same way as [`QueryResult::collect`] but won't panic if row isn't convertible
    /// to `R`.
    pub async fn try_collect<R>(&mut self) -> Result<Vec<StdResult<R, FromRowError>>>
    where
        R: FromRow + Send + 'static,
    {
        self.reduce(Vec::new(), |mut acc, row| {
            acc.push(FromRow::from_row_opt(row));
            acc
        })
        .await
    }

    /// Returns a future that collects the current result set of this query result and drops
    /// everything else.
    ///
    /// # Panic
    ///
    /// It'll panic if any row isn't convertible to `R` (i.e. programmer error or unknown schema).
    /// * In case of programmer error see `FromRow` docs;
    /// * In case of unknown schema use [`QueryResult::try_collect`].
    pub async fn collect_and_drop<'b, R>(mut self) -> Result<Vec<R>>
    where
        R: FromRow + Send + 'static,
    {
        let output = self.collect::<R>().await?;
        self.drop_result().await?;
        Ok(output)
    }

    /// Returns a future that collects the current result set of this query result and drops
    /// everything else.
    ///
    /// It works the same way as [`QueryResult::collect_and_drop`] but won't panic if row isn't
    /// convertible to `R`.
    pub async fn try_collect_and_drop<R>(mut self) -> Result<Vec<StdResult<R, FromRowError>>>
    where
        R: FromRow + Send + 'static,
    {
        let output = self.try_collect().await?;
        self.drop_result().await?;
        Ok(output)
    }

    /// Returns a future that will execute `fun` on every row of the current result set.
    ///
    /// It will stop on the nearest result set boundary (see `QueryResult::collect` docs).
    pub async fn for_each<F>(&mut self, mut fun: F) -> Result<()>
    where
        F: FnMut(Row),
    {
        if self.is_empty() {
            Ok(())
        } else {
            loop {
                let row = self.get_row().await?;
                if let Some(row) = row {
                    fun(row);
                } else {
                    break Ok(());
                }
            }
        }
    }

    /// Returns a future that will execute `fun` on every row of the current result set and drop
    /// everything else.
    pub async fn for_each_and_drop<F>(mut self, fun: F) -> Result<()>
    where
        F: FnMut(Row),
    {
        self.for_each(fun).await?;
        self.drop_result().await?;
        Ok(())
    }

    /// Returns a future that will map every row of the current result set to `U` using `fun`.
    ///
    /// It will stop on the nearest result set boundary (see `QueryResult::collect` docs).
    pub async fn map<F, U>(&mut self, mut fun: F) -> Result<Vec<U>>
    where
        F: FnMut(Row) -> U,
    {
        if self.is_empty() {
            Ok(Vec::new())
        } else {
            let mut rows = Vec::new();
            loop {
                let row = self.get_row().await?;
                if let Some(row) = row {
                    rows.push(fun(row));
                } else {
                    break Ok(rows);
                }
            }
        }
    }

    /// Returns a future that will map every row of the current result set to `U` using `fun`
    /// and drop everything else.
    pub async fn map_and_drop<F, U>(mut self, fun: F) -> Result<Vec<U>>
    where
        F: FnMut(Row) -> U,
    {
        let rows = self.map(fun).await?;
        self.drop_result().await?;
        Ok(rows)
    }

    /// Returns a future that will reduce rows of the current result set to `U` using `fun`.
    ///
    /// It will stop on the nearest result set boundary (see `QueryResult::collect` docs).
    pub async fn reduce<F, U>(&mut self, init: U, mut fun: F) -> Result<U>
    where
        F: FnMut(U, Row) -> U,
    {
        if self.is_empty() {
            Ok(init)
        } else {
            let mut acc = init;
            loop {
                let row = self.get_row().await?;
                if let Some(row) = row {
                    acc = fun(acc, row);
                } else {
                    break Ok(acc);
                }
            }
        }
    }

    /// Returns a future that will reduce rows of the current result set to `U` using `fun` and drop
    /// everything else.
    pub async fn reduce_and_drop<F, U>(mut self, init: U, fun: F) -> Result<U>
    where
        F: FnMut(U, Row) -> U,
    {
        let acc = self.reduce(init, fun).await?;
        self.drop_result().await?;
        Ok(acc)
    }

    /// Returns a future that will drop this query result.
    pub async fn drop_result(mut self) -> Result<()> {
        loop {
            if !self.has_rows() {
                if self.more_results_exists() {
                    let (inner, _) = self.disassemble();
                    self = inner.read_result_set().await?;
                } else {
                    break;
                }
            } else {
                self.get_row_raw().await?;
            }
        }

        Ok(())
    }

    /// Returns a reference to a columns list of this query result.
    pub fn columns_ref(&self) -> &[Column] {
        self.inner
            .columns()
            .map(|columns| &***columns)
            .unwrap_or_default()
    }

    /// Returns a copy of a columns list of this query result.
    pub fn columns(&self) -> Option<Arc<Vec<Column>>> {
        self.inner.columns().cloned()
    }
}
