// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::row::convert::FromRowError;
use mysql_common::{io::ReadMysqlExt, packets::parse_local_infile_packet};
use tokio::prelude::*;

use std::{borrow::Cow, marker::PhantomData, result::Result as StdResult, sync::Arc};

use crate::{
    connection_like::Connection,
    error::*,
    prelude::{FromRow, Protocol},
    Column, Row,
};

/// Result set metadata.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ResultSetMeta {
    /// Text result set, that may contain rows.
    Text(Arc<Vec<Column>>),
    /// Binary result set, that may contain rows.
    Binary(Arc<Vec<Column>>),
    /// Error result set.
    Error(ServerError),
}

impl ResultSetMeta {
    fn columns(&self) -> StdResult<&Arc<Vec<Column>>, &ServerError> {
        match self {
            ResultSetMeta::Text(cols) | ResultSetMeta::Binary(cols) => Ok(cols),
            ResultSetMeta::Error(err) => Err(err),
        }
    }
}

/// Result of a query or statement execution.
#[derive(Debug)]
pub struct QueryResult<'a, 't: 'a, P> {
    conn: Connection<'a, 't>,
    __phantom: PhantomData<P>,
}

impl<'a, 't: 'a, P> QueryResult<'a, 't, P>
where
    P: Protocol,
{
    pub(crate) fn new<T: Into<Connection<'a, 't>>>(conn: T) -> Self {
        QueryResult {
            conn: conn.into(),
            __phantom: PhantomData,
        }
    }

    /// Returns `true` if this query result may contain rows.
    ///
    /// If `false` then there is no rows possible (e.g. result of an UPDATE query).
    fn has_rows(&self) -> bool {
        self.conn
            .get_pending_result()
            .and_then(|meta| meta.columns().map(|columns| columns.len() > 0).ok())
            .unwrap_or(false)
    }

    /// `true` if there is no more rows nor result sets in this query.
    ///
    /// One could use it to check if there is more than one result set in this query result.
    pub fn is_empty(&self) -> bool {
        !self.has_rows() && !self.conn.more_results_exists()
    }

    pub async fn next(&mut self) -> Result<Option<Row>> {
        loop {
            let columns = match self.conn.get_pending_result() {
                Some(ResultSetMeta::Text(cols)) | Some(ResultSetMeta::Binary(cols)) => {
                    Ok(Some(cols.clone()))
                }
                Some(ResultSetMeta::Error(err)) => Err(Error::from(err.clone())),
                None => Ok(None),
            };

            match columns {
                Ok(Some(columns)) => {
                    if columns.is_empty() {
                        // Empty, but not yet consumed result set.
                        self.conn.set_pending_result(None);
                        return Ok(None);
                    } else {
                        // Not yet consumed non-empty result set.
                        let packet = match self.conn.read_packet().await {
                            Ok(packet) => packet,
                            Err(err) => {
                                // Next row contained an error. No more data will follow.
                                self.conn.set_pending_result(None);
                                return Err(err);
                            }
                        };

                        if P::is_last_result_set_packet(self.conn.capabilities(), &packet) {
                            // `packet` is a result set terminator.
                            self.conn.set_pending_result(None);
                            return Ok(None);
                        } else {
                            // `packet` is a result set row.
                            return Ok(Some(P::read_result_set_row(&packet, columns)?));
                        }
                    }
                }
                Ok(None) => {
                    // Consumed result set.
                    if self.conn.more_results_exists() {
                        // More data will follow.
                        self.conn.sync_seq_id();
                        self.conn.read_result_set::<P>().await?;
                        continue;
                    } else {
                        // The end of a query result.
                        return Ok(None);
                    }
                }
                Err(err) => {
                    // Error result set. No more data will follow.
                    self.conn.set_pending_result(None);
                    return Err(err);
                }
            }
        }
    }

    /// Last insert id, if any.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.conn.last_insert_id()
    }

    /// Number of affected rows, as reported by the server, or `0`.
    pub fn affected_rows(&self) -> u64 {
        self.conn.affected_rows()
    }

    /// Text information, as reported by the server, or an empty string.
    pub fn info(&self) -> Cow<'_, str> {
        self.conn.info()
    }

    /// Number of warnings, as reported by the server, or `0`.
    pub fn warnings(&self) -> u16 {
        self.conn.get_warnings()
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
    pub async fn collect<R>(&mut self) -> Result<Vec<R>>
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
    pub async fn collect_and_drop<R>(mut self) -> Result<Vec<R>>
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
            while let Some(row) = self.next().await? {
                fun(row);
            }
            Ok(())
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
        let mut acc = Vec::new();
        while let Some(row) = self.next().await? {
            acc.push(fun(crate::from_row(row)));
        }
        Ok(acc)
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
    pub async fn reduce<T, F, U>(&mut self, mut init: U, mut fun: F) -> Result<U>
    where
        F: FnMut(U, T) -> U,
        T: FromRow + Send + 'static,
    {
        while let Some(row) = self.next().await? {
            init = fun(init, crate::from_row(row));
        }
        Ok(init)
    }

    /// Returns a future that will reduce rows of the current result set to `U` using `fun` and drop
    /// everything else.
    pub async fn reduce_and_drop<T, F, U>(mut self, init: U, fun: F) -> Result<U>
    where
        F: FnMut(U, T) -> U,
        T: FromRow + Send + 'static,
    {
        let acc = self.reduce(init, fun).await?;
        self.drop_result().await?;
        Ok(acc)
    }

    /// Returns a future that will drop this query result.
    pub async fn drop_result(mut self) -> Result<()> {
        loop {
            while let Some(_) = self.next().await? {}
            if !self.conn.more_results_exists() {
                break Ok(());
            }
        }
    }

    /// Returns a reference to a columns list of this query result.
    ///
    /// Empty list means, that this result set was never meant to contain rows.
    pub fn columns_ref(&self) -> &[Column] {
        self.conn
            .get_pending_result()
            .and_then(|meta| meta.columns().map(|cols| &cols[..]).ok())
            .unwrap_or_default()
    }

    /// Returns a copy of a columns list of this query result.
    pub fn columns(&self) -> Option<Arc<Vec<Column>>> {
        self.conn
            .get_pending_result()
            .and_then(|meta| meta.columns().map(|columns| columns.clone()).ok())
    }
}

impl crate::Conn {
    /// Will read result set and write pending result into `self` (if any).
    pub(crate) async fn read_result_set<P>(&mut self) -> Result<()>
    where
        P: Protocol,
    {
        let packet = self.read_packet().await?;

        match packet.get(0) {
            Some(0x00) => self.set_pending_result(Some(P::result_set_meta(Default::default()))),
            Some(0xFB) => self.handle_local_infile::<P>(&*packet).await?,
            _ => self.handle_result_set::<P>(&*packet).await?,
        }

        Ok(())
    }

    /// Will handle local infile packet.
    pub(crate) async fn handle_local_infile<P>(&mut self, packet: &[u8]) -> Result<()>
    where
        P: Protocol,
    {
        let local_infile = parse_local_infile_packet(&*packet)?;
        let (local_infile, handler) = match self.opts().local_infile_handler() {
            Some(handler) => ((local_infile.into_owned(), handler)),
            None => return Err(DriverError::NoLocalInfileHandler.into()),
        };
        let mut reader = handler.handle(local_infile.file_name_ref()).await?;

        let mut buf = [0; 4096];
        loop {
            let read = reader.read(&mut buf[..]).await?;
            self.write_packet(&buf[..read]).await?;

            if read == 0 {
                break;
            }
        }

        self.read_packet().await?;
        self.set_pending_result(Some(P::result_set_meta(Default::default())));
        Ok(())
    }

    /// Helper that handles result set packet.
    ///
    /// Requires that `packet` contains non-zero length-encoded integer.
    pub(crate) async fn handle_result_set<P>(&mut self, mut packet: &[u8]) -> Result<()>
    where
        P: Protocol,
    {
        let column_count = packet.read_lenenc_int()?;
        let columns = self.read_column_defs(column_count as usize).await?;
        let meta = P::result_set_meta(Arc::new(columns));
        self.set_pending_result(Some(meta));
        Ok(())
    }
}
