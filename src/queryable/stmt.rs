// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::{
    connection_like::{ConnectionLike, StmtCacheResult},
    error::*,
    prelude::FromRow,
    queryable::{query_result::QueryResult, transaction::TxStatus, BinaryProtocol},
    Column, Params,
    Value::{self},
};
use mysql_common::{
    constants::MAX_PAYLOAD_LEN,
    packets::{parse_stmt_packet, ComStmtExecuteRequestBuilder, ComStmtSendLongData},
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
    pub fn new(payload: &[u8], named_params: Option<Vec<String>>) -> Result<InnerStmt> {
        let packet = parse_stmt_packet(payload)?;

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

/// Prepared statement.
#[derive(Debug)]
pub struct Stmt<'a, T> {
    conn_like: &'a mut T,
    inner: InnerStmt,
    /// None => In use elsewhere
    /// Some(Cached) => Should not be closed
    /// Some(NotCached(_)) => Should be closed
    pub(crate) cached: Option<StmtCacheResult>,
}

impl<'a, T> Stmt<'a, T>
where
    T: crate::prelude::ConnectionLike,
{
    pub(crate) fn new(
        conn_like: &'a mut T,
        inner: InnerStmt,
        cached: StmtCacheResult,
    ) -> Stmt<'a, T> {
        Stmt {
            conn_like,
            inner,
            cached: Some(cached),
        }
    }

    /// Returns an identifier of the statement.
    pub fn id(&self) -> u32 {
        self.inner.statement_id
    }

    /// Returns a list of statement columns.
    ///
    /// ```rust
    /// # use mysql_async::test_misc::get_opts;
    /// use mysql_async::{consts::{ColumnFlags, ColumnType}, Pool};
    /// use mysql_async::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), mysql_async::error::Error> {
    ///     let pool = Pool::new(get_opts());
    ///     let mut conn = pool.get_conn().await?;
    ///
    ///     let stmt = conn.prepare("SELECT 'foo', CAST(42 AS UNSIGNED)").await?;
    ///
    ///     let columns = stmt.columns();
    ///
    ///     assert_eq!(columns.len(), 2);
    ///     assert_eq!(columns[0].column_type(), ColumnType::MYSQL_TYPE_VAR_STRING);
    ///     assert!(columns[1].flags().contains(ColumnFlags::UNSIGNED_FLAG));
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn columns(&self) -> &[Column] {
        self.inner
            .columns
            .as_ref()
            .map(|c| &c[..])
            .unwrap_or_default()
    }

    /// Returns a list of statement parameters.
    ///
    /// ```rust
    /// # use mysql_async::test_misc::get_opts;
    /// use mysql_async::{consts::{ColumnFlags, ColumnType}, Pool};
    /// use mysql_async::prelude::*;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), mysql_async::error::Error> {
    ///     let pool = Pool::new(get_opts());
    ///     let mut conn = pool.get_conn().await?;
    ///
    ///     let stmt = conn.prepare("SELECT ?, ?").await?;
    ///
    ///     assert_eq!(stmt.params().len(), 2);
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn params(&self) -> &[Column] {
        self.inner
            .params
            .as_ref()
            .map(|c| &c[..])
            .unwrap_or_default()
    }

    async fn send_long_data(&mut self, params: Vec<Value>) -> Result<()> {
        for (i, value) in params.into_iter().enumerate() {
            if let Value::Bytes(bytes) = value {
                let chunks = bytes.chunks(MAX_PAYLOAD_LEN - 6);
                let chunks = chunks.chain(if bytes.is_empty() {
                    Some(&[][..])
                } else {
                    None
                });
                for chunk in chunks {
                    let com = ComStmtSendLongData::new(self.inner.statement_id, i, chunk);
                    self.write_command_raw(com.into()).await?;
                }
            }
        }

        Ok(())
    }

    async fn execute_positional<U>(
        &mut self,
        params: U,
    ) -> Result<QueryResult<'_, Stmt<'a, T>, BinaryProtocol>>
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

        if as_long_data {
            self.send_long_data(params).await?
        }

        self.write_command_raw(body).await?;
        self.read_result_set(None).await
    }

    async fn execute_named(
        &mut self,
        params: Params,
    ) -> Result<QueryResult<'_, Stmt<'a, T>, BinaryProtocol>> {
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

    async fn execute_empty(&mut self) -> Result<QueryResult<'_, Stmt<'a, T>, BinaryProtocol>> {
        if self.inner.num_params > 0 {
            let error = DriverError::StmtParamsMismatch {
                required: self.inner.num_params,
                supplied: 0,
            }
            .into();
            return Err(error);
        }

        let (body, _) = ComStmtExecuteRequestBuilder::new(self.inner.statement_id).build(&[]);
        self.write_command_raw(body).await?;
        self.read_result_set(None).await
    }

    /// See [`Queryable::execute`].
    pub async fn execute<P>(
        &mut self,
        params: P,
    ) -> Result<QueryResult<'_, Stmt<'a, T>, BinaryProtocol>>
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

    /// See [`Queryable::first`].
    pub async fn first<P, R>(&mut self, params: P) -> Result<Option<R>>
    where
        P: Into<Params> + 'static,
        R: FromRow,
    {
        let result = self.execute(params).await?;
        let mut rows = result.collect_and_drop::<crate::Row>().await?;
        if rows.len() > 0 {
            Ok(Some(FromRow::from_row(rows.swap_remove(0))))
        } else {
            Ok(None)
        }
    }

    /// See [`Queryable::batch`].
    pub async fn batch<I, P>(&mut self, params_iter: I) -> Result<()>
    where
        I: IntoIterator<Item = P>,
        I::IntoIter: Send + 'static,
        Params: From<P>,
        P: 'static,
    {
        let mut params_iter = params_iter.into_iter().map(Params::from);
        loop {
            match params_iter.next() {
                Some(params) => {
                    let result = self.execute(params).await?;
                    result.drop_result().await?;
                }
                None => break Ok(()),
            }
        }
    }

    /// This will close statement (if it's not in the cache).
    pub async fn close(mut self) -> Result<()> {
        let cached = self.cached.take();
        if let Some(StmtCacheResult::NotCached(stmt_id)) = cached {
            self.conn_like.close_stmt(stmt_id).await?;
        }
        Ok(())
    }
}

impl<'a, T: ConnectionLike> ConnectionLike for Stmt<'a, T> {
    fn stream_mut(&mut self) -> &mut crate::io::Stream {
        self.conn_like.stream_mut()
    }
    fn stmt_cache_ref(&self) -> &crate::conn::stmt_cache::StmtCache {
        self.conn_like.stmt_cache_ref()
    }
    fn stmt_cache_mut(&mut self) -> &mut crate::conn::stmt_cache::StmtCache {
        self.conn_like.stmt_cache_mut()
    }
    fn get_affected_rows(&self) -> u64 {
        self.conn_like.get_affected_rows()
    }
    fn get_capabilities(&self) -> crate::consts::CapabilityFlags {
        self.conn_like.get_capabilities()
    }
    fn get_tx_status(&self) -> TxStatus {
        self.conn_like.get_tx_status()
    }
    fn get_last_insert_id(&self) -> Option<u64> {
        self.conn_like.get_last_insert_id()
    }
    fn get_info(&self) -> std::borrow::Cow<'_, str> {
        self.conn_like.get_info()
    }
    fn get_warnings(&self) -> u16 {
        self.conn_like.get_warnings()
    }
    fn get_local_infile_handler(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::local_infile_handler::LocalInfileHandler>> {
        self.conn_like.get_local_infile_handler()
    }
    fn get_max_allowed_packet(&self) -> usize {
        self.conn_like.get_max_allowed_packet()
    }
    fn get_opts(&self) -> &crate::Opts {
        self.conn_like.get_opts()
    }
    fn get_pending_result(&self) -> Option<&crate::conn::PendingResult> {
        self.conn_like.get_pending_result()
    }
    fn get_server_version(&self) -> (u16, u16, u16) {
        self.conn_like.get_server_version()
    }
    fn get_status(&self) -> crate::consts::StatusFlags {
        self.conn_like.get_status()
    }
    fn set_last_ok_packet(&mut self, ok_packet: Option<mysql_common::packets::OkPacket<'static>>) {
        self.conn_like.set_last_ok_packet(ok_packet)
    }
    fn set_tx_status(&mut self, tx_status: TxStatus) {
        self.conn_like.set_tx_status(tx_status)
    }
    fn set_pending_result(&mut self, meta: Option<crate::conn::PendingResult>) {
        self.conn_like.set_pending_result(meta)
    }
    fn set_status(&mut self, status: crate::consts::StatusFlags) {
        self.conn_like.set_status(status)
    }
    fn reset_seq_id(&mut self) {
        self.conn_like.reset_seq_id()
    }
    fn sync_seq_id(&mut self) {
        self.conn_like.sync_seq_id()
    }
    fn touch(&mut self) -> () {
        self.conn_like.touch()
    }
    fn on_disconnect(&mut self) {
        self.conn_like.on_disconnect()
    }
}
