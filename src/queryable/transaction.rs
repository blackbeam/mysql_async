// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::fmt;

use crate::{connection_like::ConnectionLike, error::*, queryable::Queryable, Conn};

/// Transaction status.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum TxStatus {
    /// Connection is in transaction at the moment.
    InTransaction,
    /// `Transaction` was dropped without explicit call to `commit` or `rollback`.
    RequiresRollback,
    /// Connection is not in transaction at the moment.
    None,
}

impl Conn {
    /// Returns a future that starts a transaction.
    pub async fn start_transaction(
        &mut self,
        options: TransactionOptions,
    ) -> Result<Transaction<'_, Self>> {
        Transaction::new(self, options).await
    }
}

/// Transaction options.
#[derive(Eq, PartialEq, Debug, Hash, Clone, Default)]
pub struct TransactionOptions {
    consistent_snapshot: bool,
    isolation_level: Option<IsolationLevel>,
    readonly: Option<bool>,
}

impl TransactionOptions {
    /// Creates a default instance.
    pub fn new() -> TransactionOptions {
        TransactionOptions::default()
    }

    /// See [`TransactionOptions::consistent_snapshot`].
    pub fn set_consistent_snapshot(&mut self, value: bool) -> &mut Self {
        self.consistent_snapshot = value;
        self
    }

    /// See [`TransactionOptions::isolation_level`].
    pub fn set_isolation_level<T>(&mut self, value: T) -> &mut Self
    where
        T: Into<Option<IsolationLevel>>,
    {
        self.isolation_level = value.into();
        self
    }

    /// See [`TransactionOptions::readonly`].
    pub fn set_readonly<T>(&mut self, value: T) -> &mut Self
    where
        T: Into<Option<bool>>,
    {
        self.readonly = value.into();
        self
    }

    /// If true, then `START TRANSACTION WITH CONSISTENT SNAPSHOT` will be performed.
    /// Defaults to `false`.
    pub fn consistent_snapshot(&self) -> bool {
        self.consistent_snapshot
    }

    /// If not `None`, then `SET TRANSACTION ISOLATION LEVEL ..` will be performed.
    /// Defaults to `None`.
    pub fn isolation_level(&self) -> Option<IsolationLevel> {
        self.isolation_level
    }

    /// If not `None`, then `SET TRANSACTION READ ONLY|WRITE` will be performed.
    /// Defaults to `None`.
    ///
    /// Only available since MySql 5.6.5.
    pub fn readonly(&self) -> Option<bool> {
        self.readonly
    }
}

/// Transaction isolation level.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Hash)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            IsolationLevel::ReadUncommitted => write!(f, "READ UNCOMMITTED"),
            IsolationLevel::ReadCommitted => write!(f, "READ COMMITTED"),
            IsolationLevel::RepeatableRead => write!(f, "REPEATABLE READ"),
            IsolationLevel::Serializable => write!(f, "SERIALIZABLE"),
        }
    }
}

/// This struct represents MySql transaction.
///
/// `Transaction` is just a sugar for `START TRANSACTION`, `ROLLBACK` and `COMMIT` queries, so one
/// should note that it is easy to mess things up calling this queries manually. Also you will get
/// `NestedTransaction` error if you call `transaction.start_transaction(_)`.
pub struct Transaction<'a, T: ConnectionLike>(&'a mut T);

impl<'a, T: Queryable> Transaction<'a, T> {
    pub(crate) async fn new(
        conn_like: &'a mut T,
        options: TransactionOptions,
    ) -> Result<Transaction<'a, T>> {
        let TransactionOptions {
            consistent_snapshot,
            isolation_level,
            readonly,
        } = options;

        if conn_like.get_tx_status() != TxStatus::None {
            return Err(DriverError::NestedTransaction.into());
        }

        if readonly.is_some() && conn_like.get_server_version() < (5, 6, 5) {
            return Err(DriverError::ReadOnlyTransNotSupported.into());
        }

        if let Some(isolation_level) = isolation_level {
            let query = format!("SET TRANSACTION ISOLATION LEVEL {}", isolation_level);
            conn_like.query_drop(query).await?;
        }

        if let Some(readonly) = readonly {
            if readonly {
                conn_like.query_drop("SET TRANSACTION READ ONLY").await?;
            } else {
                conn_like.query_drop("SET TRANSACTION READ WRITE").await?;
            }
        }

        if consistent_snapshot {
            conn_like
                .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .await?
        } else {
            conn_like.query_drop("START TRANSACTION").await?
        };

        conn_like.set_tx_status(TxStatus::InTransaction);
        Ok(Transaction(conn_like))
    }

    /// Performs `COMMIT` query.
    pub async fn commit(mut self) -> Result<()> {
        let result = self.0.query_iter("COMMIT").await?;
        result.drop_result().await?;
        self.set_tx_status(TxStatus::None);
        Ok(())
    }

    /// Performs `ROLLBACK` query.
    pub async fn rollback(mut self) -> Result<()> {
        let result = self.0.query_iter("ROLLBACK").await?;
        result.drop_result().await?;
        self.set_tx_status(TxStatus::None);
        Ok(())
    }
}

impl<T: ConnectionLike> Drop for Transaction<'_, T> {
    fn drop(&mut self) {
        if self.get_tx_status() == TxStatus::InTransaction {
            self.set_tx_status(TxStatus::RequiresRollback);
        }
    }
}

impl<'a, T: ConnectionLike> ConnectionLike for Transaction<'a, T> {
    fn conn_mut(&mut self) -> &mut crate::Conn {
        self.0.conn_mut()
    }

    fn stream_mut(&mut self) -> &mut crate::io::Stream {
        self.0.stream_mut()
    }
    fn get_affected_rows(&self) -> u64 {
        self.0.get_affected_rows()
    }
    fn get_capabilities(&self) -> crate::consts::CapabilityFlags {
        self.0.get_capabilities()
    }
    fn get_tx_status(&self) -> TxStatus {
        self.0.get_tx_status()
    }
    fn get_last_insert_id(&self) -> Option<u64> {
        self.0.get_last_insert_id()
    }
    fn get_info(&self) -> std::borrow::Cow<'_, str> {
        self.0.get_info()
    }
    fn get_warnings(&self) -> u16 {
        self.0.get_warnings()
    }
    fn get_local_infile_handler(
        &self,
    ) -> Option<std::sync::Arc<dyn crate::local_infile_handler::LocalInfileHandler>> {
        self.0.get_local_infile_handler()
    }
    fn get_max_allowed_packet(&self) -> usize {
        self.0.get_max_allowed_packet()
    }
    fn get_opts(&self) -> &crate::Opts {
        self.0.get_opts()
    }
    fn get_pending_result(&self) -> Option<&crate::conn::PendingResult> {
        self.0.get_pending_result()
    }
    fn get_server_version(&self) -> (u16, u16, u16) {
        self.0.get_server_version()
    }
    fn get_status(&self) -> crate::consts::StatusFlags {
        self.0.get_status()
    }
    fn set_last_ok_packet(&mut self, ok_packet: Option<mysql_common::packets::OkPacket<'static>>) {
        self.0.set_last_ok_packet(ok_packet)
    }
    fn set_tx_status(&mut self, tx_status: TxStatus) {
        self.0.set_tx_status(tx_status)
    }
    fn set_pending_result(&mut self, meta: Option<crate::conn::PendingResult>) {
        self.0.set_pending_result(meta)
    }
    fn set_status(&mut self, status: crate::consts::StatusFlags) {
        self.0.set_status(status)
    }
    fn reset_seq_id(&mut self) {
        self.0.reset_seq_id()
    }
    fn sync_seq_id(&mut self) {
        self.0.sync_seq_id()
    }
    fn touch(&mut self) -> () {
        self.0.touch()
    }
    fn on_disconnect(&mut self) {
        self.0.on_disconnect()
    }
    fn connection_id(&self) -> u32 {
        self.0.connection_id()
    }
    fn stmt_cache_ref(&self) -> &crate::conn::stmt_cache::StmtCache {
        self.0.stmt_cache_ref()
    }
    fn stmt_cache_mut(&mut self) -> &mut crate::conn::stmt_cache::StmtCache {
        self.0.stmt_cache_mut()
    }
}
