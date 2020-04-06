// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::fmt;

use crate::{
    connection_like::{Connection, ConnectionLike},
    error::*,
    queryable::Queryable,
    Conn,
};

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
    pub async fn start_transaction(&mut self, options: TxOpts) -> Result<Transaction<'_>> {
        Transaction::new(self, options).await
    }
}

/// Transaction options.
///
/// Example:
///
/// ```
/// # use mysql_async::*;
/// # fn main() -> Result<()> {
/// let tx_opts = TxOpts::default()
///     .with_consistent_snapshot(true)
///     .with_isolation_level(IsolationLevel::RepeatableRead);
/// # Ok(()) }
/// ```
#[derive(Eq, PartialEq, Debug, Hash, Clone, Default)]
pub struct TxOpts {
    consistent_snapshot: bool,
    isolation_level: Option<IsolationLevel>,
    readonly: Option<bool>,
}

impl TxOpts {
    /// Creates a default instance.
    pub fn new() -> TxOpts {
        TxOpts::default()
    }

    /// See [`TxOpts::consistent_snapshot`].
    pub fn with_consistent_snapshot(&mut self, value: bool) -> &mut Self {
        self.consistent_snapshot = value;
        self
    }

    /// See [`TxOpts::isolation_level`].
    pub fn with_isolation_level<T>(&mut self, value: T) -> &mut Self
    where
        T: Into<Option<IsolationLevel>>,
    {
        self.isolation_level = value.into();
        self
    }

    /// See [`TxOpts::readonly`].
    pub fn with_readonly<T>(&mut self, value: T) -> &mut Self
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
#[derive(Debug)]
pub struct Transaction<'a>(Connection<'a, 'static>);

impl<'a> Transaction<'a> {
    pub(crate) async fn new<T: Into<Connection<'a, 'static>>>(
        conn: T,
        options: TxOpts,
    ) -> Result<Transaction<'a>> {
        let TxOpts {
            consistent_snapshot,
            isolation_level,
            readonly,
        } = options;

        let mut conn = conn.into();

        if conn.conn_ref().get_tx_status() != TxStatus::None {
            return Err(DriverError::NestedTransaction.into());
        }

        if readonly.is_some() && conn.conn_ref().server_version() < (5, 6, 5) {
            return Err(DriverError::ReadOnlyTransNotSupported.into());
        }

        if let Some(isolation_level) = isolation_level {
            let query = format!("SET TRANSACTION ISOLATION LEVEL {}", isolation_level);
            conn.conn_mut().query_drop(query).await?;
        }

        if let Some(readonly) = readonly {
            if readonly {
                conn.conn_mut()
                    .query_drop("SET TRANSACTION READ ONLY")
                    .await?;
            } else {
                conn.conn_mut()
                    .query_drop("SET TRANSACTION READ WRITE")
                    .await?;
            }
        }

        if consistent_snapshot {
            conn.conn_mut()
                .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .await?
        } else {
            conn.conn_mut().query_drop("START TRANSACTION").await?
        };

        conn.conn_mut().set_tx_status(TxStatus::InTransaction);
        Ok(Transaction(conn))
    }

    /// Performs `COMMIT` query.
    pub async fn commit(mut self) -> Result<()> {
        let result = self.0.conn_mut().query_iter("COMMIT").await?;
        result.drop_result().await?;
        self.0.conn_mut().set_tx_status(TxStatus::None);
        Ok(())
    }

    /// Performs `ROLLBACK` query.
    pub async fn rollback(mut self) -> Result<()> {
        let result = self.0.conn_mut().query_iter("ROLLBACK").await?;
        result.drop_result().await?;
        self.0.conn_mut().set_tx_status(TxStatus::None);
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        let conn = self.0.conn_mut();
        if conn.get_tx_status() == TxStatus::InTransaction {
            conn.set_tx_status(TxStatus::RequiresRollback);
        }
    }
}

impl ConnectionLike for Transaction<'_> {
    fn conn_ref(&self) -> &crate::Conn {
        self.0.conn_ref()
    }

    fn conn_mut(&mut self) -> &mut crate::Conn {
        self.0.conn_mut()
    }
}
