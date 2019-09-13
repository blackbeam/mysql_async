// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_util::future::Either;

use std::fmt;

use crate::{
    connection_like::{streamless::Streamless, ConnectionLike, ConnectionLikeWrapper},
    error::*,
    io,
    queryable::Queryable,
};

/// Options for transaction
#[derive(Eq, PartialEq, Debug, Hash, Clone, Default)]
pub struct TransactionOptions {
    consistent_snapshot: bool,
    isolation_level: Option<IsolationLevel>,
    readonly: Option<bool>,
}

impl TransactionOptions {
    pub fn new() -> TransactionOptions {
        TransactionOptions::default()
    }

    pub fn set_consistent_snapshot(&mut self, value: bool) -> &mut Self {
        self.consistent_snapshot = value;
        self
    }

    pub fn set_isolation_level<T>(&mut self, value: T) -> &mut Self
    where
        T: Into<Option<IsolationLevel>>,
    {
        self.isolation_level = value.into();
        self
    }

    pub fn set_readonly<T>(&mut self, value: T) -> &mut Self
    where
        T: Into<Option<bool>>,
    {
        self.readonly = value.into();
        self
    }

    pub fn consistent_snapshot(&self) -> bool {
        self.consistent_snapshot
    }

    pub fn isolation_level(&self) -> Option<IsolationLevel> {
        self.isolation_level
    }

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
/// `Transaction` it's a sugar for `START TRANSACTION`, `ROLLBACK` and `COMMIT` queries, so one
/// should note that it is easy to mess things up calling this queries manually. Also you will get
/// `NestedTransaction` error if you call `transaction.start_transaction(_)`.
pub struct Transaction<T>(Option<Either<T, Streamless<T>>>);

pub async fn new<T>(conn_like: T, options: TransactionOptions) -> Result<Transaction<T>>
where
    T: Queryable + ConnectionLike,
{
    Transaction::new(conn_like, options).await
}

impl<T: Queryable + ConnectionLike> Transaction<T> {
    async fn new(mut conn_like: T, options: TransactionOptions) -> Result<Transaction<T>> {
        let TransactionOptions {
            consistent_snapshot,
            isolation_level,
            readonly,
        } = options;

        if conn_like.get_in_transaction() {
            return Err(DriverError::NestedTransaction.into());
        }

        if readonly.is_some() && conn_like.get_server_version() < (5, 6, 5) {
            return Err(DriverError::ReadOnlyTransNotSupported.into());
        }

        if let Some(isolation_level) = isolation_level {
            let query = format!("SET TRANSACTION ISOLATION LEVEL {}", isolation_level);
            conn_like = conn_like.drop_query(query).await?;
        }

        if let Some(readonly) = readonly {
            if readonly {
                conn_like = conn_like.drop_query("SET TRANSACTION READ ONLY").await?;
            } else {
                conn_like = conn_like.drop_query("SET TRANSACTION READ WRITE").await?;
            }
        }

        conn_like = if consistent_snapshot {
            conn_like
                .drop_query("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .await?
        } else {
            conn_like.drop_query("START TRANSACTION").await?
        };

        conn_like.set_in_transaction(true);
        Ok(Transaction(Some(Either::Left(conn_like))))
    }

    fn unwrap(self) -> T {
        match self {
            Transaction(Some(Either::Left(conn_like))) => conn_like,
            _ => unreachable!(),
        }
    }

    /// Returns future that will perform `COMMIT` query and resolve to a wrapped `Queryable`.
    pub async fn commit(self) -> Result<T> {
        let mut this = self.drop_query("COMMIT").await?;
        this.set_in_transaction(false);
        Ok(this.unwrap())
    }

    /// Returns future that will perform `ROLLBACK` query and resolve to a wrapped `Queryable`.
    pub async fn rollback(self) -> Result<T> {
        let mut this = self.drop_query("ROLLBACK").await?;
        this.set_in_transaction(false);
        Ok(this.unwrap())
    }
}

impl<T: ConnectionLike + 'static> ConnectionLikeWrapper for Transaction<T> {
    type ConnLike = T;

    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized,
    {
        let Transaction(conn_like) = self;
        match conn_like {
            Some(Either::Left(conn_like)) => {
                let (streamless, stream) = conn_like.take_stream();
                let this = Transaction(Some(Either::Right(streamless)));
                (Streamless::new(this), stream)
            }
            _ => unreachable!(),
        }
    }

    fn return_stream(&mut self, stream: io::Stream) {
        let conn_like = self.0.take().unwrap();
        match conn_like {
            Either::Right(streamless) => {
                self.0 = Some(Either::Left(streamless.return_stream(stream)));
            }
            _ => unreachable!(),
        }
    }

    fn conn_like_ref(&self) -> &Self::ConnLike {
        match self.0 {
            Some(Either::Left(ref conn_like)) => conn_like,
            _ => unreachable!(),
        }
    }

    fn conn_like_mut(&mut self) -> &mut Self::ConnLike {
        match self.0 {
            Some(Either::Left(ref mut conn_like)) => conn_like,
            _ => unreachable!(),
        }
    }
}
