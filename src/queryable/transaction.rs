// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use BoxFuture;
use Column;
use connection_like::ConnectionLike;
use connection_like::streamless::Streamless;
use consts::{CapabilityFlags, Command, StatusFlags};
use errors::*;
use io;
use lib_futures::future::{Either, Future, IntoFuture, err, ok};
use lib_futures::future::Either::*;
use local_infile_handler::LocalInfileHandler;
use proto::OkPacket;
use queryable::Queryable;
use queryable::stmt::InnerStmt;
use std::fmt;
use std::sync::Arc;

/// Options for transaction
#[derive(Eq, PartialEq, Debug, Hash, Clone, Default)]
pub struct TransactionOptions {
    consistent_snapshot: bool,
    isolation_level: Option<IsolationLevel>,
    readonly: Option<bool>
}

impl TransactionOptions {
    pub fn set_consistent_snapshot(&mut self, value: bool) -> &mut Self {
        self.consistent_snapshot = value;
        self
    }

    pub fn set_isolation_level<T>(&mut self, value: T) -> &mut Self
        where T: Into<Option<IsolationLevel>>
    {
        self.isolation_level = value.into();
        self
    }

    pub fn set_readonly<T>(&mut self, value: T) -> &mut Self
        where T: Into<Option<bool>>
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

pub fn new<T>(conn_like: T, options: TransactionOptions) -> BoxFuture<Transaction<T>>
    where T: Queryable + ConnectionLike
{
    Transaction::new(conn_like, options)
}

impl<T: Queryable + ConnectionLike> Transaction<T> {
    fn new(conn_like: T,options: TransactionOptions) -> BoxFuture<Transaction<T>> {
        let TransactionOptions {
            consistent_snapshot,
            isolation_level,
            readonly,
        } = options;

        if conn_like.get_in_transaction() {
            return Box::new(err(ErrorKind::NestedTransaction.into()));
        }

        if readonly.is_some() && conn_like.get_server_version() < (5, 6, 5) {
            return Box::new(err(ErrorKind::ReadOnlyTransNotSupported.into()));
        }

        let fut = if let Some(isolation_level) = isolation_level {
            A(conn_like.drop_query(format!("SET TRANSACTION ISOLATION LEVEL {}", isolation_level)))
        } else {
            B(ok(conn_like))
        };

        let fut = fut.into_future()
            .and_then(
                move |conn_like| if let Some(readonly) = readonly {
                    if readonly {
                        A(A(conn_like.drop_query("SET TRANSACTION READ ONLY")))
                    } else {
                        A(B(conn_like.drop_query("SET TRANSACTION READ WRITE")))
                    }
                } else {
                    B(ok(conn_like))
                }
            )
            .and_then(
                move |conn_like| if consistent_snapshot {
                    conn_like.drop_query("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                } else {
                    conn_like.drop_query("START TRANSACTION")
                }
            )
            .map(
                |mut conn_like| {
                    conn_like.set_in_transaction(true);
                    Transaction(Some(A(conn_like)))
                }
            );

        Box::new(fut)
    }

    fn conn_like_ref(&self) -> &T {
        match self.0 {
            Some(A(ref conn_like)) => conn_like,
            _ => unreachable!()
        }
    }

    fn conn_like_mut(&mut self) -> &mut T {
        match self.0 {
            Some(A(ref mut conn_like)) => conn_like,
            _ => unreachable!()
        }
    }

    fn unwrap(self) -> T {
        match self {
            Transaction(Some(A(conn_like))) => conn_like,
            _ => unreachable!(),
        }
    }

    /// Returns future that will perform `COMMIT` query and resolve to a wrapped `Queryable`.
    pub fn commit(self) -> BoxFuture<T> {
        let fut = self.drop_query("COMMIT")
            .map(|mut this| {
                this.set_in_transaction(false);
                this.unwrap()
            });
        Box::new(fut)
    }

    /// Returns future that will perform `ROLLBACK` query and resolve to a wrapped `Queryable`.
    pub fn rollback(self) -> BoxFuture<T> {
        let fut = self.drop_query("ROLLBACK")
            .map(|mut this| {
                this.set_in_transaction(false);
                this.unwrap()
            });
        Box::new(fut)
    }
}

impl<T: Queryable + ConnectionLike> ConnectionLike for Transaction<T> {
    fn take_stream(self) -> (Streamless<Self>, io::Stream) where Self: Sized {
        let Transaction(conn_like) = self;
        match conn_like {
            Some(A(conn_like)) => {
                let (streamless, stream) = conn_like.take_stream();
                let this = Transaction(Some(B(streamless)));
                (Streamless::new(this), stream)
            },
            _ => unreachable!(),
        }
    }

    fn return_stream(&mut self, stream: io::Stream) -> () {
        let conn_like = self.0.take().unwrap();
        match conn_like {
            B(streamless) => {
                self.0 = Some(A(streamless.return_stream(stream)));
            },
            _ => unreachable!(),
        }
    }

    fn cache_stmt(&mut self, query: String, stmt: InnerStmt) {
        self.conn_like_mut().cache_stmt(query, stmt);
    }

    fn get_affected_rows(&self) -> u64 {
        self.conn_like_ref().get_affected_rows()
    }

    fn get_cached_stmt(&self, query: &String) -> Option<&InnerStmt> {
        self.conn_like_ref().get_cached_stmt(query)
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

    fn get_pending_result(&self) -> Option<&(Arc<Vec<Column>>, Option<OkPacket>, Option<InnerStmt>)> {
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

    fn set_pending_result(&mut self, meta: Option<(Arc<Vec<Column>>, Option<OkPacket>, Option<InnerStmt>)>) {
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
