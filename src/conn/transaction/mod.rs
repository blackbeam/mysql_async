// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use lib_futures::Future;
use self::futures::*;
use self::futures::query_result::*;
use std::fmt;
use value::FromRow;
use value::Params;


pub mod futures;


/// Transaction isolation level.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
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

/// Wrapper for in-transaction connection.
pub struct Transaction {
    conn: Conn,
}

impl Transaction {
    /// Creates transaction
    fn new(mut conn: Conn) -> Transaction {
        conn.in_transaction = true;
        Transaction { conn: conn }
    }

    /// Cleans connection
    fn clean_conn(mut conn: Conn) -> Conn {
        conn.in_transaction = false;
        conn
    }

    /// Will create transaction without affecting conn.in_transaction
    fn new_raw(conn: Conn) -> Transaction {
        Transaction { conn: conn }
    }

    /// Returns future that commits transaction and resolves to `Conn`.
    pub fn commit(self) -> Commit {
        new_commit(self)
    }

    /// Returns future that rolls back transaction and resolves to `Conn`.
    pub fn rollback(self) -> Rollback {
        new_rollback(self)
    }

    /// Returns future that executes query and resolves to `TransTextQueryResult`.
    pub fn query<Q: AsRef<str>>(self, query: Q) -> TransQuery {
        self.conn.query(query).map(new_text)
    }

    /// Returns future that executes query and resolves to `Transaction`.
    ///
    /// Result will be dropped.
    pub fn drop_query<Q: AsRef<str>>(self, query: Q) -> TransDropQuery {
        self.conn.drop_query(query).map(Transaction::new_raw)
    }

    /// Returns future that executes query and resolves to `(Option<R>, Transaction)`.
    ///
    /// Where `Option<R>` is the first row of a query execution result (if any).
    pub fn first<R, Q>(self, query: Q) -> TransFirst<R>
        where R: FromRow,
              Q: AsRef<str>
    {
        fn map<R: FromRow>((row, conn): (Option<R>, Conn)) -> (Option<R>, Transaction) {
            (row, Transaction::new_raw(conn))
        }

        self.conn.first(query).map(map)
    }

    /// Returns future that prepares and executes statement and resolves to `TransBinQueryResult`.
    pub fn prep_exec<Q: AsRef<str>, P: Into<Params>>(self, query: Q, params: P) -> TransPrepExec {
        self.conn.prep_exec(query, params).map(new_bin)
    }

    /// Returns future that prepares and executes statement and resolves to `Transaction`.
    ///
    /// Result will be dropped.
    pub fn drop_exec<Q, P>(self, query: Q, params: P) -> TransDropExec
        where Q: AsRef<str>,
              P: Into<Params>
    {
        self.conn
            .drop_exec(query, params)
            .map(Transaction::new_raw)
    }

    /// Returns future that prepares and executes statement and resolves to `(Option<R>, Transaction)`.
    ///
    /// Where `Option<R>` is the first row of a statement execution result (if any).
    pub fn first_exec<R, Q, P>(self, query: Q, params: P) -> TransFirstExec<R>
        where R: FromRow,
              Q: AsRef<str>,
              P: Into<Params>
    {
        fn map<R: FromRow>((row, conn): (Option<R>, Conn)) -> (Option<R>, Transaction) {
            (row, Transaction::new_raw(conn))
        }

        self.conn.first_exec(query, params).map(map)
    }

    /// Returns future that prepares and executes statement and resolves to `Transaction`.
    ///
    /// All results will be dropped.
    pub fn batch_exec<Q, P>(self, query: Q, params_vec: Vec<P>) -> TransBatchExec
        where Q: AsRef<str>,
              P: Into<Params>
    {
        self.conn
            .batch_exec(query, params_vec)
            .map(Transaction::new_raw)
    }
}
