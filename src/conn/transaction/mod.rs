// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::Query;
use conn::futures::query_result::futures::DropResult;
use conn::futures::query_result::TextQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use lib_futures::Future;
use lib_futures::Map;
use self::futures::*;
use std::fmt;

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

pub struct Transaction {
    conn: Conn,
}

impl Transaction {
    /// Creates transaction
    fn new(mut conn: Conn) -> Transaction {
        conn.in_transaction = true;
        Transaction {
            conn: conn,
        }
    }

    /// Cleans connection
    fn clean_conn(mut conn: Conn) -> Conn {
        conn.in_transaction = false;
        conn
    }

    /// Will create transaction without affecting conn.in_transaction
    fn new_raw(conn: Conn) -> Transaction {
        Transaction {
            conn: conn,
        }
    }

    /// Returns future that commits transaction and resolves to `Conn`.
    pub fn commit(self) -> Commit {
        new_commit(self)
    }

    /// Returns future that rolls back transaction and resolves to `Conn`.
    pub fn rollback(self) -> Rollback {
        new_rollback(self)
    }
}
