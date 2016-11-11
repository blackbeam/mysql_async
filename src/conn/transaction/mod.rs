use conn::Conn;
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
    fn new(mut conn: Conn) -> Transaction {
        conn.in_transaction = true;
        Transaction {
            conn: conn,
        }
    }
}
