// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_util::FutureExt;

use crate::{BoxFuture, Pool};

/// Connection.
#[derive(Debug)]
pub enum Connection<'a, 't: 'a> {
    /// Just a connection.
    Conn(crate::Conn),
    /// Mutable reference to a connection.
    ConnMut(&'a mut crate::Conn),
    /// Connection wrapped in a transaction.
    Tx(&'a mut crate::Transaction<'t>),
}

impl From<crate::Conn> for Connection<'static, 'static> {
    fn from(conn: crate::Conn) -> Self {
        Connection::Conn(conn)
    }
}

impl<'a> From<&'a mut crate::Conn> for Connection<'a, 'static> {
    fn from(conn: &'a mut crate::Conn) -> Self {
        Connection::ConnMut(conn)
    }
}

impl<'a, 't> From<&'a mut crate::Transaction<'t>> for Connection<'a, 't> {
    fn from(tx: &'a mut crate::Transaction<'t>) -> Self {
        Connection::Tx(tx)
    }
}

impl std::ops::Deref for Connection<'_, '_> {
    type Target = crate::Conn;

    fn deref(&self) -> &Self::Target {
        match self {
            Connection::Conn(ref conn) => conn,
            Connection::ConnMut(conn) => conn,
            Connection::Tx(tx) => tx.0.deref(),
        }
    }
}

impl std::ops::DerefMut for Connection<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Connection::Conn(conn) => conn,
            Connection::ConnMut(conn) => conn,
            Connection::Tx(tx) => tx.0.deref_mut(),
        }
    }
}

/// Result of `ToConnection::to_connection` call.
pub enum ToConnectionResult<'a, 't: 'a> {
    /// Connection is immediately available.
    Immediate(Connection<'a, 't>),
    /// We need some time to get a connection and the operation itself may fail.
    Mediate(BoxFuture<'a, Connection<'a, 't>>),
}

pub trait ToConnection<'a, 't: 'a>: Send {
    fn to_connection(self) -> ToConnectionResult<'a, 't>;
}

impl<'a, 't: 'a, T: Into<Connection<'a, 't>> + Send> ToConnection<'a, 't> for T {
    fn to_connection(self) -> ToConnectionResult<'a, 't> {
        ToConnectionResult::Immediate(self.into())
    }
}

impl ToConnection<'static, 'static> for Pool {
    fn to_connection(self) -> ToConnectionResult<'static, 'static> {
        let fut = async move {
            let conn = self.get_conn().await?;
            Ok(conn.into())
        }
        .boxed();
        ToConnectionResult::Mediate(fut)
    }
}

impl<'a> ToConnection<'a, 'static> for &'a Pool {
    fn to_connection(self) -> ToConnectionResult<'a, 'static> {
        let fut = async move {
            let conn = self.get_conn().await?;
            Ok(conn.into())
        }
        .boxed();
        ToConnectionResult::Mediate(fut)
    }
}
