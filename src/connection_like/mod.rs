// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::{BoxFuture, Pool};

pub trait ConnectionLike: Send + Sized {
    fn conn_ref(&self) -> &crate::Conn;
    fn conn_mut(&mut self) -> &mut crate::Conn;
}

/// Connection.
#[derive(Debug)]
pub enum Connection<'a, 't: 'a> {
    Conn(crate::Conn),
    ConnMut(&'a mut crate::Conn),
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

impl ConnectionLike for Connection<'_, '_> {
    fn conn_ref(&self) -> &crate::Conn {
        match self {
            Connection::Conn(ref conn) => conn,
            Connection::ConnMut(conn) => conn,
            Connection::Tx(tx) => tx.conn_ref(),
        }
    }
    fn conn_mut(&mut self) -> &mut crate::Conn {
        match self {
            Connection::Conn(conn) => conn,
            Connection::ConnMut(conn) => conn,
            Connection::Tx(tx) => tx.conn_mut(),
        }
    }
}

pub enum ToConnectionResult<'a, 't: 'a> {
    Immediate(Connection<'a, 't>),
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

impl<'a> ToConnection<'a, 'static> for &'a Pool {
    fn to_connection(self) -> ToConnectionResult<'a, 'static> {
        let fut = BoxFuture(Box::pin(async move {
            let conn = self.get_conn().await?;
            Ok(conn.into())
        }));
        ToConnectionResult::Mediate(fut)
    }
}
