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
pub enum Connection<'a> {
    Conn(crate::Conn),
    ConnMut(&'a mut crate::Conn),
}

impl From<crate::Conn> for Connection<'_> {
    fn from(conn: crate::Conn) -> Self {
        Connection::Conn(conn)
    }
}

impl<'a> From<&'a mut crate::Conn> for Connection<'a> {
    fn from(conn: &'a mut crate::Conn) -> Self {
        Connection::ConnMut(conn)
    }
}

impl ConnectionLike for Connection<'_> {
    fn conn_ref(&self) -> &crate::Conn {
        match self {
            Connection::Conn(ref conn) => conn,
            Connection::ConnMut(conn) => conn,
        }
    }
    fn conn_mut(&mut self) -> &mut crate::Conn {
        match self {
            Connection::Conn(conn) => conn,
            Connection::ConnMut(conn) => conn,
        }
    }
}

pub enum ToConnectionResult<'a> {
    Immediate(Connection<'a>),
    Mediate(BoxFuture<'a, Connection<'a>>),
}

pub trait ToConnection<'a>: Send {
    fn to_connection(self) -> ToConnectionResult<'a>;
}

impl<'a, T: Into<Connection<'a>> + Send> ToConnection<'a> for T {
    fn to_connection(self) -> ToConnectionResult<'a> {
        ToConnectionResult::Immediate(self.into())
    }
}

impl<'a> ToConnection<'a> for &'a Pool {
    fn to_connection(self) -> ToConnectionResult<'a> {
        let fut = BoxFuture(Box::pin(async move {
            let conn = self.get_conn().await?;
            Ok(conn.into())
        }));
        ToConnectionResult::Mediate(fut)
    }
}
