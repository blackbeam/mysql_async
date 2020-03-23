// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::{
    packets::parse_ok_packet,
    row::new_row,
    value::{read_bin_values, read_text_values},
};

use std::sync::Arc;

use self::{
    query_result::QueryResult,
    stmt::Stmt,
    transaction::{Transaction, TransactionOptions},
};
use crate::{
    connection_like::ConnectionLike, consts::Command, error::*, prelude::FromRow, BoxFuture,
    Column, Conn, Params, Row,
};
use mysql_common::value::ServerSide;

pub mod query_result;
pub mod stmt;
pub mod transaction;

pub trait Protocol: Send + 'static {
    fn read_result_set_row(packet: &[u8], columns: Arc<Vec<Column>>) -> Result<Row>;
    fn is_last_result_set_packet<T>(conn_like: &T, packet: &[u8]) -> bool
    where
        T: ConnectionLike,
    {
        parse_ok_packet(packet, conn_like.get_capabilities()).is_ok()
    }
}

/// Phantom struct used to specify MySql text protocol.
pub struct TextProtocol;

/// Phantom struct used to specify MySql binary protocol.
pub struct BinaryProtocol;

impl Protocol for TextProtocol {
    fn read_result_set_row(packet: &[u8], columns: Arc<Vec<Column>>) -> Result<Row> {
        read_text_values(packet, columns.len())
            .map(|values| new_row(values, columns))
            .map_err(Into::into)
    }
}
impl Protocol for BinaryProtocol {
    fn read_result_set_row(packet: &[u8], columns: Arc<Vec<Column>>) -> Result<Row> {
        read_bin_values::<ServerSide>(packet, &*columns)
            .map(|values| new_row(values, columns))
            .map_err(Into::into)
    }

    fn is_last_result_set_packet<T>(conn_like: &T, packet: &[u8]) -> bool
    where
        T: ConnectionLike,
    {
        parse_ok_packet(packet, conn_like.get_capabilities()).is_ok()
            && packet.get(0).cloned() == Some(0xFE)
    }
}

/// Represents something queryable like connection or transaction.
pub trait Queryable: ConnectionLike
where
    Self: Sized + 'static,
{
    /// Returns future that resolves to `Conn` if `COM_PING` executed successfully.
    fn ping(self) -> BoxFuture<Self> {
        Box::pin(async move {
            Ok(self
                .write_command_data(Command::COM_PING, &[])
                .await?
                .read_packet()
                .await?
                .0)
        })
    }

    /// Returns future, that disconnects this connection from a server.
    fn disconnect(mut self) -> BoxFuture<()> {
        self.on_disconnect();
        let f = self.write_command_data(Command::COM_QUIT, &[]);
        Box::pin(async move {
            let (_, stream) = f.await?.take_stream();
            stream.close().await?;
            Ok(())
        })
    }

    /// Returns future that performs `query`.
    fn query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<QueryResult<Self, TextProtocol>> {
        let f = self.write_command_data(Command::COM_QUERY, query.as_ref().as_bytes());
        Box::pin(async move { f.await?.read_result_set(None).await })
    }

    /// Returns future that resolves to a first row of result of a `query` execution (if any).
    ///
    /// Returned future will call `R::from_row(row)` internally.
    fn first<Q, R>(self, query: Q) -> BoxFuture<(Self, Option<R>)>
    where
        Q: AsRef<str>,
        R: FromRow,
    {
        let f = self.query(query);
        Box::pin(async move {
            let (this, mut rows) = f.await?.collect_and_drop::<Row>().await?;
            if rows.len() > 1 {
                Ok((this, Some(FromRow::from_row(rows.swap_remove(0)))))
            } else {
                Ok((this, rows.pop().map(FromRow::from_row)))
            }
        })
    }

    /// Returns future that performs query. Result will be dropped.
    fn drop_query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Self> {
        let f = self.query(query);
        Box::pin(async move { f.await?.drop_result().await })
    }

    /// Returns future that prepares statement.
    fn prepare<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Stmt<Self>> {
        let f = self.prepare_stmt(query);
        Box::pin(async move {
            let (this, inner_stmt, stmt_cache_result) = f.await?;
            Ok(stmt::new(this, inner_stmt, stmt_cache_result))
        })
    }

    /// Returns future that prepares and executes statement in one pass.
    fn prep_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<QueryResult<Self, BinaryProtocol>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
    {
        let params: Params = params.into();
        let f = self.prepare(query);
        Box::pin(async move {
            let result = f.await?.execute(params).await?;
            let (stmt, columns, _) = query_result::disassemble(result);
            let (conn_like, cached) = stmt.unwrap();
            Ok(query_result::assemble(conn_like, columns, cached))
        })
    }

    /// Returns future that resolves to a first row of result of a statement execution (if any).
    ///
    /// Returned future will call `R::from_row(row)` internally.
    fn first_exec<Q, P, R>(self, query: Q, params: P) -> BoxFuture<(Self, Option<R>)>
    where
        Q: AsRef<str>,
        P: Into<Params>,
        R: FromRow,
    {
        let f = self.prep_exec(query, params);
        Box::pin(async move {
            let (this, mut rows) = f.await?.collect_and_drop::<Row>().await?;
            if rows.len() > 1 {
                Ok((this, Some(FromRow::from_row(rows.swap_remove(0)))))
            } else {
                Ok((this, rows.pop().map(FromRow::from_row)))
            }
        })
    }

    /// Returns future that prepares and executes statement. Result will be dropped.
    fn drop_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<Self>
    where
        Q: AsRef<str>,
        P: Into<Params>,
    {
        let f = self.prep_exec(query, params);
        Box::pin(async move { f.await?.drop_result().await })
    }

    /// Returns future that prepares statement and performs batch execution.
    /// Results will be dropped.
    fn batch_exec<Q, I, P>(self, query: Q, params_iter: I) -> BoxFuture<Self>
    where
        Q: AsRef<str>,
        I: IntoIterator<Item = P> + Send + 'static,
        I::IntoIter: Send + 'static,
        Params: From<P>,
        P: Send + 'static,
    {
        let f = self.prepare(query);
        Box::pin(async move { f.await?.batch(params_iter).await?.close().await })
    }

    /// Returns future that starts transaction.
    fn start_transaction(self, options: TransactionOptions) -> BoxFuture<Transaction<Self>> {
        Box::pin(transaction::new(self, options))
    }
}

impl Queryable for Conn {}
impl<T: Queryable + ConnectionLike> Queryable for Transaction<T> {}
