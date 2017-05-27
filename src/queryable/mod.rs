// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use BoxFuture;
use Column;
use Conn;
use Row;
use Value;
use connection_like::ConnectionLike;
use consts::{Command, CLIENT_DEPRECATE_EOF};
use errors::*;
use lib_futures::future::Future;
use proto::{Packet, PacketType};
use self::query_result::QueryResult;
use self::stmt::Stmt;
use self::transaction::{Transaction, TransactionOptions};
use std::sync::Arc;
use value::{FromRow, Params};

pub mod query_result;
pub mod stmt;
pub mod transaction;

pub trait Protocol {
    fn read_result_set_row(packet: &Packet, columns: Arc<Vec<Column>>) -> Result<Row>;
    fn is_last_result_set_packet<T>(conn_like: &T, packet: &Packet) -> bool
        where T: ConnectionLike;
}

pub struct TextProtocol;
pub struct BinaryProtocol;

impl Protocol for TextProtocol {
    fn read_result_set_row(packet: &Packet, columns: Arc<Vec<Column>>) -> Result<Row> {
        Value::from_payload(packet.as_ref(), columns.len()).map(|values| Row::new(values, columns))
    }

    fn is_last_result_set_packet<T>(conn_like: &T, packet: &Packet) -> bool
        where T: ConnectionLike
    {
        if conn_like.get_capabilities().contains(CLIENT_DEPRECATE_EOF) {
            packet.is(PacketType::Ok)
        } else {
            packet.is(PacketType::Eof)
        }
    }
}
impl Protocol for BinaryProtocol {
    fn read_result_set_row(packet: &Packet, columns: Arc<Vec<Column>>) -> Result<Row> {
        Value::from_bin_payload(packet.as_ref(), &columns)
            .map(|values| Row::new(values, columns))
    }

    fn is_last_result_set_packet<T>(_: &T, packet: &Packet) -> bool
        where T: ConnectionLike
    {
        packet.is(PacketType::Eof)
    }
}

/// Represents something queryable like connection or transaction.
pub trait Queryable:
    where Self: Sized + 'static
{
    /// Returns future that resolves to `Conn` if `COM_PING` executed successfully.
    fn ping(self) -> BoxFuture<Self>;

    /// Returns future that disconnects this connection from a server.
    fn disconnect(self) -> BoxFuture<()>;

    /// Returns future that performs `query`.
    fn query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<QueryResult<Self, TextProtocol>>;

    /// Returns future that resolves to a first row of result of a `query` execution (if any).
    ///
    /// Returned future will call `R::from_row(row)` internally.
    fn first<Q, R>(self, query: Q) -> BoxFuture<(Self, Option<R>)>
        where Q: AsRef<str>,
              R: FromRow;

    /// Returns future that performs query. Result will be dropped.
    fn drop_query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Self>;

    /// Returns future that prepares statement.
    fn prepare<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Stmt<Self>>;

    /// Returns future that prepares and executes statement in one pass.
    fn prep_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<QueryResult<Self, BinaryProtocol>>
        where Q: AsRef<str>, P: Into<Params>;

    /// Returns future that resolves to a first row of result of a statement execution (if any).
    ///
    /// Returned future will call `R::from_row(row)` internally.
    fn first_exec<Q, P, R>(self, query: Q, params: P) -> BoxFuture<(Self, Option<R>)>
        where Q: AsRef<str>,
              P: Into<Params>,
              R: FromRow;

    /// Returns future that prepares and executes statement. Result will be dropped.
    fn drop_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<Self>
        where Q: AsRef<str>,
              P: Into<Params>;

    /// Returns future that prepares statement and performs batch execution.
    /// Results will be dropped.
    fn batch_exec<Q, P>(self, query: Q, params_vec: Vec<P>) -> BoxFuture<Self>
        where Q: AsRef<str>,
              P: Into<Params>;

    /// Returns future that starts transaction.
    fn start_transaction(self, options: TransactionOptions) -> BoxFuture<Transaction<Self>>;
}

impl Queryable for Conn {
    fn ping(self) -> BoxFuture<Self> {
        let fut = self.write_command_data(Command::COM_PING, &[])
            .and_then(|this| this.read_packet())
            .map(|(this, _)| this);
        Box::new(fut)
    }

    fn disconnect(mut self) -> BoxFuture<()> {
        self.on_disconnect();
        let fut = self.write_command_data(Command::COM_QUIT, &[]).map(|_| ());
        Box::new(fut)
    }

    fn query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<QueryResult<Self, TextProtocol>> {
        let fut = self.write_command_data(Command::COM_QUERY, query.as_ref().as_bytes())
            .and_then(|conn_like| {
                conn_like.read_result_set()
            });
        Box::new(fut)
    }

    fn first<Q, R>(self, query: Q) -> BoxFuture<(Self, Option<R>)>
        where Q: AsRef<str>,
              R: FromRow,
    {
        let fut = self.query(query)
            .and_then(|result| result.collect_and_drop::<Row>())
            .map(|(this, mut rows)| if rows.len() > 1 {
                (this, Some(FromRow::from_row(rows.swap_remove(0))))
            } else {
                (this, rows.pop().map(FromRow::from_row))
            });
        Box::new(fut)
    }

    fn drop_query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Self> {
        let fut = self.query(query)
            .and_then(|result| result.drop_result());
        Box::new(fut)
    }

    fn prepare<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Stmt<Self>> {
        let fut = self.prepare_stmt(query)
            .map(|(this, inner_stmt)| stmt::new(this, inner_stmt));
        Box::new(fut)
    }

    fn prep_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<QueryResult<Self, BinaryProtocol>>
        where Q: AsRef<str>,
              P: Into<Params>
    {
        let params: Params = params.into();
        let fut = self.prepare(query)
            .and_then(|stmt| stmt.execute(params))
            .map(|result| {
                let (stmt, columns) = query_result::disassemble(result);
                let this = stmt.unwrap();
                query_result::assemble(this, columns)
            });
        Box::new(fut)
    }

    fn first_exec<Q, P, R>(self, query: Q, params: P) -> BoxFuture<(Self, Option<R>)>
        where Q: AsRef<str>,
              P: Into<Params>,
              R: FromRow
    {

        let fut = self.prep_exec(query, params)
            .and_then(|result| result.collect_and_drop::<Row>())
            .map(|(this, mut rows)| if rows.len() > 1 {
                (this, Some(FromRow::from_row(rows.swap_remove(0))))
            } else {
                (this, rows.pop().map(FromRow::from_row))
            });
        Box::new(fut)
    }

    fn drop_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<Self>
        where Q: AsRef<str>,
              P: Into<Params>
    {
        let fut = self.prep_exec(query, params)
            .and_then(|result| result.drop_result());
        Box::new(fut)
    }

    fn batch_exec<Q, P>(self, query: Q, params_vec: Vec<P>) -> BoxFuture<Self>
        where Q: AsRef<str>,
              P: Into<Params>
    {
        let params_vec: Vec<Params> = params_vec.into_iter().map(Into::into).collect();
        let fut = self.prepare(query)
            .and_then(|stmt| stmt.batch(params_vec))
            .map(|stmt| stmt.unwrap());
        Box::new(fut)
    }

    fn start_transaction(self, options: TransactionOptions) -> BoxFuture<Transaction<Self>> {
        transaction::new(self, options)
    }
}

impl<T: Queryable + ConnectionLike> Queryable for Transaction<T> {
    fn ping(self) -> BoxFuture<Self> {
        let fut = self.write_command_data(Command::COM_PING, &[])
            .and_then(|this| this.read_packet())
            .map(|(this, _)| this);
        Box::new(fut)
    }

    fn disconnect(mut self) -> BoxFuture<()> {
        self.on_disconnect();
        let fut = self.write_command_data(Command::COM_QUIT, &[]).map(|_| ());
        Box::new(fut)
    }

    fn query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<QueryResult<Self, TextProtocol>> {
        let fut = self.write_command_data(Command::COM_QUERY, query.as_ref().as_bytes())
            .and_then(|conn_like| {
                conn_like.read_result_set()
            });
        Box::new(fut)
    }

    fn first<Q, R>(self, query: Q) -> BoxFuture<(Self, Option<R>)>
        where Q: AsRef<str>,
              R: FromRow,
    {
        let fut = self.query(query)
            .and_then(|result| result.collect_and_drop::<Row>())
            .map(|(this, mut rows)| if rows.len() > 1 {
                (this, Some(FromRow::from_row(rows.swap_remove(0))))
            } else {
                (this, rows.pop().map(FromRow::from_row))
            });
        Box::new(fut)
    }

    fn drop_query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Self> {
        let fut = self.query(query)
            .and_then(|result| result.drop_result());
        Box::new(fut)
    }

    fn prepare<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Stmt<Self>> {
        let fut = self.prepare_stmt(query)
            .map(|(this, inner_stmt)| stmt::new(this, inner_stmt));
        Box::new(fut)
    }

    fn prep_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<QueryResult<Self, BinaryProtocol>>
        where Q: AsRef<str>,
              P: Into<Params>
    {
        let params: Params = params.into();
        let fut = self.prepare(query)
            .and_then(|stmt| stmt.execute(params))
            .map(|result| {
                let (stmt, columns) = query_result::disassemble(result);
                let this = stmt.unwrap();
                query_result::assemble(this, columns)
            });
        Box::new(fut)
    }

    fn first_exec<Q, P, R>(self, query: Q, params: P) -> BoxFuture<(Self, Option<R>)>
        where Q: AsRef<str>,
              P: Into<Params>,
              R: FromRow
    {

        let fut = self.prep_exec(query, params)
            .and_then(|result| result.collect_and_drop::<Row>())
            .map(|(this, mut rows)| if rows.len() > 1 {
                (this, Some(FromRow::from_row(rows.swap_remove(0))))
            } else {
                (this, rows.pop().map(FromRow::from_row))
            });
        Box::new(fut)
    }

    fn drop_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<Self>
        where Q: AsRef<str>,
              P: Into<Params>
    {
        let fut = self.prep_exec(query, params)
            .and_then(|result| result.drop_result());
        Box::new(fut)
    }

    fn batch_exec<Q, P>(self, query: Q, params_vec: Vec<P>) -> BoxFuture<Self>
        where Q: AsRef<str>,
              P: Into<Params>
    {
        let params_vec: Vec<Params> = params_vec.into_iter().map(Into::into).collect();
        let fut = self.prepare(query)
            .and_then(|stmt| stmt.batch(params_vec))
            .map(|stmt| stmt.unwrap());
        Box::new(fut)
    }

    fn start_transaction(self, options: TransactionOptions) -> BoxFuture<Transaction<Self>> {
        transaction::new(self, options)
    }
}
