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
use Params;
use Row;
use connection_like::ConnectionLike;
use consts::Command;
use errors::*;
use lib_futures::future::Future;
use myc::packets::{parse_ok_packet, RawPacket};
use myc::row::new_row;
use myc::value::{read_text_values, read_bin_values};
use prelude::FromRow;
use self::query_result::QueryResult;
use self::stmt::Stmt;
use self::transaction::{Transaction, TransactionOptions};
use std::sync::Arc;

pub mod query_result;
pub mod stmt;
pub mod transaction;

pub trait Protocol {
    fn read_result_set_row(packet: &RawPacket, columns: Arc<Vec<Column>>) -> Result<Row>;
    fn is_last_result_set_packet<T>(conn_like: &T, packet: &RawPacket) -> bool
    where
        T: ConnectionLike,
    {
        parse_ok_packet(&*packet.0, conn_like.get_capabilities()).is_ok()
    }
}

/// Phantom struct used to specify MySql text protocol.
pub struct TextProtocol;

/// Phantom struct used to specify MySql binary protocol.
pub struct BinaryProtocol;

impl Protocol for TextProtocol {
    fn read_result_set_row(packet: &RawPacket, columns: Arc<Vec<Column>>) -> Result<Row> {
        read_text_values(&*packet.0, columns.len())
            .map(|values| new_row(values, columns))
            .map_err(Into::into)
    }
}
impl Protocol for BinaryProtocol {
    fn read_result_set_row(packet: &RawPacket, columns: Arc<Vec<Column>>) -> Result<Row> {
        read_bin_values(&*packet.0, &*columns)
            .map(|values| new_row(values, columns))
            .map_err(Into::into)
    }

    fn is_last_result_set_packet<T>(conn_like: &T, packet: &RawPacket) -> bool
    where
        T: ConnectionLike,
    {
        (parse_ok_packet(&*packet.0, conn_like.get_capabilities()).is_ok() && packet.0[0] == 0xFE)
    }
}

/// Represents something queryable like connection or transaction.
pub trait Queryable: ConnectionLike
where
    Self: Sized + 'static,
{
    /// Returns future that resolves to `Conn` if `COM_PING` executed successfully.
    fn ping(self) -> BoxFuture<Self> {
        let fut = self.write_command_data(Command::COM_PING, &[])
            .and_then(|this| this.read_packet())
            .map(|(this, _)| this);
        Box::new(fut)
    }

    /// Returns future that disconnects this connection from a server.
    fn disconnect(mut self) -> BoxFuture<()> {
        self.on_disconnect();
        let fut = self.write_command_data(Command::COM_QUIT, &[]).map(|_| ());
        Box::new(fut)
    }

    /// Returns future that performs `query`.
    fn query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<QueryResult<Self, TextProtocol>> {
        let fut = self.write_command_data(Command::COM_QUERY, query.as_ref().as_bytes())
            .and_then(|conn_like| conn_like.read_result_set(None));
        Box::new(fut)
    }

    /// Returns future that resolves to a first row of result of a `query` execution (if any).
    ///
    /// Returned future will call `R::from_row(row)` internally.
    fn first<Q, R>(self, query: Q) -> BoxFuture<(Self, Option<R>)>
    where
        Q: AsRef<str>,
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

    /// Returns future that performs query. Result will be dropped.
    fn drop_query<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Self> {
        let fut = self.query(query).and_then(|result| result.drop_result());
        Box::new(fut)
    }

    /// Returns future that prepares statement.
    fn prepare<Q: AsRef<str>>(self, query: Q) -> BoxFuture<Stmt<Self>> {
        let fut = self.prepare_stmt(query).map(|(this,
          inner_stmt,
          stmt_cache_result)| {
            stmt::new(this, inner_stmt, stmt_cache_result)
        });
        Box::new(fut)
    }

    /// Returns future that prepares and executes statement in one pass.
    fn prep_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<QueryResult<Self, BinaryProtocol>>
    where
        Q: AsRef<str>,
        P: Into<Params>,
    {
        let params: Params = params.into();
        let fut = self.prepare(query)
            .and_then(|stmt| stmt.execute(params))
            .map(|result| {
                let (stmt, columns, _) = query_result::disassemble(result);
                let (conn_like, cached) = stmt.unwrap();
                query_result::assemble(conn_like, columns, cached)
            });
        Box::new(fut)
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

        let fut = self.prep_exec(query, params)
            .and_then(|result| result.collect_and_drop::<Row>())
            .map(|(this, mut rows)| if rows.len() > 1 {
                (this, Some(FromRow::from_row(rows.swap_remove(0))))
            } else {
                (this, rows.pop().map(FromRow::from_row))
            });
        Box::new(fut)
    }

    /// Returns future that prepares and executes statement. Result will be dropped.
    fn drop_exec<Q, P>(self, query: Q, params: P) -> BoxFuture<Self>
    where
        Q: AsRef<str>,
        P: Into<Params>,
    {
        let fut = self.prep_exec(query, params).and_then(
            |result| result.drop_result(),
        );
        Box::new(fut)
    }

    /// Returns future that prepares statement and performs batch execution.
    /// Results will be dropped.
    fn batch_exec<Q, I, P>(self, query: Q, params_iter: I) -> BoxFuture<Self>
    where
        Q: AsRef<str>,
        I: IntoIterator<Item = P> + 'static,
        Params: From<P>,
        P: 'static,
    {
        let fut = self.prepare(query)
            .and_then(|stmt| stmt.batch(params_iter))
            .and_then(|stmt| stmt.close());
        Box::new(fut)
    }

    /// Returns future that starts transaction.
    fn start_transaction(self, options: TransactionOptions) -> BoxFuture<Transaction<Self>> {
        transaction::new(self, options)
    }
}

impl Queryable for Conn {}
impl<T: Queryable + ConnectionLike> Queryable for Transaction<T> {}
