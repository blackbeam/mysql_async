// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use mysql_common::{
    packets::{parse_ok_packet, OkPacketKind},
    row::new_row,
    value::{read_bin_values, read_text_values},
};

use std::sync::Arc;

use self::{
    query_result::QueryResult,
    stmt::Stmt,
    transaction::{Transaction, TransactionOptions, TxStatus},
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
        parse_ok_packet(
            packet,
            conn_like.get_capabilities(),
            OkPacketKind::ResultSetTerminator,
        )
        .is_ok()
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
}

/// The only purpose of this function at the moment is to rollback a transaction in cases,
/// where `Transaction` is dropped without an explicit call to `commit` or `rollback`.
async fn cleanup<T: Queryable + Sized>(queryable: &mut T) -> Result<()> {
    if queryable.get_tx_status() == TxStatus::RequiresRollback {
        queryable.set_tx_status(TxStatus::None);
        queryable.drop_query("ROLLBACK").await?;
    }
    Ok(())
}

/// Represents something queryable, e.g. connection or transaction.
pub trait Queryable: crate::prelude::ConnectionLike
where
    Self: Sized,
{
    /// Returns a future, that executes `COM_PING`.
    fn ping(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            cleanup(self).await?;
            self.write_command_data(Command::COM_PING, &[]).await?;
            self.read_packet().await?;
            Ok(())
        })
    }

    /// Returns a future that performs the given query.
    fn query<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, QueryResult<'a, Self, TextProtocol>>
    where
        Q: AsRef<str> + Sync + Send + 'static,
    {
        Box::pin(async move {
            cleanup(self).await?;
            self.write_command_data(Command::COM_QUERY, query.as_ref().as_bytes())
                .await?;
            self.read_result_set(None).await
        })
    }

    /// Returns a future that executes the given query and returns the first row (if any).
    ///
    /// Returned future will call `R::from_row(row)` internally.
    fn first<'a, Q, R>(&'a mut self, query: Q) -> BoxFuture<'a, Option<R>>
    where
        Q: AsRef<str> + Sync + Send + 'static,
        R: FromRow,
    {
        Box::pin(async move {
            let result = self.query(query).await?;
            let mut rows = result.collect_and_drop::<Row>().await?;
            if rows.len() > 1 {
                Ok(Some(FromRow::from_row(rows.swap_remove(0))))
            } else {
                Ok(rows.pop().map(FromRow::from_row))
            }
        })
    }

    /// Returns a future that performs the given query. Result will be dropped.
    fn drop_query<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, ()>
    where
        Q: AsRef<str> + Sync + Send + 'static,
    {
        Box::pin(async move {
            let result = self.query(query).await?;
            result.drop_result().await?;
            Ok(())
        })
    }

    /// Returns a future that prepares the given statement.
    fn prepare<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Stmt<'a, Self>>
    where
        Q: AsRef<str> + Send + 'static,
    {
        Box::pin(async move {
            cleanup(self).await?;
            let f = self.prepare_stmt(query);
            let (inner_stmt, stmt_cache_result) = f.await?;
            Ok(Stmt::new(self, inner_stmt, stmt_cache_result))
        })
    }

    /// Returns a future that prepares and executes the given statement in one pass.
    fn prep_exec<'a, Q, P>(
        &'a mut self,
        query: Q,
        params: P,
    ) -> BoxFuture<'a, QueryResult<'a, Self, BinaryProtocol>>
    where
        Q: AsRef<str> + Send + 'static,
        P: Into<Params>,
    {
        let params: Params = params.into();
        Box::pin(async move {
            let mut stmt = self.prepare(query).await?;
            let result = stmt.execute(params).await?;
            let (stmt, columns, _) = result.disassemble();
            let cached = stmt.cached.clone();
            Ok(QueryResult::new(self, columns, cached))
        })
    }

    /// Returns a future that prepares and executes the given statement,
    /// and resolves to the first row (if any).
    ///
    /// Returned future will call `R::from_row(row)` internally.
    fn first_exec<Q, P, R>(&mut self, query: Q, params: P) -> BoxFuture<'_, Option<R>>
    where
        Q: AsRef<str> + Sync + Send + 'static,
        P: Into<Params>,
        R: FromRow,
    {
        let params = params.into();
        Box::pin(async move {
            let mut rows = self
                .prep_exec(query, params)
                .await?
                .collect_and_drop::<Row>()
                .await?;
            if rows.len() > 1 {
                Ok(Some(FromRow::from_row(rows.swap_remove(0))))
            } else {
                Ok(rows.pop().map(FromRow::from_row))
            }
        })
    }

    /// Returns a future that prepares and executes the given statement. Result will be dropped.
    fn drop_exec<Q, P>(&mut self, query: Q, params: P) -> BoxFuture<'_, ()>
    where
        Q: AsRef<str> + Send + 'static,
        P: Into<Params>,
    {
        let f = self.prep_exec(query, params);
        Box::pin(async move { f.await?.drop_result().await })
    }

    /// Returns a future that prepares the given statement and performs batch execution using
    /// the given params. Results will be dropped.
    fn batch_exec<Q, I, P>(&mut self, query: Q, params_iter: I) -> BoxFuture<'_, ()>
    where
        Q: AsRef<str> + Sync + Send + 'static,
        I: IntoIterator<Item = P>,
        I::IntoIter: Send + 'static,
        Params: From<P>,
    {
        let params_iter = params_iter.into_iter();
        Box::pin(async move {
            let mut stmt = self.prepare(query).await?;
            stmt.batch(params_iter).await?;
            stmt.close().await
        })
    }

    /// Returns a future that starts a transaction.
    fn start_transaction<'a>(
        &'a mut self,
        options: TransactionOptions,
    ) -> BoxFuture<'a, Transaction<'a, Self>> {
        Box::pin(async move {
            cleanup(self).await?;
            Transaction::new(self, options).await
        })
    }
}

impl Queryable for Conn {}
impl<'a, T: Queryable + crate::prelude::ConnectionLike> Queryable for Transaction<'a, T> {}
