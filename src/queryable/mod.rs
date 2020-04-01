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
    value::{read_bin_values, read_text_values, ServerSide},
};

use std::sync::Arc;

use self::{
    query_result::QueryResult,
    stmt::Statement,
    transaction::{Transaction, TxStatus},
};
use crate::{
    connection_like::ConnectionLike,
    consts::Command,
    error::*,
    prelude::FromRow,
    queryable::{
        query_result::{read_result_set, ResultSetMeta},
        stmt::StatementLike,
    },
    BoxFuture, Column, Conn, Params, Row,
};

pub mod query_result;
pub mod stmt;
pub mod transaction;

pub trait Protocol: Send + 'static {
    /// Returns `ResultSetMeta`, that corresponds to the current protocol.
    fn result_set_meta(columns: Arc<Vec<Column>>) -> ResultSetMeta;
    fn read_result_set_row(packet: &[u8], columns: Arc<Vec<Column>>) -> Result<Row>;
    fn is_last_result_set_packet<T>(conn_like: &T, packet: &[u8]) -> bool
    where
        T: ConnectionLike,
    {
        parse_ok_packet(
            packet,
            conn_like.conn_ref().capabilities(),
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
    fn result_set_meta(columns: Arc<Vec<Column>>) -> ResultSetMeta {
        ResultSetMeta::Text(columns)
    }

    fn read_result_set_row(packet: &[u8], columns: Arc<Vec<Column>>) -> Result<Row> {
        read_text_values(packet, columns.len())
            .map(|values| new_row(values, columns))
            .map_err(Into::into)
    }
}

impl Protocol for BinaryProtocol {
    fn result_set_meta(columns: Arc<Vec<Column>>) -> ResultSetMeta {
        ResultSetMeta::Binary(columns)
    }

    fn read_result_set_row(packet: &[u8], columns: Arc<Vec<Column>>) -> Result<Row> {
        read_bin_values::<ServerSide>(packet, &*columns)
            .map(|values| new_row(values, columns))
            .map_err(Into::into)
    }
}

/// The only purpose of this function at the moment is to rollback a transaction in cases,
/// where `Transaction` is dropped without an explicit call to `commit` or `rollback`.
async fn cleanup<T: Queryable>(queryable: &mut T) -> Result<()> {
    queryable.conn_mut().drop_result().await?;
    if queryable.conn_ref().get_tx_status() == TxStatus::RequiresRollback {
        queryable.conn_mut().set_tx_status(TxStatus::None);
        queryable.exec_drop("ROLLBACK", ()).await?;
    }
    Ok(())
}

pub trait Queryable: crate::prelude::ConnectionLike {
    /// Executes `COM_PING`.
    fn ping(&mut self) -> BoxFuture<'_, ()> {
        BoxFuture(Box::pin(async move {
            cleanup(self).await?;
            self.write_command_raw(vec![Command::COM_PING as u8])
                .await?;
            self.read_packet().await?;
            Ok(())
        }))
    }

    /// Performs the given query.
    fn query_iter<'a, Q>(
        &'a mut self,
        query: Q,
    ) -> BoxFuture<'a, QueryResult<'a, Self, TextProtocol>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
    {
        BoxFuture(Box::pin(async move {
            cleanup(self).await?;
            self.write_command_data(Command::COM_QUERY, query.as_ref().as_bytes())
                .await?;
            read_result_set(self).await
        }))
    }

    /// Prepares the given statement.
    fn prep<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Statement>
    where
        Q: AsRef<str> + Sync + Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            cleanup(self).await?;
            self.conn_mut().get_statement(query.as_ref()).await
        }))
    }

    /// Closes the given statement.
    fn close(&mut self, stmt: Statement) -> BoxFuture<'_, ()> {
        BoxFuture(Box::pin(async move {
            cleanup(self).await?;
            self.conn_mut().stmt_cache_mut().remove(stmt.id());
            self.conn_mut().close_statement(stmt.id()).await
        }))
    }

    /// Executes the given statement with the given params.
    fn exec_iter<'a: 'b, 'b, Q, P>(
        &'a mut self,
        stmt: &'b Q,
        params: P,
    ) -> BoxFuture<'b, QueryResult<'a, crate::Conn, BinaryProtocol>>
    where
        Q: StatementLike + ?Sized + 'a,
        P: Into<Params>,
    {
        let params = params.into();
        BoxFuture(Box::pin(async move {
            cleanup(self).await?;
            let statement = self.conn_mut().get_statement(stmt).await?;
            self.conn_mut().execute_statement(&statement, params).await
        }))
    }

    /// Performs the given query and collects the first result set.
    fn query<'a, T, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Vec<T>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            self.query_iter(query).await?.collect_and_drop::<T>().await
        }))
    }

    /// Performs the given query and returns the firt row of the first result set.
    fn query_first<'a, T, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Option<T>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            let mut result = self.query_iter(query).await?;
            let output = if result.is_empty() {
                None
            } else {
                result.get_row().await?.map(crate::from_row)
            };
            result.drop_result().await?;
            Ok(output)
        }))
    }

    /// Performs the given query and returns the firt row of the first result set.
    fn query_map<'a, T, F, Q, U>(&'a mut self, query: Q, mut f: F) -> BoxFuture<'a, Vec<U>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
        F: FnMut(T) -> U + Send + 'a,
        U: Send,
    {
        BoxFuture(Box::pin(async move {
            self.query_fold(query, Vec::new(), |mut acc, row| {
                acc.push(f(crate::from_row(row)));
                acc
            })
            .await
        }))
    }

    /// Performs the given query and folds the first result set to a single value.
    fn query_fold<'a, T, F, Q, U>(&'a mut self, query: Q, init: U, mut f: F) -> BoxFuture<'a, U>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
        F: FnMut(U, T) -> U + Send + 'a,
        U: Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            self.query_iter(query)
                .await?
                .reduce_and_drop(init, |acc, row| f(acc, crate::from_row(row)))
                .await
        }))
    }

    /// Performs the given query and drops the query result.
    fn query_drop<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, ()>
    where
        Q: AsRef<str> + Send + Sync + 'a,
    {
        BoxFuture(Box::pin(async move {
            self.query_iter(query).await?.drop_result().await
        }))
    }

    /// Prepares the given statement, and exectues it with each item in the given params iterator.
    fn exec_batch<'a: 'b, 'b, S, P, I>(
        &'a mut self,
        stmt: &'b S,
        params_iter: I,
    ) -> BoxFuture<'b, ()>
    where
        S: StatementLike + ?Sized + 'b,
        I: IntoIterator<Item = P> + Send + 'b,
        I::IntoIter: Send,
        P: Into<Params> + Send,
    {
        BoxFuture(Box::pin(async move {
            cleanup(self).await?;
            let statement = self.conn_mut().get_statement(stmt).await?;
            for params in params_iter {
                self.conn_mut()
                    .execute_statement(&statement, params)
                    .await?
                    .drop_result()
                    .await?;
            }
            Ok(())
        }))
    }

    /// Exectues the given statement and collects the first result set.
    fn exec<'a: 'b, 'b, T, S, P>(&'a mut self, stmt: &'b S, params: P) -> BoxFuture<'b, Vec<T>>
    where
        S: StatementLike + ?Sized + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            self.exec_iter(stmt, params)
                .await?
                .collect_and_drop::<T>()
                .await
        }))
    }

    /// Exectues the given statement and returns the first row of the first result set.
    fn exec_first<'a: 'b, 'b, T, S, P>(
        &'a mut self,
        stmt: &'b S,
        params: P,
    ) -> BoxFuture<'b, Option<T>>
    where
        S: StatementLike + ?Sized + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            let mut result = self.exec_iter(stmt, params).await?;
            let row = if result.is_empty() {
                None
            } else {
                result.get_row().await?
            };
            result.drop_result().await?;
            Ok(row.map(crate::from_row))
        }))
    }

    /// Exectues the given stmt and folds the first result set to a signel value.
    fn exec_map<'a: 'b, 'b, T, S, P, U, F>(
        &'a mut self,
        stmt: &'b S,
        params: P,
        mut f: F,
    ) -> BoxFuture<'b, Vec<U>>
    where
        S: StatementLike + ?Sized + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
        F: FnMut(T) -> U + Send + 'a,
        U: Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            self.exec_fold(stmt, params, Vec::new(), |mut acc, row| {
                acc.push(f(crate::from_row(row)));
                acc
            })
            .await
        }))
    }

    /// Exectues the given stmt and folds the first result set to a signel value.
    fn exec_fold<'a: 'b, 'b, T, S, P, U, F>(
        &'a mut self,
        stmt: &'b S,
        params: P,
        init: U,
        mut f: F,
    ) -> BoxFuture<'b, U>
    where
        S: StatementLike + ?Sized + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
        F: FnMut(U, T) -> U + Send + 'a,
        U: Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            self.exec_iter(stmt, params)
                .await?
                .reduce_and_drop(init, |acc, row| f(acc, crate::from_row(row)))
                .await
        }))
    }

    /// Exectues the given statement and drops the result.
    fn exec_drop<'a: 'b, 'b, S, P>(&'a mut self, stmt: &'b S, params: P) -> BoxFuture<'b, ()>
    where
        S: StatementLike + ?Sized + 'b,
        P: Into<Params> + Send + 'b,
    {
        BoxFuture(Box::pin(async move {
            self.exec_iter(stmt, params).await?.drop_result().await
        }))
    }
}

impl Queryable for Conn {}
impl<T: Queryable> Queryable for Transaction<'_, T> {}

#[cfg(test)]
mod tests {
    use super::Queryable;
    use crate::{error::Result, prelude::*, test_misc::get_opts, Conn};

    #[tokio::test]
    async fn should_prep() -> Result<()> {
        const NAMED: &str = "SELECT :foo, :bar, :foo";
        const POSITIONAL: &str = "SELECT ?, ?, ?";

        let mut conn = Conn::new(get_opts()).await?;

        let stmt_named = conn.prep(NAMED).await?;
        let stmt_positional = conn.prep(POSITIONAL).await?;

        let result_stmt_named: Option<(String, u8, String)> = conn
            .exec_first(&stmt_named, params! { "foo" => "bar", "bar" => 42 })
            .await?;
        let result_str_named: Option<(String, u8, String)> = conn
            .exec_first(NAMED, params! { "foo" => "bar", "bar" => 42 })
            .await?;

        let result_stmt_positional: Option<(String, u8, String)> = conn
            .exec_first(&stmt_positional, ("bar", 42, "bar"))
            .await?;
        let result_str_positional: Option<(String, u8, String)> =
            conn.exec_first(NAMED, ("bar", 42, "bar")).await?;

        assert_eq!(
            Some(("bar".to_owned(), 42_u8, "bar".to_owned())),
            result_stmt_named
        );
        assert_eq!(result_stmt_named, result_str_named);
        assert_eq!(result_str_named, result_stmt_positional);
        assert_eq!(result_stmt_positional, result_str_positional);

        conn.disconnect().await?;

        Ok(())
    }
}
