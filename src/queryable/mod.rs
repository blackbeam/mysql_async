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

use std::{fmt, sync::Arc};

use self::{
    query_result::QueryResult,
    stmt::Statement,
    transaction::{Transaction, TxStatus},
};

use crate::{
    conn::routines::{PingRoutine, QueryRoutine},
    consts::CapabilityFlags,
    error::*,
    prelude::{FromRow, StatementLike},
    queryable::query_result::ResultSetMeta,
    BoxFuture, Column, Conn, Params, Row,
};

pub mod query_result;
pub mod stmt;
pub mod transaction;

pub trait Protocol: fmt::Debug + Send + Sync + 'static {
    /// Returns `ResultSetMeta`, that corresponds to the current protocol.
    fn result_set_meta(columns: Arc<[Column]>) -> ResultSetMeta;
    fn read_result_set_row(packet: &[u8], columns: Arc<[Column]>) -> Result<Row>;
    fn is_last_result_set_packet(capabilities: CapabilityFlags, packet: &[u8]) -> bool {
        parse_ok_packet(packet, capabilities, OkPacketKind::ResultSetTerminator).is_ok()
    }
}

/// Phantom struct used to specify MySql text protocol.
#[derive(Debug)]
pub struct TextProtocol;

/// Phantom struct used to specify MySql binary protocol.
#[derive(Debug)]
pub struct BinaryProtocol;

impl Protocol for TextProtocol {
    fn result_set_meta(columns: Arc<[Column]>) -> ResultSetMeta {
        ResultSetMeta::Text(columns)
    }

    fn read_result_set_row(packet: &[u8], columns: Arc<[Column]>) -> Result<Row> {
        read_text_values(packet, columns.len())
            .map(|values| new_row(values, columns))
            .map_err(Into::into)
    }
}

impl Protocol for BinaryProtocol {
    fn result_set_meta(columns: Arc<[Column]>) -> ResultSetMeta {
        ResultSetMeta::Binary(columns)
    }

    fn read_result_set_row(packet: &[u8], columns: Arc<[Column]>) -> Result<Row> {
        read_bin_values::<ServerSide>(packet, &*columns)
            .map(|values| new_row(values, columns))
            .map_err(Into::into)
    }
}

impl Conn {
    /// The purpose of this function is to rollback a transaction or to drop query result in cases,
    /// where `Transaction` was dropped without an explicit call to `commit` or `rollback`,
    /// or where `QueryResult` was dropped without being consumed.
    ///
    /// The difference betwee this function and [`Conn::cleanup`] is that this function
    /// won't rollback existing transaction. Another difference, is that this function
    /// won't ignore non-fatal errors.
    pub(crate) async fn clean_dirty(&mut self) -> Result<()> {
        self.drop_result().await?;
        if self.get_tx_status() == TxStatus::RequiresRollback {
            self.set_tx_status(TxStatus::None);
            self.exec_drop("ROLLBACK", ()).await?;
        }
        Ok(())
    }

    /// Low level function that performs a text query.
    pub(crate) async fn raw_query<'a, Q>(&'a mut self, query: Q) -> Result<()>
    where
        Q: AsRef<str> + Send + Sync + 'a,
    {
        self.routine(QueryRoutine::new(query.as_ref().as_bytes()))
            .await
    }
}

/// Methods of this trait are used to execute database queries.
///
/// `Conn` is a `Queryable` as well as `Transaction`.
pub trait Queryable: Send {
    /// Executes `COM_PING`.
    fn ping(&mut self) -> BoxFuture<'_, ()>;

    /// Performs the given query and returns the result.
    fn query_iter<'a, Q>(
        &'a mut self,
        query: Q,
    ) -> BoxFuture<'a, QueryResult<'a, 'static, TextProtocol>>
    where
        Q: AsRef<str> + Send + Sync + 'a;

    /// Prepares the given statement.
    ///
    /// Note, that `Statement` will exist only in the context of this queryable.
    ///
    /// Also note, that this call may close the least recently used statement
    /// if statement cache is at its capacity (see. [`stmt_cache_size`][stmt_cache_size]).
    ///
    /// [stmt_cache_size]: crate::Opts::stmt_cache_size
    fn prep<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Statement>
    where
        Q: AsRef<str> + Sync + Send + 'a;

    /// Closes the given statement.
    ///
    /// Usually there is no need to explicitly close statements
    /// (see. [`stmt_cache_size`][stmt_cache_size]).
    ///
    /// [stmt_cache_size]: crate::Opts::stmt_cache_size
    fn close(&mut self, stmt: Statement) -> BoxFuture<'_, ()>;

    /// Executes the given statement with given params.
    ///
    /// It'll prepare `stmt`, if necessary.
    fn exec_iter<'a: 's, 's, Q, P>(
        &'a mut self,
        stmt: Q,
        params: P,
    ) -> BoxFuture<'s, QueryResult<'a, 'static, BinaryProtocol>>
    where
        Q: StatementLike + 'a,
        P: Into<Params>;

    /// Performs the given query and collects the first result set.
    fn query<'a, T, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Vec<T>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static;

    /// Performs the given query and returns the first row of the first result set.
    fn query_first<'a, T, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Option<T>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static;

    /// Performs the given query and maps each row of the first result set.
    fn query_map<'a, T, F, Q, U>(&'a mut self, query: Q, f: F) -> BoxFuture<'a, Vec<U>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
        F: FnMut(T) -> U + Send + 'a,
        U: Send;

    /// Performs the given query and folds the first result set to a single value.
    fn query_fold<'a, T, F, Q, U>(&'a mut self, query: Q, init: U, f: F) -> BoxFuture<'a, U>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
        F: FnMut(U, T) -> U + Send + 'a,
        U: Send + 'a;

    /// Performs the given query and drops the query result.
    fn query_drop<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, ()>
    where
        Q: AsRef<str> + Send + Sync + 'a;

    /// Exectues the given statement for each item in the given params iterator.
    ///
    /// It'll prepare `stmt` (once), if necessary.
    fn exec_batch<'a: 'b, 'b, S, P, I>(&'a mut self, stmt: S, params_iter: I) -> BoxFuture<'b, ()>
    where
        S: StatementLike + 'b,
        I: IntoIterator<Item = P> + Send + 'b,
        I::IntoIter: Send,
        P: Into<Params> + Send;

    /// Exectues the given statement and collects the first result set.
    ///
    /// It'll prepare `stmt`, if necessary.
    fn exec<'a: 'b, 'b, T, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'b, Vec<T>>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static;

    /// Exectues the given statement and returns the first row of the first result set.
    ///
    /// It'll prepare `stmt`, if necessary.
    fn exec_first<'a: 'b, 'b, T, S, P>(
        &'a mut self,
        stmt: S,
        params: P,
    ) -> BoxFuture<'b, Option<T>>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static;

    /// Exectues the given stmt and maps each row of the first result set.
    ///
    /// It'll prepare `stmt`, if necessary.
    fn exec_map<'a: 'b, 'b, T, S, P, U, F>(
        &'a mut self,
        stmt: S,
        params: P,
        f: F,
    ) -> BoxFuture<'b, Vec<U>>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
        F: FnMut(T) -> U + Send + 'a,
        U: Send + 'a;

    /// Exectues the given stmt and folds the first result set to a signel value.
    ///
    /// It'll prepare `stmt`, if necessary.
    fn exec_fold<'a: 'b, 'b, T, S, P, U, F>(
        &'a mut self,
        stmt: S,
        params: P,
        init: U,
        f: F,
    ) -> BoxFuture<'b, U>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
        F: FnMut(U, T) -> U + Send + 'a,
        U: Send + 'a;

    /// Exectues the given statement and drops the result.
    fn exec_drop<'a: 'b, 'b, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'b, ()>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b;
}

impl Queryable for Conn {
    fn ping(&mut self) -> BoxFuture<'_, ()> {
        BoxFuture(Box::pin(async move {
            self.routine(PingRoutine).await?;
            Ok(())
        }))
    }

    fn query_iter<'a, Q>(
        &'a mut self,
        query: Q,
    ) -> BoxFuture<'a, QueryResult<'a, 'static, TextProtocol>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
    {
        BoxFuture(Box::pin(async move {
            self.routine(QueryRoutine::new(query.as_ref().as_bytes()))
                .await?;
            Ok(QueryResult::new(self))
        }))
    }

    fn prep<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Statement>
    where
        Q: AsRef<str> + Sync + Send + 'a,
    {
        BoxFuture(Box::pin(
            async move { self.get_statement(query.as_ref()).await },
        ))
    }

    fn close(&mut self, stmt: Statement) -> BoxFuture<'_, ()> {
        BoxFuture(Box::pin(async move {
            self.stmt_cache_mut().remove(stmt.id());
            self.close_statement(stmt.id()).await
        }))
    }

    fn exec_iter<'a: 's, 's, Q, P>(
        &'a mut self,
        stmt: Q,
        params: P,
    ) -> BoxFuture<'s, QueryResult<'a, 'static, BinaryProtocol>>
    where
        Q: StatementLike + 'a,
        P: Into<Params>,
    {
        let params = params.into();
        BoxFuture(Box::pin(async move {
            let statement = self.get_statement(stmt).await?;
            self.execute_statement(&statement, params).await?;

            if self.stmt_cache_mut().capacity() == 0 {
                self.close(statement).await?;
            }

            Ok(QueryResult::new(self))
        }))
    }

    fn query<'a, T, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Vec<T>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            self.query_iter(query).await?.collect_and_drop::<T>().await
        }))
    }

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
                result.next().await?.map(crate::from_row)
            };
            result.drop_result().await?;
            Ok(output)
        }))
    }

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

    fn query_drop<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, ()>
    where
        Q: AsRef<str> + Send + Sync + 'a,
    {
        BoxFuture(Box::pin(async move {
            self.query_iter(query).await?.drop_result().await
        }))
    }

    fn exec_batch<'a: 'b, 'b, S, P, I>(&'a mut self, stmt: S, params_iter: I) -> BoxFuture<'b, ()>
    where
        S: StatementLike + 'b,
        I: IntoIterator<Item = P> + Send + 'b,
        I::IntoIter: Send,
        P: Into<Params> + Send,
    {
        BoxFuture(Box::pin(async move {
            let statement = self.get_statement(stmt).await?;

            for params in params_iter {
                self.execute_statement(&statement, params).await?;
                QueryResult::<BinaryProtocol>::new(&mut *self)
                    .drop_result()
                    .await?;
            }

            if self.stmt_cache_mut().capacity() == 0 {
                self.close(statement).await?;
            }

            Ok(())
        }))
    }

    fn exec<'a: 'b, 'b, T, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'b, Vec<T>>
    where
        S: StatementLike + 'b,
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

    fn exec_first<'a: 'b, 'b, T, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'b, Option<T>>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            let mut result = self.exec_iter(stmt, params).await?;
            let row = if result.is_empty() {
                None
            } else {
                result.next().await?
            };
            result.drop_result().await?;
            Ok(row.map(crate::from_row))
        }))
    }

    fn exec_map<'a: 'b, 'b, T, S, P, U, F>(
        &'a mut self,
        stmt: S,
        params: P,
        mut f: F,
    ) -> BoxFuture<'b, Vec<U>>
    where
        S: StatementLike + 'b,
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

    fn exec_fold<'a: 'b, 'b, T, S, P, U, F>(
        &'a mut self,
        stmt: S,
        params: P,
        init: U,
        mut f: F,
    ) -> BoxFuture<'b, U>
    where
        S: StatementLike + 'b,
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

    fn exec_drop<'a: 'b, 'b, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'b, ()>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
    {
        BoxFuture(Box::pin(async move {
            self.exec_iter(stmt, params).await?.drop_result().await
        }))
    }
}

impl Queryable for Transaction<'_> {
    fn ping(&mut self) -> BoxFuture<'_, ()> {
        self.0.ping()
    }

    fn query_iter<'a, Q>(
        &'a mut self,
        query: Q,
    ) -> BoxFuture<'a, QueryResult<'a, 'static, TextProtocol>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
    {
        self.0.query_iter(query)
    }

    fn prep<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Statement>
    where
        Q: AsRef<str> + Sync + Send + 'a,
    {
        self.0.prep(query)
    }
    fn close(&mut self, stmt: Statement) -> BoxFuture<'_, ()> {
        self.0.close(stmt)
    }
    fn exec_iter<'a: 's, 's, Q, P>(
        &'a mut self,
        stmt: Q,
        params: P,
    ) -> BoxFuture<'s, QueryResult<'a, 'static, BinaryProtocol>>
    where
        Q: StatementLike + 'a,
        P: Into<Params>,
    {
        self.0.exec_iter(stmt, params)
    }
    fn query<'a, T, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Vec<T>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
    {
        self.0.query(query)
    }
    fn query_first<'a, T, Q>(&'a mut self, query: Q) -> BoxFuture<'a, Option<T>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
    {
        self.0.query_first(query)
    }
    fn query_map<'a, T, F, Q, U>(&'a mut self, query: Q, f: F) -> BoxFuture<'a, Vec<U>>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
        F: FnMut(T) -> U + Send + 'a,
        U: Send,
    {
        self.0.query_map(query, f)
    }
    fn query_fold<'a, T, F, Q, U>(&'a mut self, query: Q, init: U, f: F) -> BoxFuture<'a, U>
    where
        Q: AsRef<str> + Send + Sync + 'a,
        T: FromRow + Send + 'static,
        F: FnMut(U, T) -> U + Send + 'a,
        U: Send + 'a,
    {
        self.0.query_fold(query, init, f)
    }
    fn query_drop<'a, Q>(&'a mut self, query: Q) -> BoxFuture<'a, ()>
    where
        Q: AsRef<str> + Send + Sync + 'a,
    {
        self.0.query_drop(query)
    }
    fn exec_batch<'a: 'b, 'b, S, P, I>(&'a mut self, stmt: S, params_iter: I) -> BoxFuture<'b, ()>
    where
        S: StatementLike + 'b,
        I: IntoIterator<Item = P> + Send + 'b,
        I::IntoIter: Send,
        P: Into<Params> + Send,
    {
        self.0.exec_batch(stmt, params_iter)
    }
    fn exec<'a: 'b, 'b, T, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'b, Vec<T>>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
    {
        self.0.exec(stmt, params)
    }
    fn exec_first<'a: 'b, 'b, T, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'b, Option<T>>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
    {
        self.0.exec_first(stmt, params)
    }
    fn exec_map<'a: 'b, 'b, T, S, P, U, F>(
        &'a mut self,
        stmt: S,
        params: P,
        f: F,
    ) -> BoxFuture<'b, Vec<U>>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
        F: FnMut(T) -> U + Send + 'a,
        U: Send + 'a,
    {
        self.0.exec_map(stmt, params, f)
    }
    fn exec_fold<'a: 'b, 'b, T, S, P, U, F>(
        &'a mut self,
        stmt: S,
        params: P,
        init: U,
        f: F,
    ) -> BoxFuture<'b, U>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
        T: FromRow + Send + 'static,
        F: FnMut(U, T) -> U + Send + 'a,
        U: Send + 'a,
    {
        self.0.exec_fold(stmt, params, init, f)
    }
    fn exec_drop<'a: 'b, 'b, S, P>(&'a mut self, stmt: S, params: P) -> BoxFuture<'b, ()>
    where
        S: StatementLike + 'b,
        P: Into<Params> + Send + 'b,
    {
        self.0.exec_drop(stmt, params)
    }
}

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
