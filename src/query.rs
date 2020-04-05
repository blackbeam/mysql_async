// Copyright (c) 2020 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::{
    connection_like::{ConnectionLike, ToConnection, ToConnectionResult},
    from_row,
    prelude::FromRow,
    queryable::stmt::StatementLike,
    BinaryProtocol, BoxFuture, Params, QueryResult, TextProtocol,
};

/// MySql text query.
///
/// This trait covers the set of `query*` methods on the `Queryable` trait.
/// Please see the corresponding section of the crate level docs for details.
///
/// Example:
///
/// ```rust
/// # use mysql_async::test_misc::get_opts;
/// # #[tokio::main]
/// # async fn main() -> mysql_async::Result<()> {
/// use mysql_async::*;
/// use mysql_async::prelude::*;
/// let pool = Pool::new(get_opts());
///
/// let num: Option<u32> = "SELECT 42".first(&pool).await?;
///
/// assert_eq!(num, Some(42));
/// # Ok(()) }
/// ```
pub trait TextQuery: Send + Sized {
    /// This methods corresponds to [`Queryable::query_iter`].
    fn run<'a, C>(self, conn: C) -> BoxFuture<'a, QueryResult<'a, TextProtocol>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a;

    /// This methods corresponds to [`Queryable::query_first`].
    fn first<'a, T, C>(self, conn: C) -> BoxFuture<'a, Option<T>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            let mut result = self.run(conn).await?;
            let output = if result.is_empty() {
                None
            } else {
                result.get_row().await?.map(from_row)
            };
            result.drop_result().await?;
            Ok(output)
        }))
    }

    /// This methods corresponds to [`Queryable::query`].
    fn fetch<'a, T, C>(self, conn: C) -> BoxFuture<'a, Vec<T>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            let result = self.run(conn).await?;
            result.collect_and_drop::<T>().await
        }))
    }

    /// This methods corresponds to [`Queryable::query_fold`].
    fn reduce<'a, T, U, F, C>(self, conn: C, init: U, next: F) -> BoxFuture<'a, U>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
        F: FnMut(U, T) -> U + Send + 'a,
        T: FromRow + Send + 'static,
        U: Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            let result = self.run(conn).await?;
            let output = result.reduce_and_drop(init, next).await?;
            Ok(output)
        }))
    }

    /// This methods corresponds to [`Queryable::query_map`].
    fn map<'a, T, U, F, C>(self, conn: C, mut map: F) -> BoxFuture<'a, Vec<U>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
        F: FnMut(T) -> U + Send + 'a,
        T: FromRow + Send + 'static,
        U: Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            let result = self
                .reduce(conn, Vec::new(), move |mut acc, row: T| {
                    acc.push(map(row));
                    acc
                })
                .await?;
            Ok(result)
        }))
    }
}

impl<Q: AsRef<str> + Send + Sync> TextQuery for Q {
    fn run<'a, C>(self, conn: C) -> BoxFuture<'a, QueryResult<'a, TextProtocol>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
    {
        BoxFuture(Box::pin(async move {
            let mut conn = match conn.to_connection() {
                ToConnectionResult::Immediate(conn) => conn,
                ToConnectionResult::Mediate(fut) => fut.await?,
            };
            conn.conn_mut().raw_query(self).await?;
            Ok(QueryResult::new(conn))
        }))
    }
}

/// Representaion of a prepared statement query.
///
/// See `BinQuery` for details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryWithParams<Q, P> {
    pub query: Q,
    pub params: P,
}

/// Helper, that constructs [`QueryWithParams`].
pub trait WithParams: Sized {
    fn with<P>(&self, params: P) -> QueryWithParams<Self, P>;
}

impl<'a, T: StatementLike + ?Sized> WithParams for &'a T {
    fn with<P>(&self, params: P) -> QueryWithParams<Self, P> {
        QueryWithParams {
            query: self,
            params,
        }
    }
}

/// MySql prepared statement query.
///
/// This trait covers the set of `exec*` methods on the [`Queryable`] trait.
/// Please see the corresponding section of the crate level docs for details.
///
/// Example:
///
/// ```rust
/// # use mysql_async::test_misc::get_opts;
/// # #[tokio::main]
/// # async fn main() -> mysql_async::Result<()> {
/// use mysql_async::*;
/// use mysql_async::prelude::*;
///
/// let pool = Pool::new(get_opts());
///
/// let row: Option<(u32, String)> = "SELECT ?, ?".with((42_u8, "foo")).first(&pool).await?;
///
/// assert_eq!(row, Some((42, "foo".into())));
/// # Ok(()) }
/// ```
pub trait BinQuery: Send + Sized {
    /// This methods corresponds to [`Queryable::exec_iter`].
    fn run<'a, C>(self, conn: C) -> BoxFuture<'a, QueryResult<'a, BinaryProtocol>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a;

    /// This methods corresponds to [`Queryable::exec_first`].
    fn first<'a, T, C>(self, conn: C) -> BoxFuture<'a, Option<T>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            let mut result = self.run(conn).await?;
            let output = if result.is_empty() {
                None
            } else {
                result.get_row().await?.map(from_row)
            };
            result.drop_result().await?;
            Ok(output)
        }))
    }

    /// This methods corresponds to [`Queryable::exec`].
    fn fetch<'a, T, C>(self, conn: C) -> BoxFuture<'a, Vec<T>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            let result = self.run(conn).await?;
            result.collect_and_drop::<T>().await
        }))
    }

    /// This methods corresponds to [`Queryable::exec_fold`].
    fn reduce<'a, T, U, F, C>(self, conn: C, init: U, next: F) -> BoxFuture<'a, U>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
        F: FnMut(U, T) -> U + Send + 'a,
        T: FromRow + Send + 'static,
        U: Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            let result = self.run(conn).await?;
            let output = result.reduce_and_drop(init, next).await?;
            Ok(output)
        }))
    }

    /// This methods corresponds to [`Queryable::exec_map`].
    fn map<'a, T, U, F, C>(self, conn: C, mut map: F) -> BoxFuture<'a, Vec<U>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
        T: FromRow + Send + 'static,
        F: FnMut(T) -> U + Send + 'a,
        U: Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            let result = self
                .reduce(conn, Vec::new(), move |mut acc, row: T| {
                    acc.push(map(row));
                    acc
                })
                .await?;
            Ok(result)
        }))
    }
}

impl<Q, P> BinQuery for QueryWithParams<&'_ Q, P>
where
    Q: StatementLike + ?Sized,
    P: Into<Params> + Send,
{
    fn run<'a, C>(self, conn: C) -> BoxFuture<'a, QueryResult<'a, BinaryProtocol>>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
    {
        BoxFuture(Box::pin(async move {
            let query = self.query;
            let params = self.params.into();

            let mut conn = match conn.to_connection() {
                ToConnectionResult::Immediate(conn) => conn,
                ToConnectionResult::Mediate(fut) => fut.await?,
            };

            let statement = conn.conn_mut().get_statement(query).await?;
            conn.conn_mut()
                .execute_statement(&statement, params)
                .await?;
            Ok(QueryResult::new(conn))
        }))
    }
}

/// Helper trait for batch statement execution.
///
/// This trait covers the [`Queryable::exec_batch`] method.
///
/// Example:
///
/// ```rust
/// # use mysql_async::test_misc::get_opts;
/// # #[tokio::main]
/// # async fn main() -> mysql_async::Result<()> {
/// use mysql_async::*;
/// use mysql_async::prelude::*;
///
/// let pool = Pool::new(get_opts());
///
/// // This will prepare `DO ?` and execute `DO 0`, `DO 1`, `DO 2` and so on.
/// "DO ?"
///     .with((0..10).map(|x| (x,)))
///     .batch(&pool)
///     .await?;
/// # Ok(()) }
/// ```
pub trait BatchQuery {
    fn batch<'a, C>(self, conn: C) -> BoxFuture<'a, ()>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a;
}

impl<Q, I, P> BatchQuery for QueryWithParams<&'_ Q, I>
where
    Q: StatementLike + ?Sized,
    I: IntoIterator<Item = P> + Send,
    I::IntoIter: Send,
    P: Into<Params> + Send,
{
    fn batch<'a, C>(self, conn: C) -> BoxFuture<'a, ()>
    where
        Self: 'a,
        C: ToConnection<'a> + 'a,
    {
        BoxFuture(Box::pin(async move {
            let query = self.query;
            let params = self.params;

            let mut conn = match conn.to_connection() {
                ToConnectionResult::Immediate(conn) => conn,
                ToConnectionResult::Mediate(fut) => fut.await?,
            };

            let statement = conn.conn_mut().get_statement(query).await?;

            for params in params {
                conn.conn_mut()
                    .execute_statement(&statement, params)
                    .await?;
            }

            Ok(())
        }))
    }
}
