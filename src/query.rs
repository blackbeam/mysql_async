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
/// // text protocol query
/// let num: Option<u32> = "SELECT 42".first(&pool).await?;
///
/// // binary protocol query (prepared statement)
/// // that will prepare `DO ?` and execute `DO 0`, `DO 1`, `DO 2` and so on.
/// "DO ?"
///     .with((0..10).map(|x| (x,)))
///     .batch(&pool)
///     .await?;
///
/// assert_eq!(num, Some(42));
/// # Ok(()) }
/// ```
pub trait Query: Send + Sized {
    /// Query protocol.
    type Protocol: crate::prelude::Protocol;

    /// This methods corresponds to [`Queryable::query_iter`].
    fn run<'a, 't: 'a, C>(self, conn: C) -> BoxFuture<'a, QueryResult<'a, 't, Self::Protocol>>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a;

    /// This methods corresponds to [`Queryable::query_first`].
    fn first<'a, 't: 'a, T, C>(self, conn: C) -> BoxFuture<'a, Option<T>>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a,
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
    fn fetch<'a, 't: 'a, T, C>(self, conn: C) -> BoxFuture<'a, Vec<T>>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a,
        T: FromRow + Send + 'static,
    {
        BoxFuture(Box::pin(async move {
            self.run(conn).await?.collect_and_drop::<T>().await
        }))
    }

    /// This methods corresponds to [`Queryable::query_fold`].
    fn reduce<'a, 't: 'a, T, U, F, C>(self, conn: C, init: U, next: F) -> BoxFuture<'a, U>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a,
        F: FnMut(U, T) -> U + Send + 'a,
        T: FromRow + Send + 'static,
        U: Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            self.run(conn).await?.reduce_and_drop(init, next).await
        }))
    }

    /// This methods corresponds to [`Queryable::query_map`].
    fn map<'a, 't: 'a, T, U, F, C>(self, conn: C, mut map: F) -> BoxFuture<'a, Vec<U>>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a,
        F: FnMut(T) -> U + Send + 'a,
        T: FromRow + Send + 'static,
        U: Send + 'a,
    {
        BoxFuture(Box::pin(async move {
            self.run(conn)
                .await?
                .map_and_drop(|row| map(from_row(row)))
                .await
        }))
    }

    /// This method corresponds to [`Queryable::query_drop`].
    fn ignore<'a, 't: 'a, C>(self, conn: C) -> BoxFuture<'a, ()>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a,
    {
        BoxFuture(Box::pin(async move {
            self.run(conn).await?.drop_result().await
        }))
    }
}

impl<Q: AsRef<str> + Send + Sync> Query for Q {
    type Protocol = TextProtocol;

    fn run<'a, 't: 'a, C>(self, conn: C) -> BoxFuture<'a, QueryResult<'a, 't, TextProtocol>>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a,
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

impl<Q, P> Query for QueryWithParams<&'_ Q, P>
where
    Q: StatementLike + ?Sized,
    P: Into<Params> + Send,
{
    type Protocol = BinaryProtocol;

    fn run<'a, 't: 'a, C>(self, conn: C) -> BoxFuture<'a, QueryResult<'a, 't, BinaryProtocol>>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a,
    {
        BoxFuture(Box::pin(async move {
            let mut conn = match conn.to_connection() {
                ToConnectionResult::Immediate(conn) => conn,
                ToConnectionResult::Mediate(fut) => fut.await?,
            };

            let statement = conn.conn_mut().get_statement(self.query).await?;

            conn.conn_mut()
                .execute_statement(&statement, self.params.into())
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
    fn batch<'a, 't: 'a, C>(self, conn: C) -> BoxFuture<'a, ()>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a;
}

impl<Q, I, P> BatchQuery for QueryWithParams<&'_ Q, I>
where
    Q: StatementLike + ?Sized,
    I: IntoIterator<Item = P> + Send,
    I::IntoIter: Send,
    P: Into<Params> + Send,
{
    fn batch<'a, 't: 'a, C>(self, conn: C) -> BoxFuture<'a, ()>
    where
        Self: 'a,
        C: ToConnection<'a, 't> + 'a,
    {
        BoxFuture(Box::pin(async move {
            let mut conn = match conn.to_connection() {
                ToConnectionResult::Immediate(conn) => conn,
                ToConnectionResult::Mediate(fut) => fut.await?,
            };

            let statement = conn.conn_mut().get_statement(self.query).await?;

            for params in self.params {
                conn.conn_mut()
                    .execute_statement(&statement, params)
                    .await?;
            }

            Ok(())
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::{prelude::*, test_misc::get_opts, *};

    #[tokio::test]
    async fn should_run_text_query() -> Result<()> {
        let query_static = "SELECT 1, 2 UNION ALL SELECT 3, 4; SELECT 5, 6;";
        let query_string = String::from(query_static);

        macro_rules! test {
            ($query:expr, $conn:expr) => {{
                let mut result = $query.run($conn).await?;
                let result1: Vec<(u8, u8)> = result.collect().await?;
                let result2: Vec<(u8, u8)> = result.collect().await?;
                assert_eq!(result1, vec![(1, 2), (3, 4)]);
                assert_eq!(result2, vec![(5, 6)]);

                $query.ignore($conn).await?;

                let result: Option<(u8, u8)> = $query.first($conn).await?;
                assert_eq!(result, Some((1, 2)));

                let result: Vec<(u8, u8)> = $query.fetch($conn).await?;
                assert_eq!(result, vec![(1, 2), (3, 4)]);

                let result = $query
                    .map($conn, |row: (u8, u8)| format!("{:?}", row))
                    .await?;
                assert_eq!(result, vec![String::from("(1, 2)"), String::from("(3, 4)")]);

                let result = $query
                    .reduce($conn, 0_u8, |acc, row: (u8, u8)| acc + row.0 + row.1)
                    .await?;
                assert_eq!(result, 10);
            }};
        }

        let mut conn = Conn::new(get_opts()).await?;
        test!(query_static, &mut conn);
        test!(query_string.as_str(), &mut conn);

        let mut tx = conn.start_transaction(Default::default()).await?;
        test!(query_static, &mut tx);
        test!(query_string.as_str(), &mut tx);
        tx.rollback().await?;

        conn.disconnect().await?;

        let pool = Pool::new(get_opts());
        test!(query_static, &pool);
        test!(query_string.as_str(), &pool);

        let mut tx = pool.start_transaction(Default::default()).await?;
        test!(query_static, &mut tx);
        test!(query_string.as_str(), &mut tx);
        tx.rollback().await?;

        pool.disconnect().await?;

        Ok(())
    }

    #[tokio::test]
    async fn should_run_bin_query() -> Result<()> {
        let query_static = "SELECT ?, ? UNION ALL SELECT ?, ?";
        let query_string = String::from(query_static);
        let params_static = ("1", "2", "3", "4");
        let params_string = (
            "1".to_owned(),
            "2".to_owned(),
            "3".to_owned(),
            "4".to_owned(),
        );

        macro_rules! test {
            ($query:expr, $params:expr, $conn:expr) => {{
                let mut result = $query.with($params).run($conn).await?;
                let result1: Vec<(u8, u8)> = result.collect().await?;
                assert_eq!(result1, vec![(1, 2), (3, 4)]);

                $query.with($params).ignore($conn).await?;

                let result: Option<(u8, u8)> = $query.with($params).first($conn).await?;
                assert_eq!(result, Some((1, 2)));

                let result: Vec<(u8, u8)> = $query.with($params).fetch($conn).await?;
                assert_eq!(result, vec![(1, 2), (3, 4)]);

                let result = $query
                    .with($params)
                    .map($conn, |row: (u8, u8)| format!("{:?}", row))
                    .await?;
                assert_eq!(result, vec![String::from("(1, 2)"), String::from("(3, 4)")]);

                let result = $query
                    .with($params)
                    .reduce($conn, 0_u8, |acc, row: (u8, u8)| acc + row.0 + row.1)
                    .await?;
                assert_eq!(result, 10);

                $query
                    .with(vec![$params, $params, $params, $params])
                    .batch($conn)
                    .await?;
            }};
        }

        let mut conn = Conn::new(get_opts()).await?;
        test!(query_static, params_static, &mut conn);
        test!(query_string.as_str(), params_string.clone(), &mut conn);

        let mut tx = conn.start_transaction(Default::default()).await?;
        test!(query_static, params_string.clone(), &mut tx);
        test!(query_string.as_str(), params_static, &mut tx);
        tx.rollback().await?;

        conn.disconnect().await?;

        let pool = Pool::new(get_opts());
        test!(query_static, params_static, &pool);
        test!(query_string.as_str(), params_string.clone(), &pool);

        let mut tx = pool.start_transaction(Default::default()).await?;
        test!(query_static, params_string.clone(), &mut tx);
        test!(query_string.as_str(), params_static, &mut tx);
        tx.rollback().await?;

        pool.disconnect().await?;

        Ok(())
    }
}
