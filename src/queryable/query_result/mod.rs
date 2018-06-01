// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use BoxFuture;
use Column;
use Row;
use connection_like::{ConnectionLike, ConnectionLikeWrapper, StmtCacheResult};
use connection_like::streamless::Streamless;
use consts::StatusFlags;
use errors::*;
use io;
use lib_futures::future::{AndThen, Either, Future, FutureResult, Loop, loop_fn, ok};
use lib_futures::future::Either::*;
use myc::packets::RawPacket;
use prelude::FromRow;
use queryable::Protocol;
use self::QueryResultInner::*;
pub use self::for_each::ForEach;
pub use self::map::Map;
pub use self::reduce::Reduce;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

mod for_each;
mod map;
mod reduce;

pub type ForEachAndDrop<S, T, P, F> = AndThen<
    Either<FutureResult<S, Error>, ForEach<T, P, F>>,
    BoxFuture<T>,
    fn(S) -> BoxFuture<T>,
>;

pub type MapAndDrop<S, T, P, F, U> = AndThen<
    Either<
        FutureResult<(S, Vec<U>), Error>,
        Map<T, P, F, U>,
    >,
    (BoxFuture<T>, FutureResult<Vec<U>, Error>),
    fn((S, Vec<U>))
       -> (BoxFuture<T>, FutureResult<Vec<U>, Error>),
>;

pub type ReduceAndDrop<S, T, P, F, U> = AndThen<
    Either<
        FutureResult<(S, U), Error>,
        Reduce<T, P, F, U>,
    >,
    (BoxFuture<T>, FutureResult<U, Error>),
    fn((S, U)) -> (BoxFuture<T>, FutureResult<U, Error>),
>;

pub fn new<T, P>(
    conn_like: T,
    columns: Option<Arc<Vec<Column>>>,
    cached: Option<StmtCacheResult>,
) -> QueryResult<T, P>
where
    T: ConnectionLike + Sized + 'static,
    P: Protocol + 'static,
{
    QueryResult::new(conn_like, columns, cached)
}

pub fn disassemble<T, P>(
    query_result: QueryResult<T, P>,
) -> (T, Option<Arc<Vec<Column>>>, Option<StmtCacheResult>) {
    match query_result {
        QueryResult(Empty(Some(A(conn_like)), cached, _)) => (conn_like, None, cached),
        QueryResult(WithRows(Some(A(conn_like)), columns, cached, _)) => {
            (conn_like, Some(columns), cached)
        }
        _ => unreachable!(),
    }
}

pub fn assemble<T, P>(
    conn_like: T,
    columns: Option<Arc<Vec<Column>>>,
    cached: Option<StmtCacheResult>,
) -> QueryResult<T, P>
where
    T: ConnectionLike + Sized + 'static,
    P: Protocol + 'static,
{
    match columns {
        Some(columns) => QueryResult(WithRows(Some(A(conn_like)), columns, cached, PhantomData)),
        None => QueryResult(Empty(Some(A(conn_like)), cached, PhantomData)),
    }
}

enum QueryResultInner<T, P> {
    Empty(Option<Either<T, Streamless<T>>>, Option<StmtCacheResult>, PhantomData<P>),
    WithRows(
        Option<Either<T, Streamless<T>>>,
        Arc<Vec<Column>>,
        Option<StmtCacheResult>,
        PhantomData<P>
    ),
}

/// Result of a query or statement execution.
pub struct QueryResult<T, P>(QueryResultInner<T, P>);

impl<T, P> QueryResult<T, P>
where
    T: ConnectionLike + Sized + 'static,
    P: Protocol + 'static,
{
    fn into_empty(mut self) -> Self {
        self.set_pending_result(None);
        match self {
            QueryResult(WithRows(conn_like, _, cached, _)) => {
                QueryResult(Empty(conn_like, cached, PhantomData))
            }
            x => x,
        }
    }

    fn into_inner(self) -> (T, Option<StmtCacheResult>) {
        match self {
            QueryResult(Empty(conn_like, cached, _)) |
            QueryResult(WithRows(conn_like, _, cached, _)) => {
                match conn_like {
                    Some(A(conn_like)) => (conn_like, cached),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn get_row_raw(self) -> BoxFuture<(Self, Option<RawPacket>)> {
        if self.is_empty() {
            return Box::new(ok((self, None)));
        }
        let fut = self.read_packet().and_then(|(this, packet)| {
            if P::is_last_result_set_packet(&this, &packet) {
                if this.get_status().contains(
                    StatusFlags::SERVER_MORE_RESULTS_EXISTS,
                )
                {
                    let (inner, cached) = this.into_inner();
                    A(A(inner.read_result_set(cached).map(
                        |new_this| (new_this, None),
                    )))
                } else {
                    A(B(ok((this.into_empty(), None))))
                }
            } else {
                B(ok((this, Some(packet))))
            }
        });
        Box::new(fut)
    }

    fn get_row(self) -> BoxFuture<(Self, Option<Row>)> {
        let fut = self.get_row_raw().and_then(
            |(this, packet_opt)| match packet_opt {
                Some(packet) => {
                    match this {
                        QueryResult(WithRows(_, ref columns, ..)) => {
                            P::read_result_set_row(&packet, columns.clone())
                        }
                        _ => unreachable!(),
                    }.map(|row| (this, Some(row)))
                }
                None => Ok((this, None)),
            },
        );
        Box::new(fut)
    }

    fn new(
        conn_like: T,
        columns: Option<Arc<Vec<Column>>>,
        cached: Option<StmtCacheResult>,
    ) -> QueryResult<T, P> {
        match columns {
            Some(columns) => {
                QueryResult(WithRows(Some(A(conn_like)), columns, cached, PhantomData))
            }
            None => QueryResult(Empty(Some(A(conn_like)), cached, PhantomData)),
        }
    }

    /// Last insert id (if not 0).
    pub fn last_insert_id(&self) -> Option<u64> {
        self.get_last_insert_id()
    }

    /// Value of `affected_rows` returned from a server.
    pub fn affected_rows(&self) -> u64 {
        self.get_affected_rows()
    }

    /// `true` if there is no more rows nor result sets in this query.
    ///
    /// One could use it to check if there is more than one result set in this query result.
    pub fn is_empty(&self) -> bool {
        match *self {
            QueryResult(Empty(..)) => !self.more_results_exists(),
            _ => false,
        }
    }

    /// Returns `true` if the SERVER_MORE_RESULTS_EXISTS flag is contained in status flags
    /// of the connection.
    fn more_results_exists(&self) -> bool {
        self.get_status().contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
    }

    /// `true` if rows may exists for this query result.
    ///
    /// If `false` then there is no rows possible (for example UPDATE query).
    fn has_rows(&self) -> bool {
        match *self {
            QueryResult(Empty(..)) => false,
            _ => true,
        }
    }

    /// Returns future that collects result set of this query result.
    ///
    /// It is parametrized by `R` and internally calls `R::from_row(Row)` on each row.
    ///
    /// It will stop collecting on result set boundary. This means that you should call `collect`
    /// as many times as result sets in your query result. For example query
    /// `SELECT 'foo'; SELECT 'foo', 'bar';` will produce `QueryResult` with two result sets in it.
    /// One can use `QueryResult::is_empty` to make sure that there is no more result sets.
    pub fn collect<R>(self) -> BoxFuture<(Self, Vec<R>)>
    where
        R: FromRow + Send + 'static,
    {
        let fut = self.reduce(Vec::new(), |mut acc, row| {
            acc.push(FromRow::from_row(row));
            acc
        });
        Box::new(fut)
    }

    /// Returns future that collects result set of a query result and drops everything else.
    /// It will resolve to a pair of wrapped `Queryable` and collected result set.
    pub fn collect_and_drop<R>(self) -> BoxFuture<(T, Vec<R>)>
    where
        R: FromRow + Send + 'static,
    {
        let fut = self.collect().and_then(|(this, output)| {
            (this.drop_result(), ok(output))
        });
        Box::new(fut)
    }

    /// Returns future that will execute `fun` on every row of current result set.
    ///
    /// It will stop on result set boundary (see `QueryResult::collect` docs).
    pub fn for_each<F>(self, fun: F) -> Either<FutureResult<Self, Error>, ForEach<T, P, F>>
    where
        F: FnMut(Row),
    {
        if self.is_empty() {
            A(ok(self))
        } else {
            B(ForEach::new(self, fun))
        }
    }

    /// Returns future that will execute `fun` on every row of current result set and drop
    /// everything else. It will resolve to a wrapped `Queryable`.
    pub fn for_each_and_drop<F>(self, fun: F) -> ForEachAndDrop<Self, T, P, F>
    where
        F: FnMut(Row),
    {
        self.for_each(fun).and_then(QueryResult::drop_result)
    }

    /// Returns future that will map every row of current result set to `U` using `fun`.
    ///
    /// It will stop on result set boundary (see `QueryResult::collect` docs).
    pub fn map<F, U>(self, fun: F) -> Either<FutureResult<(Self, Vec<U>), Error>, Map<T, P, F, U>>
    where
        F: FnMut(Row) -> U,
    {
        if self.is_empty() {
            A(ok((self, Vec::new())))
        } else {
            B(Map::new(self, fun))
        }
    }

    /// Returns future that will map every row of current result set to `U` using `fun` and drop
    /// everything else. It will resolve to a pair of wrapped `Queryable` and mapped result set.
    pub fn map_and_drop<F, U>(self, fun: F) -> MapAndDrop<Self, T, P, F, U>
    where
        F: FnMut(Row) -> U,
    {
        fn join_drop<T, P, U>(
            (this, output): (QueryResult<T, P>, Vec<U>),
        ) -> (BoxFuture<T>, FutureResult<Vec<U>, Error>)
        where
            T: ConnectionLike + Sized + 'static,
            P: Protocol + 'static,
        {
            (QueryResult::drop_result(this), ok(output))
        }

        self.map(fun).and_then(join_drop)
    }

    /// Returns future that will reduce rows of current result set to `U` using `fun`.
    ///
    /// It will stop on result set boundary (see `QueryResult::collect` docs).
    pub fn reduce<F, U>(
        self,
        init: U,
        fun: F,
    ) -> Either<FutureResult<(Self, U), Error>, Reduce<T, P, F, U>>
    where
        F: FnMut(U, Row) -> U,
    {
        if self.is_empty() {
            A(ok((self, init)))
        } else {
            B(Reduce::new(self, init, fun))
        }
    }

    /// Returns future that will reduce rows of current result set to `U` using `fun` and drop
    /// everything else. It will resolve to a pair of wrapped `Queryable` and `U`.
    pub fn reduce_and_drop<F, U>(self, init: U, fun: F) -> ReduceAndDrop<Self, T, P, F, U>
    where
        F: FnMut(U, Row) -> U,
    {
        fn join_drop<T, P, U>(
            (this, output): (QueryResult<T, P>, U),
        ) -> (BoxFuture<T>, FutureResult<U, Error>)
        where
            T: ConnectionLike + Sized + 'static,
            P: Protocol + 'static,
        {
            (QueryResult::drop_result(this), ok(output))
        }

        self.reduce(init, fun).and_then(join_drop)
    }

    /// Returns future that will drop this query result end resolve to a wrapped `Queryable`.
    pub fn drop_result(self) -> BoxFuture<T> {
        let fut = loop_fn(self, |this| if !this.has_rows() {
            if this.more_results_exists() {
                let (inner, cached) = this.into_inner();
                A(A(inner.read_result_set(cached).map(|new_this| Loop::Continue(new_this))))
            } else {
                A(B(ok(Loop::Break(this.into_inner()))))
            }
        } else {
            B(this.get_row_raw().map(|(this, _)| Loop::Continue(this)))
        });
        let fut = fut.and_then(|(conn_like, cached)| {
            if let Some(StmtCacheResult::NotCached(statement_id)) = cached {
                A(conn_like.close_stmt(statement_id))
            } else {
                B(ok(conn_like))
            }
        });
        Box::new(fut)
    }
}

impl<T: ConnectionLike + 'static, P: Protocol> ConnectionLikeWrapper for QueryResult<T, P> {
    type ConnLike = T;

    fn take_stream(self) -> (Streamless<Self>, io::Stream)
    where
        Self: Sized,
    {
        match self {
            QueryResult(Empty(conn_like, cached, _)) => {
                match conn_like {
                    Some(A(conn_like)) => {
                        let (streamless, stream) = conn_like.take_stream();
                        let self_streamless = Streamless::new(QueryResult(
                            Empty(Some(B(streamless)), cached, PhantomData),
                        ));
                        (self_streamless, stream)
                    }
                    Some(B(..)) => panic!("Logic error: stream taken"),
                    None => unreachable!(),
                }
            }
            QueryResult(WithRows(conn_like, columns, cached, _)) => {
                match conn_like {
                    Some(A(conn_like)) => {
                        let (streamless, stream) = conn_like.take_stream();
                        let self_streamless =
                            Streamless::new(QueryResult(
                                WithRows(Some(B(streamless)), columns, cached, PhantomData),
                            ));
                        (self_streamless, stream)
                    }
                    Some(B(..)) => panic!("Logic error: stream taken"),
                    None => unreachable!(),
                }
            }
        }
    }

    fn return_stream(&mut self, stream: io::Stream) {
        match *self {
            QueryResult(Empty(ref mut conn_like, ..)) |
            QueryResult(WithRows(ref mut conn_like, ..)) => {
                let actual_conn_like = mem::replace(conn_like, None);
                match actual_conn_like {
                    Some(A(..)) => panic!("Logic error: stream exists"),
                    Some(B(streamless)) => {
                        *conn_like = Some(A(streamless.return_stream(stream)));
                    }
                    None => unreachable!(),
                }
            }
        }
    }

    fn conn_like_ref(&self) -> &Self::ConnLike {
        match *self {
            QueryResult(Empty(ref conn_like, ..)) |
            QueryResult(WithRows(ref conn_like, ..)) => {
                match *conn_like {
                    Some(A(ref conn_like)) => conn_like,
                    _ => unreachable!(),
                }
            }
        }
    }

    fn conn_like_mut(&mut self) -> &mut Self::ConnLike {
        match *self {
            QueryResult(Empty(ref mut conn_like, ..)) |
            QueryResult(WithRows(ref mut conn_like, ..)) => {
                match *conn_like {
                    Some(A(ref mut conn_like)) => conn_like,
                    _ => unreachable!(),
                }
            }
        }
    }
}
