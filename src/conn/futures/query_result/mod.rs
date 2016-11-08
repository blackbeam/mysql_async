// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::new_raw_query_result::NewRawQueryResult;
use conn::futures::query_result::futures::*;
use conn::futures::read_packet::ReadPacket;
use conn::stmt::InnerStmt;
use conn::stmt::new_stmt;
use conn::stmt::Stmt;
use consts;
use either::Either;
use either::Left;
use either::Right;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use lib_futures::stream::Stream;
use proto::Column;
use proto::OkPacket;
use proto::Packet;
use proto::PacketType;
use proto::Row;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use value::FromRow;
use value::Value;


pub mod futures;


/// Result set of a query or statement execution.
///
/// It represents query result which has been collected.
/// It could be dereferenced into it's query result to get access to `QueryResult` methods.
pub struct ResultSet<T, Q>(pub Vec<T>, Q);

impl<T, Q: QueryResult> ResultSet<T, Q> {
    pub fn as_ref(&self) -> &[T] {
        &*self.0
    }
}

impl<T, Q: QueryResult> Deref for ResultSet<T, Q> {
    type Target = Q;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

impl<T, Q: QueryResult> IntoIterator for ResultSet<T, Q> {
    type Item = T;
    type IntoIter = ::std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Result kind of a query result.
pub trait ResultKind {
    /// Output of this result kind.
    ///
    /// For `TextResult` it is either next `TextQueryResult` of a multi-result set or `Conn` on
    /// which query was executed.
    ///
    /// For `BinaryResult` it is `Stmt` which was executed.
    type Output: Sized;
}

pub trait InnerResultKind: ResultKind {
    fn read_values(packet: Packet, cols: &Arc<Vec<Column>>) -> Result<Vec<Value>>;
    fn handle_next_query_result(next: RawQueryResult<Self>) -> Self::Output;
    fn handle_end(conn: Conn, inner_stmt: Option<InnerStmt>) -> Self::Output;
}

/// Binary protocol result (i.e. statement execution result).
pub struct BinaryResult;

impl ResultKind for BinaryResult {
    type Output = Stmt;
}

impl InnerResultKind for BinaryResult {
    fn read_values(packet: Packet, cols: &Arc<Vec<Column>>) -> Result<Vec<Value>> {
        Value::from_bin_payload(packet.as_ref(), cols)
    }

    fn handle_next_query_result(_: RawQueryResult<Self>) -> Self::Output {
        panic!("Binary protocol query can't produce multi-result set");
    }

    fn handle_end(conn: Conn, inner_stmt: Option<InnerStmt>) -> Self::Output {
        new_stmt(inner_stmt.unwrap(), conn)
    }
}

/// Text protocol result (i.e. query execution result).
pub struct TextResult;

impl ResultKind for TextResult {
    type Output = Either<TextQueryResult, Conn>;
}

impl InnerResultKind for TextResult {
    fn read_values(packet: Packet, cols: &Arc<Vec<Column>>) -> Result<Vec<Value>> {
        Value::from_payload(packet.as_ref(), cols.len())
    }

    fn handle_next_query_result(next: RawQueryResult<Self>) -> Self::Output {
        Left(next.into())
    }

    fn handle_end(conn: Conn, _: Option<InnerStmt>) -> Self::Output {
        Right(conn)
    }
}

enum Step<K: ResultKind + ?Sized> {
    ReadPacket(ReadPacket),
    NextResult(NewRawQueryResult<K>),
    Done(Conn),
    Consumed,
}

enum Out<K: ResultKind + ?Sized> {
    ReadPacket((Conn, Packet)),
    NextResult(RawQueryResult<K>),
    Done,
}

/// Raw query result implementation parametrized by it's kind.
pub struct RawQueryResult<K: ResultKind + ?Sized> {
    step: Step<K>,
    columns: Arc<Vec<Column>>,
    ok_packet: Option<OkPacket>,
    inner_stmt: Option<InnerStmt>,
    _phantom: PhantomData<K>,
}

pub fn new_raw<K: ?Sized, T>(mut conn: Conn,
                             cols: T,
                             ok_packet: Option<OkPacket>,
                             inner_stmt: Option<InnerStmt>) -> RawQueryResult<K>
where T: Into<Arc<Vec<Column>>>,
      K: ResultKind,
{
    let cols = cols.into();
    let step = if cols.len() == 0 {
        Step::Done(conn)
    } else {
        conn.has_result = Some((cols.clone(), ok_packet.clone(), inner_stmt.clone()));
        Step::ReadPacket(conn.read_packet())
    };

    RawQueryResult {
        step: step,
        columns: cols,
        ok_packet: ok_packet,
        inner_stmt: inner_stmt,
        _phantom: PhantomData,
    }
}

impl<K: ResultKind> RawQueryResult<K> {
    fn either_poll(&mut self) -> Result<Async<Option<Out<K>>>> {
        match self.step {
            Step::ReadPacket(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Some(Out::ReadPacket(val))))
            },
            Step::NextResult(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Some(Out::NextResult(val))))
            },
            Step::Done(_) => {
                Ok(Ready(Some(Out::Done)))
            },
            Step::Consumed => {
                Ok(Ready(None))
            },
        }
    }
}

impl<K: ResultKind + InnerResultKind> Stream for RawQueryResult<K> {
    type Item = Either<Row, K::Output>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match try_ready!(self.either_poll()) {
            Some(Out::ReadPacket((mut conn, packet))) => if packet.is(PacketType::Eof) {
                if conn.status.contains(consts::SERVER_MORE_RESULTS_EXISTS) {
                    self.step = Step::NextResult(conn.handle_result_set::<K>(self.inner_stmt.clone()));
                    self.poll()
                } else {
                    conn.has_result = None;
                    self.step = Step::Done(conn);
                    self.poll()
                }
            } else {
                let values = try!(K::read_values(packet, &self.columns));
                let row = Row::new(values, self.columns.clone());
                self.step = Step::ReadPacket(conn.read_packet());
                Ok(Ready(Some(Left(row))))
            },
            Some(Out::NextResult(query_result)) => {
                let output = K::handle_next_query_result(query_result);
                self.step = Step::Consumed;
                Ok(Ready(Some(Right(output))))
            },
            Some(Out::Done) => {
                if let Step::Done(mut conn) = mem::replace(&mut self.step, Step::Consumed) {
                    conn.has_result = None;
                    let output = K::handle_end(conn, self.inner_stmt.clone());
                    Ok(Ready(Some(Right(output))))
                } else {
                    unreachable!();
                }
            },
            None => Ok(Ready(None)),
        }
    }
}

/// Final result of a query result.
pub trait QueryResultOutput {
    /// Type of query result for this query result output.
    type Result: QueryResult;
    /// Output of a query result output.
    ///
    /// It is `Conn` for text protocol or `Stmt` fot binary protocol.
    type Output;

    /// A way to generically extract possible next result and output of query result output.
    ///
    /// Returns (previous result, Either<next result, output>)
    fn into_next_or_output(self, prev: Self::Result) -> (Self::Result,
                                                         Either<Self::Result, Self::Output>);
}

impl QueryResultOutput for Stmt {
    type Result = BinQueryResult;
    type Output = Stmt;

    fn into_next_or_output(self, prev: BinQueryResult) -> (Self::Result,
                                                           Either<Self::Result, Self::Output>)
    {
        (prev, Right(self))
    }
}

impl QueryResultOutput for Either<TextQueryResult, Conn> {
    type Result = TextQueryResult;
    type Output = Conn;

    fn into_next_or_output(self, prev: TextQueryResult) -> (Self::Result,
                                                            Either<Self::Result, Self::Output>)
    {
        (prev, self)
    }
}

/// Query result which was not consumed yet.
pub trait UnconsumedQueryResult: InnerQueryResult {
    type Output: QueryResultOutput;

    /// Returns future which collects result set of this query result.
    ///
    /// It is parametrized by `R` and internally calls `R::from_row(Row)` on each row.
    fn collect<R>(self) -> Collect<R, Self>
        where Self: Sized,
              R: FromRow,
    {
        new_collect(self)
    }

    /// It returns future that collects all result sets of a multi-result set.
    ///
    /// Since only text protocol result could be a multi-result set this method makes sense only for
    /// `TextQueryResult`.
    fn collect_all(self) -> CollectAll<Self>
        where Self: Sized,
    {
        new_collect_all(self)
    }

    /// It returns future that maps every `Row` of this query result to `U`.
    fn map<F, U>(self, fun: F) -> Map<F, U, Self>
        where Self: Sized,
              F: FnMut(Row) -> U,
    {
        new_map(self, fun)
    }

    /// It returns future that reduce rows of this query result to an instance of `A`.
    fn reduce<A, F>(self, init: A, fun: F) -> Reduce<A, F, Self>
        where Self: Sized,
              F: FnMut(A, Row) -> A,
    {
        new_reduce(self, init, fun)
    }

    /// It returns future that applies `F` to every `Row` of this query result.
    fn for_each<F>(self, fun: F) -> ForEach<F, Self>
        where Self: Sized,
              F: FnMut(Row),
    {
        new_for_each(self, fun)
    }

    /// It returns future that drops result and resolves to wrapped `Conn` on `Stmt`.
    fn drop_result(self) -> DropResult<Self>
        where Self: Sized,
    {
        new_drop_result(self)
    }
}

/// Inner methods of `QueryResult`.
pub trait InnerQueryResult: QueryResult {
    /// Polls another row of this query result.
    fn poll(&mut self) -> Result<Async<Either<Row, Self::Output>>>
    where Self: UnconsumedQueryResult;

    fn ok_packet_ref(&self) -> Option<&OkPacket>;
}

/// Common methods of all query results.
pub trait QueryResult {
    /// Affected rows value returned by a server, if any.
    fn affected_rows(&self) -> Option<u64>
        where Self: InnerQueryResult
    {
        self.ok_packet_ref().map(OkPacket::affected_rows)
    }

    /// Last insert id value returned by a server, if any.
    fn last_insert_id(&self) -> Option<u64>
        where Self: InnerQueryResult
    {
        self.ok_packet_ref().map(OkPacket::last_insert_id)
    }

    /// Warnings count value returned by a server, if any.
    fn warnings(&self) -> Option<u16>
        where Self: InnerQueryResult
    {
        self.ok_packet_ref().map(OkPacket::warnings)
    }

    /// Bytes of info string returned by a server, if any.
    fn info_bytes(&self) -> Option<&[u8]>
        where Self: InnerQueryResult
    {
        self.ok_packet_ref().map(OkPacket::info_bytes)
    }

    /// Info bytes lossy converted to UTF-8, if any.
    fn info(&self) -> Option<Cow<str>>
        where Self: InnerQueryResult
    {
        self.ok_packet_ref().map(OkPacket::info)
    }

    /// Bytes of session state changed value returned by a server, if any.
    fn session_state_changes_bytes(&self) -> Option<&[u8]>
        where Self: InnerQueryResult
    {
        self.ok_packet_ref().and_then(OkPacket::session_state_changes_bytes)
    }

    /// Session state changed value lossy converted to UTF-8, if any.
    fn session_state_changes(&self) -> Option<Cow<str>>
        where Self: InnerQueryResult
    {
        self.ok_packet_ref().and_then(OkPacket::session_state_changes)
    }
}

/// Uncollected result of a query execution.
pub struct TextQueryResult(RawQueryResult<TextResult>);

#[doc(hidden)]
impl From<RawQueryResult<TextResult>> for TextQueryResult {
    fn from(raw_query_result: RawQueryResult<TextResult>) -> Self {
        TextQueryResult(raw_query_result)
    }
}

impl QueryResult for TextQueryResult {}

impl UnconsumedQueryResult for TextQueryResult {
    type Output = < TextResult as ResultKind >::Output;
}

impl InnerQueryResult for TextQueryResult {
    fn poll(&mut self) -> Result<Async<Either<Row, <Self as UnconsumedQueryResult>::Output>>>
        where Self: UnconsumedQueryResult,
    {
        let result = try_ready!(self.0.poll());
        Ok(Ready(result.expect("Can't poll consumed query result")))
    }

    fn ok_packet_ref(&self) -> Option<&OkPacket> {
        self.0.ok_packet.as_ref()
    }
}

/// Uncollected result of a statement execution.
pub struct BinQueryResult(RawQueryResult<BinaryResult>);

#[doc(hidden)]
impl From<RawQueryResult<BinaryResult>> for BinQueryResult {
    fn from(raw_query_result: RawQueryResult<BinaryResult>) -> Self {
        BinQueryResult(raw_query_result)
    }
}

impl QueryResult for BinQueryResult {}

impl UnconsumedQueryResult for BinQueryResult {
    type Output = < BinaryResult as ResultKind >::Output;
}

impl InnerQueryResult for BinQueryResult {
    #[doc(hidden)]
    fn poll(&mut self) -> Result<Async<Either<Row, <Self as UnconsumedQueryResult>::Output>>>
        where Self: UnconsumedQueryResult,
    {
        let result = try_ready!(self.0.poll());
        Ok(Ready(result.expect("Can't poll consumed query result")))
    }

    #[doc(hidden)]
    fn ok_packet_ref(&self) -> Option<&OkPacket> {
        self.0.ok_packet.as_ref()
    }
}
