use Column;
use Conn;
use Row;
use Stmt;

use conn::stmt::{
    InnerStmt,
    new_stmt,
};

use consts;

use either::{
    Either,
    Left,
    Right,
};

use errors::*;

use lib_futures::{
    Async,
    Future,
    Poll,
};
use lib_futures::Async::Ready;
use lib_futures::stream::Stream;

use proto::{
    OkPacket,
    Packet,
    PacketType,
};

use super::{
    NewTextQueryResult,
    NewRawQueryResult,
    ReadPacket,
};

use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;

use value::{
    FromRow,
    Value,
};

pub mod futures;

pub use self::futures::{
    BinCollect,
    BinForEach,
    BinMap,
    BinReduce,
    Collect,
    CollectAll,
    ForEach,
    Map,
    Reduce,
};
use self::futures::{
    new_bin_collect,
    new_bin_for_each,
    new_bin_map,
    new_bin_reduce,
    new_collect,
    new_collect_all,
    new_for_each,
    new_map,
    new_reduce,
};

use self::futures::{
    CollectAllNew,
    CollectNew,
    ForEachNew,
    MapNew,
    ReduceNew,
    new_collect_all_new,
    new_collect_new,
    new_for_each_new,
    new_map_new,
    new_reduce_new,
};

pub struct ResultSetNew<T, Q: QueryResult>(Vec<T>, Q);

impl<T, Q: QueryResult> Deref for ResultSetNew<T, Q> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<T, Q: QueryResult> IntoIterator for ResultSetNew<T, Q> {
    type Item = T;
    type IntoIter = ::std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// This type stores collected result set.
pub struct ResultSet<T>(Vec<T>, TextQueryResult);

impl<T> ResultSet<T> {
    /// Affected rows value returned by a server, if any.
    pub fn affected_rows(&self) -> Option<u64> {
        self.1.affected_rows()
    }

    /// Last insert id value returned by a server, if any.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.1.last_insert_id()
    }

    /// Warnings count value returned by a server, if any.
    pub fn warnings(&self) -> Option<u16> {
        self.1.warnings()
    }

    /// Bytes of info string returned by a server, if any.
    pub fn info_bytes(&self) -> Option<&[u8]> {
        self.1.info_bytes()
    }

    /// Info bytes lossy converted to UTF-8, if any.
    pub fn info(&self) -> Option<Cow<str>> {
        self.1.info()
    }

    /// Bytes of session state changed value returned by a server, if any.
    pub fn session_state_changes_bytes(&self) -> Option<&[u8]> {
        self.1.session_state_changes_bytes()
    }

    /// Session state changed value lossy converted to UTF-8, if any.
    pub fn session_state_changes(&self) -> Option<Cow<str>> {
        self.1.session_state_changes()
    }
}

impl<R> Deref for ResultSet<R> {
    type Target = [R];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<R> IntoIterator for ResultSet<R> {
    type Item = R;
    type IntoIter = ::std::vec::IntoIter<R>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub trait ResultKind {
    type Output: Sized;

    fn read_values(packet: Packet, cols: &Arc<Vec<Column>>) -> Result<Vec<Value>>;
    fn handle_next_query_result(next: RawQueryResult<Self>) -> Self::Output;
    fn handle_end(conn: Conn) -> Self::Output;
}

pub struct BinaryResult;
impl ResultKind for BinaryResult {
    type Output = Conn;

    fn read_values(packet: Packet, cols: &Arc<Vec<Column>>) -> Result<Vec<Value>> {
        Value::from_bin_payload(packet.as_ref(), cols)
    }

    fn handle_next_query_result(next: RawQueryResult<Self>) -> Self::Output {
        panic!("Binary protocol query can't produce multi-result set");
    }

    fn handle_end(conn: Conn) -> Self::Output {
        conn
    }
}

pub struct TextResult;
impl ResultKind for TextResult {
    type Output = Either<TextQueryResultNew, Conn>;

    fn read_values(packet: Packet, cols: &Arc<Vec<Column>>) -> Result<Vec<Value>> {
        Value::from_payload(packet.as_ref(), cols.len())
    }

    fn handle_next_query_result(next: RawQueryResult<Self>) -> Self::Output {
        Left(next.into())
    }

    fn handle_end(conn: Conn) -> Self::Output {
        Right(conn)
    }
}

enum RawStep<K: ResultKind + ?Sized> {
    ReadPacket(ReadPacket),
    NextResult(NewRawQueryResult<K>),
    Done(Conn),
    Consumed,
}

enum RawOut<K: ResultKind + ?Sized> {
    ReadPacket((Conn, Packet)),
    NextResult(RawQueryResult<K>),
    Done,
}

pub struct RawQueryResult<K: ResultKind + ?Sized> {
    step: RawStep<K>,
    columns: Arc<Vec<Column>>,
    ok_packet: Option<OkPacket>,
    _phantom: PhantomData<K>,
}

pub fn new_raw<K: ?Sized, T>(mut conn: Conn, cols: T, ok_packet: Option<OkPacket>) -> RawQueryResult<K>
where T: Into<Arc<Vec<Column>>>,
      K: ResultKind,
{
    let cols = cols.into();
    let step = if cols.len() == 0 {
        RawStep::Done(conn)
    } else {
        RawStep::ReadPacket(conn.read_packet())
    };

    RawQueryResult {
        step: step,
        columns: cols,
        ok_packet: ok_packet,
        _phantom: PhantomData,
    }
}

impl<K: ResultKind> RawQueryResult<K> {
    fn either_poll(&mut self) -> Result<Async<Option<RawOut<K>>>> {
        match self.step {
            RawStep::ReadPacket(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Async::Ready(Some(RawOut::ReadPacket(val))))
            },
            RawStep::NextResult(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Async::Ready(Some(RawOut::NextResult(val))))
            },
            RawStep::Done(_) => {
                Ok(Async::Ready(Some(RawOut::Done)))
            },
            RawStep::Consumed => {
                Ok(Async::Ready(None))
            },
        }
    }
}

impl<K: ResultKind> Stream for RawQueryResult<K> {
    type Item = Either<Row, K::Output>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match try_ready!(self.either_poll()) {
            Some(RawOut::ReadPacket((mut conn, packet))) => if packet.is(PacketType::Eof) {
                if conn.status.contains(consts::SERVER_MORE_RESULTS_EXISTS) {
                    self.step = RawStep::NextResult(conn.handle_result_set::<K>());
                    self.poll()
                } else {
                    conn.has_result = None;
                    self.step = RawStep::Done(conn);
                    self.poll()
                }
            } else {
                let values = try!(K::read_values(packet, &self.columns));
                let row = Row::new(values, self.columns.clone());
                self.step = RawStep::ReadPacket(conn.read_packet());
                Ok(Ready(Some(Left(row))))
            },
            Some(RawOut::NextResult(query_result)) => {
                let output = K::handle_next_query_result(query_result);
                self.step = RawStep::Consumed;
                Ok(Ready(Some(Right(output))))
            },
            Some(RawOut::Done) => {
                if let RawStep::Done(conn) = mem::replace(&mut self.step, RawStep::Consumed) {
                    let output = K::handle_end(conn);
                    Ok(Async::Ready(Some(Right(output))))
                } else {
                    unreachable!();
                }
            },
            None => Ok(Ready(None)),
        }
    }
}

pub trait QueryResultOutput {
    type Payload;

    fn into_next_or_conn(self) -> Either<Self::Payload, Conn>;
}

impl QueryResultOutput for Conn {
    type Payload = ();

    fn into_next_or_conn(self) -> Either<Self::Payload, Conn> {
        Right(self)
    }
}

impl QueryResultOutput for Either<TextQueryResultNew, Conn> {
    type Payload = TextQueryResultNew;

    fn into_next_or_conn(self) -> Either<TextQueryResultNew, Conn> {
        self
    }
}

pub trait QueryResult {
    type Output: QueryResultOutput;

    #[doc(hidden)]
    fn poll(&mut self) -> Result<Async<Either<Row, Self::Output>>>;

    fn collect<R>(self) -> CollectNew<R, Self>
        where Self: Sized,
              R: FromRow,
    {
        new_collect_new(self)
    }

    fn collect_all(self) -> CollectAllNew<Self>
        where Self: Sized,
    {
        new_collect_all_new(self)
    }

    fn map<F, U>(self, fun: F) -> MapNew<F, U, Self>
        where Self: Sized,
              F: FnMut(Row) -> U,
    {
        new_map_new(self, fun)
    }

    fn reduce<A, F>(self, init: A, fun: F) -> ReduceNew<A, F, Self>
        where Self: Sized,
              F: FnMut(A, Row) -> A,
    {
        new_reduce_new(self, init, fun)
    }

    fn for_each<F>(self, fun: F) -> ForEachNew<F, Self>
        where Self: Sized,
              F: FnMut(Row),
    {
        new_for_each_new(self, fun)
    }
}

pub struct TextQueryResultNew {
    // TODO: Is option needed?
    raw: Option<RawQueryResult<TextResult>>,
}

impl From<RawQueryResult<TextResult>> for TextQueryResultNew {
    fn from(raw_query_result: RawQueryResult<TextResult>) -> Self {
        TextQueryResultNew {
            raw: Some(raw_query_result),
        }
    }
}

impl QueryResult for TextQueryResultNew {
    type Output = <TextResult as ResultKind>::Output;

    fn poll(&mut self) -> Result<Async<Either<Row, Self::Output>>> {
        let result = try_ready!(self.raw.as_mut().unwrap().poll());
        Ok(Async::Ready(result.expect("Can't poll consumed query result")))
    }
}

pub struct BinQueryResultNew {
    raw: Option<RawQueryResult<BinaryResult>>,
}

impl From<RawQueryResult<BinaryResult>> for BinQueryResultNew {
    fn from(raw_query_result: RawQueryResult<BinaryResult>) -> Self {
        BinQueryResultNew {
            raw: Some(raw_query_result),
        }
    }
}

impl QueryResult for BinQueryResultNew {
    type Output = <BinaryResult as ResultKind>::Output;

    fn poll(&mut self) -> Result<Async<Either<Row, Self::Output>>> {
        let result = try_ready!(self.raw.as_mut().unwrap().poll());
        Ok(Async::Ready(result.expect("Can't poll consumed query result")))
    }
}

//impl Stream for TextQueryResult {
//    type Item = MaybeRow;
//    type Error = Error;
//
//    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
//        match try_ready!(self.either_poll()) {
//            Some(Out::NextResult(query_result)) => {
//                self.step = Step::Consumed;
//                Ok(Async::Ready(Some(MaybeRow::End(Left(query_result)))))
//            },
//            Some(Out::Done) => {
//                if let Step::Done(conn) = mem::replace(&mut self.step, Step::Consumed) {
//                    Ok(Async::Ready(Some(MaybeRow::End(Right(conn)))))
//                } else {
//                    unreachable!();
//                }
//            },
//    }
//}
//impl Stream for BinQueryResult {
//    type Item = BinMaybeRow;
//    type Error = Error;
//
//    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
//        match try_ready!(self.wrapped.poll()) {
//            Some(MaybeRow::Row(row)) => Ok(Async::Ready(Some(BinMaybeRow::Row(row)))),
//            Some(MaybeRow::End(Right(conn))) => {
//                Ok(Async::Ready(Some(BinMaybeRow::End(new_stmt(self.inner_stmt.clone(), conn)))))
//            },
//            _ => Ok(Async::Ready(None)),
//        }
//    }
//}

pub enum MaybeRow {
    /// Row of a result set
    Row(Row),
    /// Either next result set of a multi-result set if it exists or wrapped `Conn`.
    End(Either<TextQueryResult, Conn>),
}

enum Step {
    ReadPacket(ReadPacket),
    NextResult(NewTextQueryResult),
    Done(Conn),
    Consumed,
}

enum Out {
    ReadPacket((Conn, Packet)),
    NextResult(TextQueryResult),
    Done,
}

// TODO: Give a way to generalize over (Bin)MaybeRow.
/// Mysql query result set.
///
/// `TextQueryResult` may contain zero or more rows. It may also be followed by another result set in
/// case of multi-result set.
pub struct TextQueryResult {
    step: Step,
    columns: Arc<Vec<Column>>,
    ok_packet: Option<OkPacket>,
    is_bin: bool,
}

pub fn new<T: Into<Arc<Vec<Column>>>>(mut conn: Conn,
                                      columns: T,
                                      ok_packet: Option<OkPacket>,
                                      is_bin: bool) -> TextQueryResult
{
    let columns = columns.into();
    let cols_len = columns.len();
    conn.has_result = Some((columns.clone(), ok_packet.clone(), is_bin));
    TextQueryResult {
        step: if cols_len == 0 { Step::Done(conn) } else { Step::ReadPacket(conn.read_packet()) },
        columns: columns,
        ok_packet: ok_packet,
        is_bin: is_bin,
    }
}

impl TextQueryResult {
    /// Collects remaining rows of this result set and returns them as `ResultSet`.
    ///
    /// Second element of a resulting tuple is either next `TextQueryResult` if it exists in
    /// multi-result set or `Conn`.
    pub fn collect<R>(self) -> Collect<R>
        where R: FromRow,
              R: Send + 'static,
    {
        new_collect(self)
    }

    /// Collects remaining rows of this and all rows of following result sets.
    pub fn collect_all(self) -> CollectAll {
        new_collect_all(self)
    }

    pub fn map<F, U>(self, fun: F) -> Map<F, U>
    where F: FnMut(Row) -> U,
          Self: Sized,
    {
        new_map(self, fun)
    }

    pub fn reduce<A, F>(self, init: A, fun: F) -> Reduce<A, F>
    where F: FnMut(A, Row) -> A,
          A: Send + 'static,
          Self: Sized,
    {
        new_reduce(self, init, fun)
    }

    pub fn for_each<F>(self, fun: F) -> ForEach<F>
    where F: FnMut(Row),
          Self: Sized
    {
        new_for_each(self, fun)
    }

    /// Affected rows value returned by a server, if any.
    pub fn affected_rows(&self) -> Option<u64> {
        self.ok_packet.as_ref().map(OkPacket::affected_rows)
    }

    /// Last insert id value returned by a server, if any.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.ok_packet.as_ref().map(OkPacket::last_insert_id)
    }

    /// Warnings count value returned by a server, if any.
    pub fn warnings(&self) -> Option<u16> {
        self.ok_packet.as_ref().map(OkPacket::warnings)
    }

    /// Bytes of info string returned by a server, if any.
    pub fn info_bytes(&self) -> Option<&[u8]> {
        self.ok_packet.as_ref().map(OkPacket::info_bytes)
    }

    /// Info bytes lossy converted to UTF-8, if any.
    pub fn info(&self) -> Option<Cow<str>> {
        self.ok_packet.as_ref().map(OkPacket::info)
    }

    /// Bytes of session state changed value returned by a server, if any.
    pub fn session_state_changes_bytes(&self) -> Option<&[u8]> {
        self.ok_packet.as_ref().and_then(OkPacket::session_state_changes_bytes)
    }

    /// Session state changed value lossy converted to UTF-8, if any.
    pub fn session_state_changes(&self) -> Option<Cow<str>> {
        self.ok_packet.as_ref().and_then(OkPacket::session_state_changes)
    }

    fn either_poll(&mut self) -> Result<Async<Option<Out>>> {
        match self.step {
            Step::ReadPacket(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Async::Ready(Some(Out::ReadPacket(val))))
            },
            Step::NextResult(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Async::Ready(Some(Out::NextResult(val))))
            },
            Step::Done(_) => {
                Ok(Async::Ready(Some(Out::Done)))
            },
            Step::Consumed => {
                Ok(Async::Ready(None))
            },
        }
    }
}

impl Stream for TextQueryResult {
    type Item = MaybeRow;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match try_ready!(self.either_poll()) {
            Some(Out::ReadPacket((mut conn, packet))) => {
                if packet.is(PacketType::Eof) {
                    if conn.status.contains(consts::SERVER_MORE_RESULTS_EXISTS) {
                        self.step = Step::NextResult(conn.handle_text_resultset());
                        self.poll()
                    } else {
                        conn.has_result = None;
                        self.step = Step::Done(conn);
                        self.poll()
                    }
                } else {
                    let values = if self.is_bin {
                        try!(Value::from_bin_payload(packet.as_ref(), &self.columns))
                    } else {
                        try!(Value::from_payload(packet.as_ref(), self.columns.len()))
                    };
                    let row = Row::new(values, self.columns.clone());
                    self.step = Step::ReadPacket(conn.read_packet());
                    Ok(Async::Ready(Some(MaybeRow::Row(row))))
                }
            },
            Some(Out::NextResult(query_result)) => {
                self.step = Step::Consumed;
                Ok(Async::Ready(Some(MaybeRow::End(Left(query_result)))))
            },
            Some(Out::Done) => {
                if let Step::Done(conn) = mem::replace(&mut self.step, Step::Consumed) {
                    Ok(Async::Ready(Some(MaybeRow::End(Right(conn)))))
                } else {
                    unreachable!();
                }
            },
            None => {
                Ok(Async::Ready(None))
            },
        }
    }
}

pub struct BinQueryResult {
    wrapped: TextQueryResult,
    inner_stmt: InnerStmt,
}

pub fn new_bin(wrapped: TextQueryResult, inner_stmt: InnerStmt) -> BinQueryResult {
    BinQueryResult {
        wrapped: wrapped,
        inner_stmt: inner_stmt,
    }
}

pub fn unwrap_bin(bin_query_result: BinQueryResult) -> (TextQueryResult, InnerStmt) {
    let BinQueryResult {
        wrapped,
        inner_stmt,
    } = bin_query_result;
    (wrapped, inner_stmt)
}

impl BinQueryResult {
    /// Collects remaining rows of this result set and returns them as `ResultSet`.
    ///
    /// Second element of a resulting tuple is either next `TextQueryResult` if it exists in
    /// multi-result set or `Conn`.
    pub fn collect<R>(self) -> BinCollect<R>
        where R: FromRow,
              R: Send + 'static,
    {
        new_bin_collect(self)
    }

    pub fn map<F, U>(self, fun: F) -> BinMap<F, U>
        where F: FnMut(Row) -> U,
              Self: Sized,
    {
        new_bin_map(self, fun)
    }

    pub fn reduce<A, F>(self, init: A, fun: F) -> BinReduce<A, F>
        where F: FnMut(A, Row) -> A,
              A: Send + 'static,
              Self: Sized,
    {
        new_bin_reduce(self, init, fun)
    }

    pub fn for_each<F>(self, fun: F) -> BinForEach<F>
        where F: FnMut(Row),
              Self: Sized
    {
        new_bin_for_each(self, fun)
    }

    /// Affected rows value returned by a server, if any.
    pub fn affected_rows(&self) -> Option<u64> {
        self.wrapped.ok_packet.as_ref().map(OkPacket::affected_rows)
    }

    /// Last insert id value returned by a server, if any.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.wrapped.ok_packet.as_ref().map(OkPacket::last_insert_id)
    }

    /// Warnings count value returned by a server, if any.
    pub fn warnings(&self) -> Option<u16> {
        self.wrapped.ok_packet.as_ref().map(OkPacket::warnings)
    }

    /// Bytes of info string returned by a server, if any.
    pub fn info_bytes(&self) -> Option<&[u8]> {
        self.wrapped.ok_packet.as_ref().map(OkPacket::info_bytes)
    }

    /// Info bytes lossy converted to UTF-8, if any.
    pub fn info(&self) -> Option<Cow<str>> {
        self.wrapped.ok_packet.as_ref().map(OkPacket::info)
    }

    /// Bytes of session state changed value returned by a server, if any.
    pub fn session_state_changes_bytes(&self) -> Option<&[u8]> {
        self.wrapped.ok_packet.as_ref().and_then(OkPacket::session_state_changes_bytes)
    }

    /// Session state changed value lossy converted to UTF-8, if any.
    pub fn session_state_changes(&self) -> Option<Cow<str>> {
        self.wrapped.ok_packet.as_ref().and_then(OkPacket::session_state_changes)
    }
}

pub enum BinMaybeRow {
    Row(Row),
    End(Stmt),
}

impl Stream for BinQueryResult {
    type Item = BinMaybeRow;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match try_ready!(self.wrapped.poll()) {
            Some(MaybeRow::Row(row)) => Ok(Async::Ready(Some(BinMaybeRow::Row(row)))),
            Some(MaybeRow::End(Right(conn))) => {
                Ok(Async::Ready(Some(BinMaybeRow::End(new_stmt(self.inner_stmt.clone(), conn)))))
            },
            _ => Ok(Async::Ready(None)),
        }
    }
}
