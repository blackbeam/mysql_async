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
use lib_futures::stream::Stream;

use proto::{
    OkPacket,
    Packet,
    PacketType,
};

use super::{
    NewTextQueryResult,
    ReadPacket,
};

use std::borrow::Cow;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;

use value::{
    FromRow,
    Value,
};

mod futures;

pub use self::futures::{
    BinCollect,
    BinMap,
    BinReduce,
    Collect,
    CollectAll,
    Map,
    Reduce,
};
use self::futures::{
    new_bin_collect,
    new_bin_map,
    new_bin_reduce,
    new_collect,
    new_collect_all,
    new_map,
    new_reduce,
};

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

/// Mysql query result set.
///
/// `TextQueryResult` may contain zero or more rows. It may also be followed by another result set in
/// case of multi-result set.
pub struct TextQueryResult {
    step: Step,
    columns: Arc<Vec<Column>>,
    ok_packet: Option<OkPacket>,
    // TODO: Remove
    #[allow(dead_code)]
    is_bin: bool,
}

pub fn new(conn: Conn,
           cols: Vec<Column>,
           ok_packet: Option<OkPacket>,
           is_bin: bool) -> TextQueryResult
{
    let cols_len = cols.len();
    TextQueryResult {
        step: if cols_len == 0 { Step::Done(conn) } else { Step::ReadPacket(conn.read_packet()) },
        columns: Arc::new(cols),
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

    fn either_poll(&mut self) -> Result<Async<Option<Either<(Conn, Packet),
                                                            Either<TextQueryResult, ()>>>>> {
        match self.step {
            Step::ReadPacket(ref mut fut) => {
                Ok(Async::Ready(Some(Left(try_ready!(fut.poll())))))
            },
            Step::NextResult(ref mut fut) => {
                Ok(Async::Ready(Some(Right(Left(try_ready!(fut.poll()))))))
            },
            Step::Done(_) => {
                Ok(Async::Ready(Some(Right(Right(())))))
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
            Some(Left((conn, packet))) => {
                if packet.is(PacketType::Eof) {
                    if conn.status.contains(consts::SERVER_MORE_RESULTS_EXISTS) {
                        self.step = Step::NextResult(conn.handle_text_resultset());
                        self.poll()
                    } else {
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
            Some(Right(Left(query_result))) => {
                self.step = Step::Consumed;
                Ok(Async::Ready(Some(MaybeRow::End(Left(query_result)))))
            },
            Some(Right(Right(()))) => {
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
