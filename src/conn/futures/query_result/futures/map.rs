use Conn;
use Stmt;

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

use proto::Row;

use std::mem;

use super::super::{
    BinMaybeRow,
    BinQueryResult,
    MaybeRow,
    QueryResult,
    TextQueryResult,
};

pub struct MapNew<F, U, T: QueryResult> {
    query_result: T,
    fun: F,
    acc: Vec<U>,
}

pub fn new_new<F, U, T>(query_result: T, fun: F) -> MapNew<F, U, T>
    where F: FnMut(Row) -> U,
          T: QueryResult,
{
    MapNew {
        query_result: query_result,
        fun: fun,
        acc: Vec::new(),
    }
}

impl<F, U, T> Future for MapNew<F, U, T>
where F: FnMut(Row) -> U,
      T: QueryResult,
{
    type Item = (Vec<U>, T::Output);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.poll()) {
            Left(row) => {
                let val = (&mut self.fun)(row);
                self.acc.push(val);
                self.poll()
            },
            Right(output) => {
                let acc = mem::replace(&mut self.acc, Vec::new());
                Ok(Async::Ready((acc, output)))
            }
        }
    }
}

pub struct Map<F, U> {
    stream: TextQueryResult,
    fun: F,
    acc: Vec<U>,
}

pub fn new<F, U>(stream: TextQueryResult, fun: F) -> Map<F, U>
    where F: FnMut(Row) -> U,
{
    Map {
        stream: stream,
        fun: fun,
        acc: Vec::new(),
    }
}

impl<F, U> Future for Map<F, U>
where F: FnMut(Row) -> U,
{
    type Item = (Vec<U>, Either<TextQueryResult, Conn>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.stream.poll()) {
            Some(MaybeRow::Row(row)) => {
                let val = (&mut self.fun)(row);
                self.acc.push(val);
                self.poll()
            },
            Some(MaybeRow::End(query_result_or_conn)) => {
                let acc = mem::replace(&mut self.acc, Vec::new());
                Ok(Async::Ready((acc, query_result_or_conn)))
            },
            None => panic!("pooled twice"),
        }
    }
}

pub struct BinMap<F, U> {
    stream: BinQueryResult,
    fun: F,
    acc: Vec<U>,
}

pub fn new_bin<F, U>(stream: BinQueryResult, fun: F) -> BinMap<F, U>
    where F: FnMut(Row) -> U,
{
    BinMap {
        stream: stream,
        fun: fun,
        acc: Vec::new(),
    }
}

impl<F, U> Future for BinMap<F, U>
where F: FnMut(Row) -> U,
{
    type Item = (Vec<U>, Stmt);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.stream.poll()) {
            Some(BinMaybeRow::Row(row)) => {
                let val = (&mut self.fun)(row);
                self.acc.push(val);
                self.poll()
            },
            Some(BinMaybeRow::End(stmt)) => {
                let acc = mem::replace(&mut self.acc, Vec::new());
                Ok(Async::Ready((acc, stmt)))
            },
            None => panic!("pooled twice"),
        }
    }
}
