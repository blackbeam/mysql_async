use BinMaybeRow;
use Conn;
use FromRow;
use MaybeRow;
use ResultSet;
use Stmt;

use either::Either;

use errors::*;

use from_row;

use futures::{
    BinQueryResult,
    TextQueryResult,
};

use lib_futures::{
    Async,
    Future,
    Poll,
};
use lib_futures::stream::Stream;

use std::mem;

use super::super::{
    unwrap_bin,
};

pub struct Collect<R> {
    vec: Vec<R>,
    query_result: Option<TextQueryResult>,
}

pub fn new<R>(query_result: TextQueryResult) -> Collect<R> {
    Collect {
        vec: Vec::new(),
        query_result: Some(query_result),
    }
}

impl<R> Future for Collect<R>
where R: FromRow,
      R: Send + 'static,
{
    type Item = (ResultSet<R>, Either<TextQueryResult, Conn>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.as_mut().unwrap().poll()) {
            Some(MaybeRow::Row(row)) => {
                self.vec.push(from_row(row));
                self.poll()
            },
            Some(MaybeRow::End(next_result_set_or_row)) => {
                let query_result = self.query_result.take().unwrap();
                let vec = mem::replace(&mut self.vec, Vec::new());
                Ok(Async::Ready((ResultSet(vec, query_result), next_result_set_or_row)))
            },
            None => unreachable!(),
        }
    }
}

pub struct BinCollect<R> {
    vec: Vec<R>,
    query_result: Option<BinQueryResult>,
}

pub fn new_bin<R>(query_result: BinQueryResult) -> BinCollect<R> {
    BinCollect {
        vec: Vec::new(),
        query_result: Some(query_result),
    }
}

impl<R> Future for BinCollect<R>
where R: FromRow,
      R: Send + 'static,
{
    type Item = (ResultSet<R>, Stmt);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.as_mut().unwrap().poll()) {
            Some(BinMaybeRow::Row(row)) => {
                self.vec.push(from_row(row));
                self.poll()
            },
            Some(BinMaybeRow::End(stmt)) => {
                let query_result = self.query_result.take().unwrap();
                let vec = mem::replace(&mut self.vec, Vec::new());
                let (query_result, _) = unwrap_bin(query_result);
                Ok(Async::Ready((ResultSet(vec, query_result), stmt)))
            },
            None => unreachable!(),
        }
    }
}
