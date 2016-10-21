use BinMaybeRow;
use Conn;
use MaybeRow;
use Row;
use Stmt;

use either::{
    Either,
    Left,
    Right,
};

use errors::*;

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

use super::super::QueryResult;

pub struct ReduceNew<A, F, T: QueryResult> {
    query_result: T,
    accum: Option<A>,
    fun: F,
}

pub fn new_new<A, F, T>(query_result: T, init: A, fun: F) -> ReduceNew<A, F, T>
    where F: FnMut(A, Row) -> A,
          T: QueryResult,
{
    ReduceNew {
        query_result: query_result,
        accum: Some(init),
        fun: fun,
    }
}

impl<A, F, T> Future for ReduceNew<A, F, T>
where F: FnMut(A, Row) -> A,
      T: QueryResult,
{
    type Item = (A, T::Output);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.query_result.poll()) {
            Left(row) => {
                let old_acc_val = self.accum.take().unwrap();
                let new_acc_val = (self.fun)(old_acc_val, row);
                self.accum = Some(new_acc_val);
                self.poll()
            },
            Right(output) => {
                let acc_val = self.accum.take().unwrap();
                Ok(Async::Ready((acc_val, output)))
            },
        }
    }
}

pub struct Reduce<A, F> {
    fut: TextQueryResult,
    accum: Option<A>,
    fun: F,
}

pub fn new<A, F>(query_result: TextQueryResult, init: A, fun: F) -> Reduce<A, F>
    where F: FnMut(A, Row) -> A,
          A: Send + 'static,
{
    Reduce {
        fut: query_result,
        accum: Some(init),
        fun: fun,
    }
}

impl<A, F> Future for Reduce<A, F>
where F: FnMut(A, Row) -> A,
      A: Send + 'static,
{
    type Item = (A, Either<TextQueryResult, Conn>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.fut.poll()) {
            Some(MaybeRow::Row(row)) => {
                let old_acc_val = self.accum.take().unwrap();
                let new_acc_val = (self.fun)(old_acc_val, row);
                self.accum = Some(new_acc_val);
                self.poll()
            },
            Some(MaybeRow::End(query_result_or_conn)) => {
                let acc_val = self.accum.take().unwrap();
                Ok(Async::Ready((acc_val, query_result_or_conn)))
            },
            None => {
                panic!("reduce called on consumed query result");
            },
        }
    }
}

pub struct BinReduce<A, F> {
    fut: BinQueryResult,
    accum: Option<A>,
    fun: F,
}

pub fn new_bin<A, F>(query_result: BinQueryResult, init: A, fun: F) -> BinReduce<A, F>
    where F: FnMut(A, Row) -> A,
          A: Send + 'static,
{
    BinReduce {
        fut: query_result,
        accum: Some(init),
        fun: fun,
    }
}

impl<A, F> Future for BinReduce<A, F>
where F: FnMut(A, Row) -> A,
      A: Send + 'static,
{
    type Item = (A, Stmt);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.fut.poll()) {
            Some(BinMaybeRow::Row(row)) => {
                let old_acc_val = self.accum.take().unwrap();
                let new_acc_val = (self.fun)(old_acc_val, row);
                self.accum = Some(new_acc_val);
                self.poll()
            },
            Some(BinMaybeRow::End(stmt)) => {
                let acc_val = self.accum.take().unwrap();
                Ok(Async::Ready((acc_val, stmt)))
            },
            None => {
                panic!("reduce called on consumed query result");
            },
        }
    }
}
