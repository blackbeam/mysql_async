use Conn;
use Stmt;

use either::{
    Either,
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
    TextQueryResult,
};

pub struct ForEach<F> {
    stream: TextQueryResult,
    fun: F,
}

pub fn new<F>(stream: TextQueryResult, fun: F) -> ForEach<F>
    where F: FnMut(Row),
{
    ForEach {
        stream: stream,
        fun: fun,
    }
}

impl<F> Future for ForEach<F>
where F: FnMut(Row),
{
    type Item = Either<TextQueryResult, Conn>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.stream.poll()) {
            Some(MaybeRow::Row(row)) => {
                (&mut self.fun)(row);
                self.poll()
            },
            Some(MaybeRow::End(query_result_or_conn)) => {
                Ok(Async::Ready(query_result_or_conn))
            },
            None => panic!("pooled twice"),
        }
    }
}

pub struct BinForEach<F> {
    stream: BinQueryResult,
    fun: F,
}

pub fn new_bin<F>(stream: BinQueryResult, fun: F) -> BinForEach<F>
    where F: FnMut(Row),
{
    BinForEach {
        stream: stream,
        fun: fun,
    }
}

impl<F> Future for BinForEach<F>
where F: FnMut(Row),
{
    type Item = Stmt;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.stream.poll()) {
            Some(BinMaybeRow::Row(row)) => {
                (&mut self.fun)(row);
                self.poll()
            },
            Some(BinMaybeRow::End(stmt)) => {
                Ok(Async::Ready(stmt))
            },
            None => panic!("pooled twice"),
        }
    }
}
