// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::transaction::Transaction;
use conn::futures::query_result::BinQueryResult;
use conn::futures::query_result::InnerQueryResult;
use conn::futures::query_result::QueryResult;
use conn::futures::query_result::QueryResultOutput;
use conn::futures::query_result::TextQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use either::*;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use proto::Row;
use proto::OkPacket;


//
// TransBinQueryResult
//

/// It is like `BinQueryResult` for transactions.
pub struct TransBinQueryResult(BinQueryResult);

pub fn new_bin(query_result: BinQueryResult) -> TransBinQueryResult {
    TransBinQueryResult(query_result)
}

impl QueryResult for TransBinQueryResult {}

impl UnconsumedQueryResult for TransBinQueryResult {
    type Output = Transaction;
}

impl QueryResultOutput for Transaction {
    type Result = TransBinQueryResult;
    type Output = Transaction;

    fn into_next_or_output(self, prev: TransBinQueryResult) -> (Self::Result,
                                                                Either<Self::Result, Self::Output>)
    {
        (prev, Right(self))
    }
}

impl InnerQueryResult for TransBinQueryResult {
    #[doc(hidden)]
    fn poll(&mut self) -> Result<Async<Either<Row, <Self as UnconsumedQueryResult>::Output>>>
        where Self: UnconsumedQueryResult,
    {
        let result = try_ready!(self.0.poll());
        match result {
            Left(row) => Ok(Ready(Left(row))),
            Right(stmt) => Ok(Ready(Right(Transaction::new_raw(stmt.unwrap())))),
        }
    }

    #[doc(hidden)]
    fn ok_packet_ref(&self) -> Option<&OkPacket> {
        self.0.ok_packet_ref()
    }
}


//
// TransTextQueryResult
//

/// It is like `TextQueryResult` for transactions.
pub struct TransTextQueryResult(TextQueryResult);

pub fn new_text(query_result: TextQueryResult) -> TransTextQueryResult {
    TransTextQueryResult(query_result)
}

impl QueryResult for TransTextQueryResult {}

impl InnerQueryResult for TransTextQueryResult {
    #[doc(hidden)]
    fn poll(&mut self) -> Result<Async<Either<Row, <Self as UnconsumedQueryResult>::Output>>>
        where Self: UnconsumedQueryResult,
    {
        let result = try_ready!(self.0.poll());
        match result {
            Left(row) => Ok(Ready(Left(row))),
            Right(Left(text_query_result)) => Ok(Ready(Right(Left(new_text(text_query_result))))),
            Right(Right(conn)) => Ok(Ready(Right(Right(Transaction::new_raw(conn))))),
        }
    }

    #[doc(hidden)]
    fn ok_packet_ref(&self) -> Option<&OkPacket> {
        self.0.ok_packet_ref()
    }
}

impl UnconsumedQueryResult for TransTextQueryResult {
    type Output = Either<TransTextQueryResult, Transaction>;
}

impl QueryResultOutput for Either<TransTextQueryResult, Transaction> {
    type Result = TransTextQueryResult;
    type Output = Transaction;

    fn into_next_or_output(self, prev: TransTextQueryResult) -> (Self::Result,
                                                                 Either<Self::Result, Self::Output>)
    {
        (prev, self)
    }
}
