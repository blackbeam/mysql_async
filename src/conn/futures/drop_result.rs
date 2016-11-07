use conn::Conn;
use conn::futures::query_result::*;
use conn::futures::query_result::futures::DropResult as DropQueryResult;
use conn::stmt::InnerStmt;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use lib_futures::Done;
use lib_futures::done;
use proto::Column;
use proto::OkPacket;
use std::sync::Arc;

steps! {
    DropResult {
        DropTextResult(DropQueryResult<TextQueryResult>),
        DropBinResult(DropQueryResult<BinQueryResult>),
        Done(Done<Conn, Error>),
    }
}

pub struct DropResult {
    step: Step,
}

pub fn new(conn: Conn,
           has_result: Option<(Arc<Vec<Column>>, Option<OkPacket>, Option<InnerStmt>)>) -> DropResult
{
    let step = if let Some((cols, ok_packet, inner_stmt)) = has_result {
        if let Some(inner_stmt) = inner_stmt {
            let raw_result = new_raw::<BinaryResult, _>(conn, cols, ok_packet, Some(inner_stmt));
            Step::DropBinResult(BinQueryResult::from(raw_result).drop_result())
        } else {
            let raw_result = new_raw::<TextResult, _>(conn, cols, ok_packet, None);
            Step::DropTextResult(TextQueryResult::from(raw_result).drop_result())
        }
    } else {
        Step::Done(done(Ok(conn)))
    };
    DropResult {
        step: step,
    }
}

impl Future for DropResult {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::DropTextResult(conn) => Ok(Ready(conn)),
            Out::DropBinResult(stmt) => Ok(Ready(stmt.unwrap())),
            Out::Done(conn) => Ok(Ready(conn)),
        }
    }
}
