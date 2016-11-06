use conn::Conn;
use conn::futures::query_result::*;
use conn::futures::query_result::futures::CollectAll;
use conn::stmt::InnerStmt;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::Column;
use proto::OkPacket;
use std::mem;
use std::sync::Arc;

enum Step {
    CollectText(CollectAll<TextQueryResult>),
    CollectBin(CollectAll<BinQueryResult>),
    Done(Conn),
    Consumed,
}

enum Out {
    Done(Conn),
}

pub struct DropResult {
    step: Step,
}

pub fn new(conn: Conn,
           has_result: Option<(Arc<Vec<Column>>, Option<OkPacket>, Option<InnerStmt>)>) -> DropResult
{
    let step;
    if let Some((cols, ok_packet, inner_stmt)) = has_result {
        if let Some(inner_stmt) = inner_stmt {
            let raw_result = new_raw::<BinaryResult, _>(conn, cols, ok_packet, Some(inner_stmt));
            step = Step::CollectBin(BinQueryResult::from(raw_result).collect_all());
        } else {
            let raw_result = new_raw::<TextResult, _>(conn, cols, ok_packet, None);
            step = Step::CollectText(TextQueryResult::from(raw_result).collect_all());
        }
    } else {
        step = Step::Done(conn);
    }
    DropResult {
        step: step,
    }
}

impl DropResult {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::CollectText(ref mut fut) => {
                let (_, conn) = try_ready!(fut.poll());
                Ok(Ready(Out::Done(conn)))
            },
            Step::CollectBin(ref mut fut) => {
                let (_, stmt) = try_ready!(<CollectAll<BinQueryResult> as Future>::poll(fut));
                Ok(Ready(Out::Done(stmt.unwrap())))
            },
            Step::Done(_) => {
                if let Step::Done(conn) = mem::replace(&mut self.step, Step::Consumed) {
                    Ok(Ready(Out::Done(conn)))
                } else {
                    unreachable!()
                }
            },
            Step::Consumed => panic!("DropResult polled twice"),
        }
    }
}

impl Future for DropResult {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::Done(conn) => {
                self.step = Step::Consumed;
                Ok(Ready(conn))
            },
        }
    }
}
