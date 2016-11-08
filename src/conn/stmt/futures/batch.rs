use conn::futures::query_result::BinQueryResult;
use conn::futures::query_result::futures::DropResult;
use conn::futures::query_result::UnconsumedQueryResult;
use conn::stmt::futures::Execute;
use conn::stmt::Stmt;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::done;
use lib_futures::Done;
use lib_futures::Future;
use lib_futures::Poll;
use std::mem;
use value::Params;

steps! {
    Batch {
        Done(Done<Stmt, Error>),
        Execute(Execute),
        DropResult(DropResult<BinQueryResult>),
    }
}

/// Future that performs batch execution of a statement and resolves to `Stmt`.
///
/// All results will be dropped.
pub struct Batch {
    step: Step,
    params_vec: Vec<Params>,
    current: usize,
}

pub fn new<T: Into<Params>>(stmt: Stmt, params_vec: Vec<T>) -> Batch {
    Batch {
        step: Step::Done(done(Ok(stmt))),
        params_vec: params_vec.into_iter().map(Into::into).collect(),
        current: 0,
    }
}

impl Future for Batch {
    type Item = Stmt;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::Execute(result) => {
                self.step = Step::DropResult(result.drop_result());
                self.poll()
            },
            Out::Done(stmt) | Out::DropResult(stmt) => {
                let current = self.current;
                self.current += 1;
                let params = match self.params_vec.get_mut(current) {
                    Some(params) => mem::replace(params, Params::Empty),
                    None => return Ok(Ready(stmt)),
                };
                self.step = Step::Execute(stmt.execute(params));
                self.poll()
            },
        }
    }
}
