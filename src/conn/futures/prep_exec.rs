use conn::Conn;
use conn::futures::query_result::BinQueryResult;
use conn::futures::Prepare;
use conn::stmt::futures::Execute;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use std::mem;
use value::Params;


steps! {
    PrepExec {
        Prepare(Prepare),
        Execute(Execute),
    }
}

/// This future will prepare statement, execute it and resolve to `BinQueryResult`.
pub struct PrepExec {
    step: Step,
    params: Params,
}

pub fn new<Q, P>(conn: Conn, query: Q, params: P) -> PrepExec
    where Q: AsRef<str>,
          P: Into<Params>,
{
    PrepExec {
        step: Step::Prepare(conn.prepare(query)),
        params: params.into(),
    }
}

impl Future for PrepExec {
    type Item = BinQueryResult;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::Prepare(stmt) => {
                let params = mem::replace(&mut self.params, Params::Empty);
                self.step = Step::Execute(stmt.execute(params));
                self.poll()
            },
            Out::Execute(query_result) => Ok(Ready(query_result)),
        }
    }
}
