use conn::Conn;
use conn::futures::query_result::BinQueryResult;
use conn::futures::Prepare;
use conn::stmt::futures::Execute;
use conn::stmt::Stmt;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use value::Params;


enum Step {
    Prepare(Prepare),
    Execute(Execute),
}

enum Out {
    Prepare(Stmt),
    Execute(BinQueryResult),
}

pub struct PrepExec {
    step: Step,
    params: Option<Params>,
}

pub fn new<Q, P>(conn: Conn, query: Q, params: P) -> PrepExec
    where Q: AsRef<str>,
          P: Into<Params>,
{
    PrepExec {
        step: Step::Prepare(conn.prepare(query)),
        params: Some(params.into()),
    }
}

impl PrepExec {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::Prepare(ref mut fut) => Ok(Ready(Out::Prepare(try_ready!(fut.poll())))),
            Step::Execute(ref mut fut) => Ok(Ready(Out::Execute(try_ready!(fut.poll())))),
        }
    }
}

impl Future for PrepExec {
    type Item = BinQueryResult;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::Prepare(stmt) => {
                let params = self.params.take().unwrap();
                self.step = Step::Execute(stmt.execute(params));
                self.poll()
            },
            Out::Execute(query_result) => Ok(Ready(query_result)),
        }
    }
}
