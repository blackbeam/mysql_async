use conn::futures::query_result::BinQueryResult;
use conn::futures::query_result::futures::Collect;
use conn::futures::query_result::ResultSet;
use conn::futures::query_result::UnconsumedQueryResult;
use conn::stmt::futures::Execute;
use conn::stmt::Stmt;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use value::FromRow;
use value::Params;


enum Step<R> {
    Execute(Execute),
    Collect(Collect<R, BinQueryResult>),
}

enum Out<R> {
    Execute(BinQueryResult),
    Collect((ResultSet<R, BinQueryResult>, Stmt))
}

pub struct StmtFirst<R> {
    step: Step<R>,
}

pub fn new<R: FromRow>(stmt: Stmt, params: Params) -> StmtFirst<R> {
    StmtFirst {
        step: Step::Execute(stmt.execute(params)),
    }
}

impl<R: FromRow> StmtFirst<R> {
    fn either_poll(&mut self) -> Result<Async<Out<R>>> {
        match self.step {
            Step::Execute(ref mut fut) => Ok(Ready(Out::Execute(try_ready!(fut.poll())))),
            Step::Collect(ref mut fut) => Ok(Ready(Out::Collect(try_ready!(fut.poll())))),
        }
    }
}

impl<R: FromRow> Future for StmtFirst<R> {
    type Item = (Option<R>, Stmt);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::Execute(query_result) => {
                self.step = Step::Collect(query_result.collect::<R>());
                self.poll()
            },
            Out::Collect((mut result_set, stmt)) => {
                let row = if result_set.0.len() > 0 {
                    Some(result_set.0.swap_remove(0))
                } else {
                    None
                };
                Ok(Ready((row, stmt)))
            }
        }
    }
}
