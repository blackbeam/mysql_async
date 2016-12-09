// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::Query;
use conn::futures::query_result::futures::DropResult;
use conn::futures::query_result::TextQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use conn::transaction::IsolationLevel;
use conn::transaction::Transaction;
use either::*;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Failed;
use lib_futures::failed;
use lib_futures::Future;
use lib_futures::Poll;


steps! {
    StartTransaction {
        Failed(Failed<(), Error>),
        SetIsolationLevel(Query),
        DropSetIsolationLevel(DropResult<TextQueryResult>),
        SetReadOnly(Query),
        DropSetReadOnly(DropResult<TextQueryResult>),
        StartTransaction(Query),
        DropStartTransaction(DropResult<TextQueryResult>),
    }
}

/// Future that starts transaction and resolves to `Transaction`.
pub struct StartTransaction {
    step: Step,
    consistent_snapshot: bool,
    readonly: Option<bool>,
}

fn set_isolation_level(conn: Conn, isolation_level: Option<IsolationLevel>) -> Either<Step, Conn> {
    if let Some(isolation_level) = isolation_level {
        let query = conn.query(format!("SET TRANSACTION ISOLATION LEVEL {}", isolation_level));
        Left(Step::SetIsolationLevel(query))
    } else {
        Right(conn)
    }
}

fn start_trans(conn: Conn, consistent_snapshot: bool) -> Step {
    let query = if consistent_snapshot {
        conn.query("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    } else {
        conn.query("START TRANSACTION")
    };
    Step::StartTransaction(query)
}

fn set_read_only(conn: Conn, readonly: Option<bool>) -> Either<Step, Conn> {
    if let Some(readonly) = readonly {
        let query = if readonly {
            conn.query("SET TRANSACTION READ ONLY")
        } else {
            conn.query("SET TRANSACTION READ WRITE")
        };
        Left(Step::SetReadOnly(query))
    } else {
        Right(conn)
    }
}

pub fn new(conn: Conn,
           consistent_snapshot: bool,
           isolation_level: Option<IsolationLevel>,
           readonly: Option<bool>)
           -> StartTransaction {
    if readonly.is_some() {
        if conn.version < (5, 6, 5) {
            return StartTransaction {
                step: Step::Failed(failed(ErrorKind::ReadOnlyTransNotSupported.into())),
                consistent_snapshot: false,
                readonly: None,
            };
        }
    }

    let step = set_isolation_level(conn, isolation_level).either(|step| step, |conn| {
        set_read_only(conn, readonly)
            .either(|step| step, |conn| start_trans(conn, consistent_snapshot))
    });

    StartTransaction {
        step: step,
        consistent_snapshot: consistent_snapshot,
        readonly: readonly,
    }
}

impl Future for StartTransaction {
    type Item = Transaction;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::SetIsolationLevel(result) => {
                self.step = Step::DropSetIsolationLevel(result.drop_result());
                self.poll()
            },
            Out::DropSetIsolationLevel(conn) => {
                let step = set_read_only(conn, self.readonly).either(|step| step, |conn| {
                    start_trans(conn, self.consistent_snapshot)
                });
                self.step = step;
                self.poll()
            },
            Out::SetReadOnly(result) => {
                self.step = Step::DropSetReadOnly(result.drop_result());
                self.poll()
            },
            Out::DropSetReadOnly(conn) => {
                let step = start_trans(conn, self.consistent_snapshot);
                self.step = step;
                self.poll()
            },
            Out::StartTransaction(result) => {
                self.step = Step::DropStartTransaction(result.drop_result());
                self.poll()
            },
            Out::DropStartTransaction(conn) => Ok(Ready(Transaction::new(conn))),
            Out::Failed(_) => unreachable!(),
        }
    }
}
