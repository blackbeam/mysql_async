use Column;
use Conn;

use consts;

use errors::*;

use lib_futures::Async::*;
use lib_futures::{
    Async,
    Future,
    Poll,
};

use proto::{
    Packet,
    PacketType,
};

use conn::stmt::{
    InnerStmt,
    new_stmt,
    Stmt,
};

use std::mem;

use super::{
    ReadPacket,
    WritePacket,
};

use super::super::named_params::parse_named_params;

enum Step {
    Failed,
    WriteCommand(WritePacket),
    ReadCommandResponse(ReadPacket),
    ReadParamOrColumn(ReadPacket),
}

enum Out {
    WriteCommand(Conn),
    ReadCommandResponse((Conn, Packet)),
    ReadParamOrColumn((Conn, Packet)),
}

pub struct Prepare {
    step: Step,
    error: Option<Error>,
    params: Vec<Column>,
    columns: Vec<Column>,
    named_params: Option<Vec<String>>,
    stmt: Option<InnerStmt>,
}

impl Prepare {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::Failed => Err(self.error.take().unwrap()),
            Step::WriteCommand(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::WriteCommand(val)))
            },
            Step::ReadCommandResponse(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadCommandResponse(val)))
            },
            Step::ReadParamOrColumn(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadParamOrColumn(val)))
            },
        }
    }
}

pub fn new(conn: Conn, query: &str) -> Prepare {
    match parse_named_params(query) {
        Ok((named_params, query)) => {
            let query = query.into_owned();
            let future = conn.write_command_data(consts::Command::COM_STMT_PREPARE, &*query);
            Prepare {
                step: Step::WriteCommand(future),
                error: None,
                named_params: named_params,
                params: Vec::new(),
                columns: Vec::new(),
                stmt: None,
            }
        },
        Err(err) => {
            Prepare {
                step: Step::Failed,
                error: Some(err),
                named_params: None,
                params: Vec::default(),
                columns: Vec::default(),
                stmt: None,
            }
        },
    }
}

impl Future for Prepare {
    type Item = Stmt;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::WriteCommand(conn) => {
                self.step = Step::ReadCommandResponse(conn.read_packet());
                self.poll()
            },
            Out::ReadCommandResponse((conn, packet)) => {
                let named_params = self.named_params.take();
                let inner_stmt = try!(InnerStmt::new(packet.as_ref(), named_params));
                if inner_stmt.num_params > 0 || inner_stmt.num_columns > 0 {
                    self.params = Vec::with_capacity(inner_stmt.num_params as usize);
                    self.columns = Vec::with_capacity(inner_stmt.num_columns as usize);
                    self.stmt = Some(inner_stmt);
                    self.step = Step::ReadParamOrColumn(conn.read_packet());
                    self.poll()
                } else {
                    let stmt = new_stmt(inner_stmt, conn);
                    Ok(Ready(stmt))
                }
            },
            Out::ReadParamOrColumn((conn, packet)) => {
                if self.params.len() < self.params.capacity() {
                    let param = Column::new(packet, conn.last_command);
                    self.params.push(param);
                    self.step = Step::ReadParamOrColumn(conn.read_packet());
                    self.poll()
                } else if self.columns.len() < self.columns.capacity() {
                    if ! packet.is(PacketType::Eof) {
                        let column = Column::new(packet, conn.last_command);
                        self.columns.push(column);
                    }
                    self.step = Step::ReadParamOrColumn(conn.read_packet());
                    self.poll()
                } else {
                    let mut inner_stmt: InnerStmt = self.stmt.take().unwrap();
                    inner_stmt.params = Some(mem::replace(&mut self.params, vec![]));
                    inner_stmt.columns = Some(mem::replace(&mut self.columns, vec![]));
                    let stmt = new_stmt(inner_stmt, conn);
                    Ok(Ready(stmt))
                }
            },
        }
    }
}
