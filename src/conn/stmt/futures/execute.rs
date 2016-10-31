use byteorder::WriteBytesExt;
use byteorder::LittleEndian as LE;
use conn::Conn;
use conn::futures::NewRawQueryResult;
use conn::futures::query_result::BinaryResult;
use conn::futures::query_result::BinQueryResult;
use conn::futures::SendLongData;
use conn::futures::new_send_long_data;
use conn::futures::WritePacket;
use conn::stmt::InnerStmt;
use conn::stmt::new_stmt;
use conn::stmt::Stmt;
use consts::ColumnType;
use consts::Command;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::Column;
use std::io::Write;
use std::mem;
use value::Params;
use value::Value;
use value::Value::*;


enum Step {
    Failed(Option<ErrorKind>),
    SendLongData(SendLongData),
    WriteCommand(WritePacket),
    HandleResultSet(NewRawQueryResult<BinaryResult>),
}

enum Out {
    SendLongData((Conn, InnerStmt, Vec<Value>)),
    WriteCommand(Conn),
    HandleResultSet(BinQueryResult),
}

/// Future that executes statement and resolves to `BinQueryResult`.
pub struct Execute {
    step: Step,
    row_data: Vec<u8>,
    bitmap: Vec<u8>,
    inner_stmt: Option<InnerStmt>,
}

impl Execute {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::Failed(ref mut error_kind) => Err(error_kind.take().unwrap().into()),
            Step::SendLongData(ref mut fut) => {
                Ok(Ready(Out::SendLongData(try_ready!(fut.poll()))))
            },
            Step::WriteCommand(ref mut fut) => {
                Ok(Ready(Out::WriteCommand(try_ready!(fut.poll()))))
            },
            Step::HandleResultSet(ref mut fut) => {
                let raw_query_result = try_ready!(fut.poll());
                let bin_query_result = raw_query_result.into();
                Ok(Ready(Out::HandleResultSet(bin_query_result)))
            }
        }
    }
}

pub fn new_new(stmt: Stmt, params: Params) -> Execute {
    let Stmt {conn, stmt: inner_stmt} = stmt;
    let mut data: Vec<u8>;
    match params {
        Params::Empty => {
            if inner_stmt.num_params != 0 {
                return Execute {
                    step: Step::Failed(Some(ErrorKind::MismatchedStmtParams(inner_stmt.num_params, 0))),
                    row_data: vec![],
                    bitmap: vec![],
                    inner_stmt: None,
                };
            }

            data = Vec::with_capacity(4 + 1 + 4);
            data.write_u32::<LE>(inner_stmt.statement_id).unwrap();
            data.write_u8(0u8).unwrap();
            data.write_u32::<LE>(1u32).unwrap();
        },
        Params::Positional(params) => {
            if inner_stmt.num_params != params.len() as u16 {
                return Execute {
                    step: Step::Failed(Some(ErrorKind::MismatchedStmtParams(inner_stmt.num_params,
                                                                            params.len() as u16))),
                    row_data: vec![],
                    bitmap: vec![],
                    inner_stmt: None,
                };
            }

            let to_payload_result = if let Some(ref sparams) = inner_stmt.params {
                Value::to_bin_payload(sparams.as_ref(), &params, conn.max_allowed_packet as usize)
            } else {
                unreachable!();
            };

            match to_payload_result {
                Ok((bitmap, row_data, Some(large_ids))) => {
                    let step = Step::SendLongData(new_send_long_data(conn,
                                                                     inner_stmt,
                                                                     params,
                                                                     large_ids));
                    return Execute {
                        step: step,
                        row_data: row_data,
                        bitmap: bitmap,
                        inner_stmt: None,
                    };
                },
                Ok((bitmap, row_data, None)) => {
                    let sparams = inner_stmt.params.as_ref().unwrap();
                    data = Vec::new();
                    write_data(&mut data, inner_stmt.statement_id, bitmap, row_data, params, sparams);
                },
                Err(err) => {
                    return Execute {
                        step: Step::Failed(Some(err.into_kind())),
                        row_data: vec![],
                        bitmap: vec![],
                        inner_stmt: None,
                    };
                }
            }
        },
        Params::Named(_) => {
            if let None = inner_stmt.named_params {
                return Execute {
                    step: Step::Failed(Some(ErrorKind::NamedParamsForPositionalQuery)),
                    row_data: vec![],
                    bitmap: vec![],
                    inner_stmt: None,
                };
            }
            let result = {
                let named_params = inner_stmt.named_params.as_ref().unwrap();
                params.into_positional(named_params)
            };

            match result {
                Ok(positional_params) => {
                    return new_new(new_stmt(inner_stmt, conn), positional_params)
                },
                Err(err) => {
                    return Execute {
                        step: Step::Failed(Some(err.into_kind())),
                        row_data: vec![],
                        bitmap: vec![],
                        inner_stmt: None,
                    };
                }
            }
        }
    }
    let future = conn.write_command_data(Command::COM_STMT_EXECUTE, data);
    Execute {
        step: Step::WriteCommand(future),
        row_data: vec![],
        bitmap: vec![],
        inner_stmt: Some(inner_stmt),
    }
}

impl Future for Execute {
    type Item = BinQueryResult;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::SendLongData((conn, inner_stmt, params)) => {
                let bitmap = mem::replace(&mut self.bitmap, vec![]);
                let row_data = mem::replace(&mut self.row_data, vec![]);
                let mut data = Vec::new();
                {
                    let sparams = inner_stmt.params.as_ref().unwrap();
                    write_data(&mut data,
                               inner_stmt.statement_id,
                               bitmap,
                               row_data,
                               params,
                               sparams);
                }
                let future = conn.write_command_data(Command::COM_STMT_EXECUTE, data);
                self.step = Step::WriteCommand(future);
                self.inner_stmt = Some(inner_stmt);
                self.poll()
            },
            Out::WriteCommand(conn) => {
                let inner_stmt = self.inner_stmt.as_ref().cloned();
                let new_raw_query_result = conn.handle_result_set::<BinaryResult>(inner_stmt);
                self.step = Step::HandleResultSet(new_raw_query_result);
                self.poll()
            },
            Out::HandleResultSet(query_result) => Ok(Ready(query_result)),
        }
    }
}

fn write_data(writer: &mut Vec<u8>,
              stmt_id: u32,
              bitmap: Vec<u8>,
              row_data: Vec<u8>,
              params: Vec<Value>,
              sparams: &Vec<Column>)
{
    let capacity = 9 + bitmap.len() + 1 + params.len() * 2 + row_data.len();
    writer.reserve(capacity);
    writer.write_u32::<LE>(stmt_id).unwrap();
    writer.write_u8(0u8).unwrap();
    writer.write_u32::<LE>(1u32).unwrap();
    writer.write_all(bitmap.as_ref()).unwrap();
    writer.write_u8(1u8).unwrap();
    for i in 0..params.len() {
        let result = match params[i] {
            NULL => writer.write_all( &[sparams[i].column_type as u8, 0u8]),
            Bytes(..) => {
                writer.write_all(&[ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0u8])
            },
            Int(..) => {
                writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8, 0u8])
            },
            UInt(..) => {
                writer.write_all(&[ColumnType::MYSQL_TYPE_LONGLONG as u8, 128u8])
            },
            Float(..) => {
                writer.write_all(&[ColumnType::MYSQL_TYPE_DOUBLE as u8, 0u8])
            },
            Date(..) => {
                writer.write_all(&[ColumnType::MYSQL_TYPE_DATETIME as u8, 0u8])
            },
            Time(..) => {
                writer.write_all(&[ColumnType::MYSQL_TYPE_TIME as u8, 0u8])
            },
        };
        result.unwrap();
    }
    writer.write_all(row_data.as_ref()).unwrap();
}
