use conn::Conn;

use errors::*;

use lib_futures::{
    Async,
    Future,
    Poll,
};

use proto::Column;

use std::mem;

use super::ReadPacket;

pub struct Columns {
    future: ReadPacket,
    count: u64,
    columns: Vec<Column>,
}

pub fn new(future: ReadPacket, column_count: u64) -> Columns {
    Columns {
        future: future,
        count: column_count,
        columns: Vec::with_capacity(column_count as usize),
    }
}

impl Future for Columns {
    type Item = (Conn, Vec<Column>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.future.poll()) {
            (conn, packet) => {
                if self.count == self.columns.len() as u64 {
                    let columns = mem::replace(&mut self.columns, Vec::new());
                    Ok(Async::Ready((conn, columns)))
                } else {
                    let column = Column::new(packet, conn.last_command);
                    self.columns.push(column);
                    self.future = conn.read_packet();
                    self.poll()
                }
            }
        }
    }
}