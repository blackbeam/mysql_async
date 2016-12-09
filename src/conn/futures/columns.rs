// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::read_packet::ReadPacket;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::Column;
use std::mem;


/// Future that resolves to a vector of columns of a result set.
///
/// It is a part of a result set.
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
                    Ok(Ready((conn, columns)))
                } else {
                    let column = Column::new(packet, conn.last_command);
                    self.columns.push(column);
                    self.future = conn.read_packet();
                    self.poll()
                }
            },
        }
    }
}
