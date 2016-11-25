// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::NewConn;
use conn::futures::ReadPacket;
use conn::futures::WritePacket;
use conn::pool::Pool;
use consts::Command::COM_RESET_CONNECTION;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::PacketType;


steps! {
    Reset {
        WritePacket(WritePacket),
        ReadResponse(ReadPacket),
        NewConn(NewConn),
    }
}

/// Future that resolves to a `Conn` with MySql's `COM_RESET_CONNECTION` executed on it.
pub struct Reset {
    step: Step,
    pool: Option<Pool>,
}

pub fn new(conn: Conn) -> Reset {
    let (pool, step) = if conn.version > (5, 7, 2) {
        (None, Step::WritePacket(conn.write_command_data(COM_RESET_CONNECTION, &[])))
    } else {
        (conn.pool.clone(), Step::NewConn(Conn::new(conn.opts.clone(), &conn.handle)))
    };
    Reset {
        step: step,
        pool: pool,
    }
}

impl Future for Reset {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::WritePacket(conn) => {
                self.step = Step::ReadResponse(conn.read_packet());
                self.poll()
            }
            Out::ReadResponse((mut conn, packet)) => {
                if packet.is(PacketType::Ok) {
                    conn.stmt_cache.clear();
                    conn.last_insert_id = 0;
                    conn.affected_rows = 0;
                    conn.warnings = 0;
                    Ok(Ready(conn))
                } else {
                    Err(ErrorKind::UnexpectedPacket.into())
                }
            }
            Out::NewConn(mut conn) => {
                conn.pool = self.pool.clone();
                Ok(Ready(conn))
            }
        }
    }
}
