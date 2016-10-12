use conn::Conn;

use errors::*;

use lib_futures::{
    Async,
    Future,
    Poll,
};

use io;

pub struct WritePacket {
    conn: Option<Conn>,
    future: io::WritePacket,
}

pub fn new(conn: Conn, future: io::WritePacket) -> WritePacket {
    WritePacket {
        conn: Some(conn),
        future: future,
    }
}

impl Future for WritePacket {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.future.poll()) {
            (stream, seq_id) => {
                let mut conn = self.conn.take().unwrap();
                conn.seq_id = seq_id;
                conn.stream = Some(stream);
                Ok(Async::Ready(conn))
            }
        }
    }
}