use conn::Conn;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use io;


/// Futures that sends packet to a server and resolves to `Conn`.
pub struct WritePacket {
    conn: Option<Conn>,
    future: io::futures::WritePacket,
}

pub fn new(conn: Conn, future: io::futures::WritePacket) -> WritePacket {
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
                Ok(Ready(conn))
            }
        }
    }
}
