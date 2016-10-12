use Conn;

use errors::*;

use futures::First;

use lib_futures::{
    Async,
    Future,
    Poll,
};

pub struct ReadMaxAllowedPacket {
    future: First<(u64,)>,
}

pub fn new(future: First<(u64,)>) -> ReadMaxAllowedPacket {
    ReadMaxAllowedPacket {
        future: future,
    }
}

impl Future for ReadMaxAllowedPacket {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.future.poll()) {
            (maybe_row, mut conn) => {
                conn.max_allowed_packet = maybe_row.map(|x| x.0).unwrap_or(1024 * 1024 * 2);
                Ok(Async::Ready(conn))
            },
        }
    }
}
