use conn::futures::write_packet::WritePacket;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;


/// Future that disconnects `Conn` from server and consumes it.
///
/// Resolves to `()`.
pub struct Disconnect {
    future: WritePacket,
}

pub fn new(write_packet: WritePacket) -> Disconnect {
    Disconnect {
        future: write_packet,
    }
}

impl Future for Disconnect {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.future.poll()) {
            _ => Ok(Ready(()))
        }
    }
}
