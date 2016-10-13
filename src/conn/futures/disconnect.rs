use errors::*;

use lib_futures::{
    Future,
    Poll,
};
use lib_futures::Async::Ready;

use super::{
    WritePacket,
};

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
