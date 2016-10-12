use Conn;

use errors::*;

use lib_futures::{
    Async,
    Future,
    Poll,
};

use proto::{
    Packet,
    PacketType,
};

use super::{
    ReadPacket,
    WritePacket,
};

enum Step {
    WritePacket(WritePacket),
    ReadResponse(ReadPacket),
}

enum Out {
    WritePacket(Conn),
    ReadResponse((Conn, Packet)),
}

pub struct Ping {
    step: Step,
}

impl Ping {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::WritePacket(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Async::Ready(Out::WritePacket(val)))
            },
            Step::ReadResponse(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Async::Ready(Out::ReadResponse(val)))
            },
        }
    }
}

pub fn new(write_packet: WritePacket) -> Ping {
    Ping {
        step: Step::WritePacket(write_packet),
    }
}

impl Future for Ping {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::WritePacket(conn) => {
                self.step = Step::ReadResponse(conn.read_packet());
                self.poll()
            },
            Out::ReadResponse((conn, packet)) => {
                if packet.is(PacketType::Ok) {
                    Ok(Async::Ready(conn))
                } else {
                    Err(ErrorKind::UnexpectedPacket.into())
                }
            },
        }
    }
}
