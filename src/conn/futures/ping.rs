use Conn;
use conn::futures::read_packet::ReadPacket;
use conn::futures::write_packet::WritePacket;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::PacketType;


steps! {
    Ping {
        WritePacket(WritePacket),
        ReadPacket(ReadPacket),
    }
}

/// Future that resolves to `Conn` if MySql's `COM_PING` was executed successfully.
pub struct Ping {
    step: Step,
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
                self.step = Step::ReadPacket(conn.read_packet());
                self.poll()
            },
            Out::ReadPacket((conn, packet)) => {
                if packet.is(PacketType::Ok) {
                    Ok(Ready(conn))
                } else {
                    Err(ErrorKind::UnexpectedPacket.into())
                }
            },
        }
    }
}
