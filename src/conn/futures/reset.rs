use conn::Conn;
use conn::futures::read_packet::ReadPacket;
use conn::futures::write_packet::WritePacket;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use proto::Packet;
use proto::PacketType;


enum Step {
    WritePacket(WritePacket),
    ReadResponse(ReadPacket),
}

enum Out {
    WritePacket(Conn),
    ReadResponse((Conn, Packet)),
}

/// Future that resolves to a `Conn` with MySql's `COM_RESET_CONNECTION` executed on it.
pub struct Reset {
    step: Step,
}

impl Reset {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::WritePacket(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::WritePacket(val)))
            },
            Step::ReadResponse(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadResponse(val)))
            },
        }
    }
}

pub fn new(write_packet: WritePacket) -> Reset {
    Reset {
        step: Step::WritePacket(write_packet),
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
            },
            Out::ReadResponse((mut conn, packet)) => {
                if packet.is(PacketType::Ok) {
                    // TODO: Clear stmt cache
                    conn.last_insert_id = 0;
                    conn.affected_rows = 0;
                    conn.warnings = 0;
                    Ok(Ready(conn))
                } else {
                    Err(ErrorKind::UnexpectedPacket.into())
                }
            },
        }
    }
}
