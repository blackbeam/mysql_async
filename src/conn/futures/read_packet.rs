use conn::Conn;
use errors::*;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use lib_futures::stream::StreamFuture;
use io::Stream;
use proto::EofPacket;
use proto::ErrPacket;
use proto::OkPacket;
use proto::Packet;
use proto::PacketType;


/// Future that resolves to a pair of `Conn` and `Packet` which was read from it.
pub struct ReadPacket {
    conn: Option<Conn>,
    future: StreamFuture<Stream>,
}

pub fn new(conn: Conn, future: StreamFuture<Stream>) -> ReadPacket {
    ReadPacket {
        conn: Some(conn),
        future: future,
    }
}

impl Future for ReadPacket {
    type Item = (Conn, Packet);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.future.poll()) {
            (maybe_packet, stream) => match maybe_packet {
                Some((packet, _)) => { // TODO: take seq_id to account
                    let packet = {
                        let conn = self.conn.as_mut().unwrap();
                        conn.stream = Some(stream);
                        if packet.is(PacketType::Ok) {
                            let ok_packet = OkPacket::new(packet, conn.capabilities)
                                .expect("OK packet is not OK packet!?");
                            conn.affected_rows = ok_packet.affected_rows();
                            conn.last_insert_id = ok_packet.last_insert_id();
                            conn.status = ok_packet.status_flags();
                            conn.warnings = ok_packet.warnings();
                            ok_packet.unwrap()
                        } else if packet.is(PacketType::Eof) {
                            let eof_packet = EofPacket::new(packet)
                                .expect("EOF packet is not EOF packet!?");
                            conn.warnings = eof_packet.warnings();
                            conn.status = eof_packet.status_flags();
                            eof_packet.unwrap()
                        } else if packet.is(PacketType::Err) {
                            let err_packet = ErrPacket::new(packet)
                                .expect("ERR packet is not ERR packet!?");
                            return Err(ErrorKind::Server(err_packet).into())
                        } else {
                            packet
                        }
                    };
                    Ok(Ready((self.conn.take().unwrap(), packet)))
                },
                None => Err(ErrorKind::ConnectionClosed.into()),
            },
        }
    }
}
