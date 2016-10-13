use Conn;

use consts;

use errors::*;

use lib_futures::{
    Async,
    Future,
    Poll,
};
use lib_futures::Async::Ready;
use lib_futures::stream::{
    Stream as StreamTrait,
    StreamFuture,
};

use io::{
    ConnectingStream,
    Stream,
    WritePacket,
};

use Opts;

use proto::{
    ErrPacket,
    HandshakePacket,
    HandshakeResponse,
    Packet,
    PacketType,
};

use super::ReadMaxAllowedPacket;

enum Step {
    ConnectingStream(ConnectingStream),
    ReadHandshake(StreamFuture<Stream>),
    WriteHandshake(WritePacket),
    ReadResponse(StreamFuture<Stream>),
    ReadMaxAllowedPacket(ReadMaxAllowedPacket),
}

enum Out {
    ConnectingStream(Stream),
    ReadHandshake((Option<(Packet, u8)>, Stream)),
    WriteHandshake((Stream, u8)),
    ReadResponse((Option<(Packet, u8)>, Stream)),
    ReadMaxAllowedPacket(Conn),
}

pub struct NewConn {
    step: Step,
    opts: Opts,
    status: consts::StatusFlags,
    id: u32,
    version: (u16, u16, u16),
}

pub fn new(connecting_stream: ConnectingStream, opts: Opts) -> NewConn {
    NewConn {
        opts: opts,
        step: Step::ConnectingStream(connecting_stream),
        status: consts::StatusFlags::empty(),
        id: 0,
        version: (0, 0, 0),
    }
}

impl NewConn {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::ConnectingStream(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ConnectingStream(val)))
            },
            Step::ReadHandshake(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadHandshake(val)))
            },
            Step::WriteHandshake(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::WriteHandshake(val)))
            },
            Step::ReadResponse(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadResponse(val)))
            },
            Step::ReadMaxAllowedPacket(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadMaxAllowedPacket(val)))
            }
        }
    }
}

impl Future for NewConn {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::ConnectingStream(stream) => {
                self.step = Step::ReadHandshake(stream.into_future());
                self.poll()
            },
            Out::ReadHandshake((maybe_packet, stream)) => match maybe_packet {
                Some((packet, seq_id)) => {
                    if packet.is(PacketType::Err) {
                        let err_packet = ErrPacket::new(packet);
                        return Err(ErrorKind::Server(err_packet.unwrap()).into());
                    }
                    let handshake = HandshakePacket::new(packet);
                    self.version = try!(handshake.srv_ver_parsed());
                    self.id = handshake.conn_id();
                    self.status = handshake.status_flags()
                        .unwrap_or(consts::StatusFlags::empty());
                    let handshake_response = HandshakeResponse::new(&handshake,
                                                                    self.opts.get_user(),
                                                                    self.opts.get_pass(),
                                                                    self.opts.get_db_name());
                    let future = stream.write_packet(handshake_response.as_ref().to_vec(),
                                                     seq_id + 1);
                    self.step = Step::WriteHandshake(future);
                    self.poll()
                },
                None => panic!("No handshake!?"),
            },
            Out::WriteHandshake((stream, _)) => { // TODO: mentinon seq_id
                self.step = Step::ReadResponse(stream.into_future());
                self.poll()
            },
            Out::ReadResponse((maybe_packet, stream)) => match maybe_packet {
                Some((packet, seq_id)) => {
                    if packet.is(PacketType::Err) {
                        let err_packet = ErrPacket::new(packet).unwrap();
                        return Err(ErrorKind::Server(err_packet).into());
                    } else {
                        let conn = Conn {
                            last_command: consts::Command::COM_PING,
                            capabilities: consts::CLIENT_PROTOCOL_41 |
                                consts::CLIENT_SECURE_CONNECTION |
                                consts::CLIENT_LONG_PASSWORD |
                                consts::CLIENT_TRANSACTIONS |
                                consts::CLIENT_LOCAL_FILES |
                                consts::CLIENT_MULTI_STATEMENTS |
                                consts::CLIENT_MULTI_RESULTS |
                                consts::CLIENT_PS_MULTI_RESULTS,
                            status: self.status,
                            last_insert_id: 0,
                            affected_rows: 0,
                            stream: Some(stream),
                            seq_id: seq_id,
                            max_allowed_packet: 65536,
                            warnings: 0,
                            version: self.version,
                            id: self.id,
                            has_result: None,
                        };
                        self.step = Step::ReadMaxAllowedPacket(conn.read_max_allowed_packet());
                        self.poll()
                    }
                },
                None => panic!("No handshake response!?"),
            },
            Out::ReadMaxAllowedPacket(conn) => {
                Ok(Async::Ready(conn))
            },
        }
    }
}
