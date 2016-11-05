use Conn;
use conn::futures::Query;
use conn::futures::query_result::futures::CollectAll;
use conn::futures::query_result::TextQueryResult;
use conn::futures::query_result::UnconsumedQueryResult;
use conn::futures::read_max_allowed_packet::ReadMaxAllowedPacket;
use consts;
use errors::*;
use io::futures::ConnectingStream;
use io::futures::WritePacket;
use io::Stream;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;
use lib_futures::stream::Stream as StreamTrait;
use lib_futures::stream::StreamFuture;
use Opts;
use proto::ErrPacket;
use proto::HandshakePacket;
use proto::HandshakeResponse;
use proto::PacketType;


/// Future that resolves to a `Conn`.
pub struct NewConn {
    step: Step,
    opts: Opts,
    status: consts::StatusFlags,
    id: u32,
    version: (u16, u16, u16),
    init_len: usize,
}

steps! {
    NewConn {
        ConnectingStream(ConnectingStream),
        ReadHandshake(StreamFuture<Stream>),
        WriteHandshake(WritePacket),
        ReadResponse(StreamFuture<Stream>),
        ReadMaxAllowedPacket(ReadMaxAllowedPacket),
        Query(Query),
        CollectAll(CollectAll<TextQueryResult>),
    }
}

pub fn new(connecting_stream: ConnectingStream, opts: Opts) -> NewConn {
    NewConn {
        init_len: opts.get_init().len(),
        opts: opts,
        step: Step::ConnectingStream(connecting_stream),
        status: consts::StatusFlags::empty(),
        id: 0,
        version: (0, 0, 0),
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
            Out::WriteHandshake((stream, _)) => { // TODO: take seq_id to account
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
                            pool: None,
                        };
                        self.step = Step::ReadMaxAllowedPacket(conn.read_max_allowed_packet());
                        self.poll()
                    }
                },
                None => panic!("No handshake response!?"),
            },
            Out::ReadMaxAllowedPacket(conn) => {
                let step = match self.opts.get_init().get(self.opts.get_init().len() - self.init_len) {
                    Some(query) => Step::Query(conn.query(query)),
                    None => return Ok(Ready(conn)),
                };
                self.step = step;
                self.init_len -= 1;
                self.poll()
            },
            Out::Query(query_result) => {
                self.step = Step::CollectAll(query_result.collect_all());
                self.poll()
            },
            Out::CollectAll((_, conn)) => {
                let step = match self.opts.get_init().get(self.opts.get_init().len() - self.init_len) {
                    Some(query) => Step::Query(conn.query(query)),
                    None => return Ok(Ready(conn)),
                };
                self.step = step;
                self.init_len -= 1;
                self.poll()
            }
        }
    }
}
