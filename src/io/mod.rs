use errors::*;
use std::fmt;
use lib_futures::Async::NotReady;
use lib_futures::Async::Ready;
use lib_futures::Poll;
use lib_futures::stream;
use io::futures::ConnectingStream;
use io::futures::new_connecting_stream;
use io::futures::new_write_packet;
use io::futures::WritePacket;
use proto::NewPacket;
use proto::Packet;
use proto::ParseResult;
use std::cmp;
use std::collections::vec_deque::VecDeque;
use std::io;
use std::io::Read;
use std::net::ToSocketAddrs;
use tokio::net::TcpStream;
use tokio::reactor::Handle;


pub mod futures;


/// Stream connected to MySql server.
pub struct Stream {
    endpoint: Option<TcpStream>,
    closed: bool,
    next_packet: Option<ParseResult>,
    buf: Option<VecDeque<u8>>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Stream (endpoint={:?})", self.endpoint)
    }
}

impl Stream {
    pub fn connect<S>(addr: S, handle: &Handle) -> ConnectingStream
        where S: ToSocketAddrs,
    {
        new_connecting_stream(addr, handle)
    }

    pub fn write_packet(self, data: Vec<u8>, seq_id: u8) -> WritePacket {
        new_write_packet(self, data, seq_id)
    }
}

impl io::Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.endpoint.as_mut().unwrap().read(buf)
    }
}

impl io::Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.endpoint.as_mut().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.endpoint.as_mut().unwrap().flush()
    }
}

impl stream::Stream for Stream {
    type Item = (Packet, u8);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<(Packet, u8)>, Error> {
        // should read everything from self.endpoint
        if ! self.closed {
            let mut buf = [0u8; 4096];
            loop {
                match self.endpoint.as_mut().unwrap().read(&mut buf[..]) {
                    Ok(0) => {
                        break;
                    },
                    Ok(size) => {
                        let buf_handle = self.buf.as_mut().unwrap();
                        buf_handle.extend(&buf[..size]);
                    },
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        break;
                    },
                    Err(error) => {
                        self.closed = true;
                        return Err(Error::from(error))
                    },
                };
            }
        } else {
            return Ok(Ready(None))
        }

        // need to call again if there is a data in self.buf
        // or data was written to packet parser
        let mut should_poll = false;

        let next_packet = self.next_packet.take().expect("Stream.next_packet should not be None");
        let next_packet = match next_packet {
            ParseResult::Done(packet, seq_id) => {
                self.next_packet = Some(NewPacket::empty().parse());
                return Ok(Ready(Some((packet, seq_id))));
            },
            ParseResult::NeedHeader(mut new_packet, needed) => {
                {
                    let buf_handle = self.buf.as_mut().unwrap();
                    let buf_len = buf_handle.len();
                    for byte in buf_handle.drain(..cmp::min(needed, buf_len)) {
                        new_packet.push_header(byte);
                    }
                    if buf_len != 0 {
                        should_poll = true;
                    }

                    new_packet
                }
            }
            ParseResult::Incomplete(mut new_packet, needed) => {
                {
                    let buf_handle = self.buf.as_mut().unwrap();
                    let buf_len = buf_handle.len();
                    for byte in buf_handle.drain(..cmp::min(needed, buf_len)) {
                        new_packet.push(byte);
                    }
                    if buf_len != 0 {
                        should_poll = true;
                    }

                    new_packet
                }
            }
        };

        self.next_packet = Some(next_packet.parse());

        if should_poll {
            self.poll()
        } else {
            Ok(NotReady)
        }
    }
}

#[cfg(test)]
mod tests {
    use io::Stream;

    use Opts;

    use proto::{HandshakePacket, HandshakeResponse};

    use lib_futures::Future;
    use lib_futures::stream::Stream as FuturesStream;

    use test_misc::DATABASE_URL;

    use tokio::reactor::Core;

    #[test]
    fn should_connect_stream() {
        let mut lp = Core::new().unwrap();

        let opts: Opts = (&**DATABASE_URL).into();
        let ip = opts.get_ip_or_hostname();
        let port = opts.get_tcp_port();

        let stream = Stream::connect((ip, port), &lp.handle()).and_then(|stream: Stream| {
            stream.into_future().map_err(|(err, _)| err)
        }).and_then(|(maybe_packet, stream)| {
            let (packet, _) = maybe_packet.expect("no handshake!?");
            let handshake = HandshakePacket::new(packet);
            let user = "root";
            let pass = "password";
            let handshake_response = HandshakeResponse::new(&handshake,
                                                            Some(user.as_bytes()),
                                                            Some(pass.as_bytes()),
                                                            None::<Vec<u8>>);
            stream.write_packet(handshake_response.as_ref().to_vec(), 1)
        }).and_then(|(stream, _)| {
            stream.into_future().map_err(|(err, _)| err)
        }).map(|(maybe_packet, _)| {
            let (_, _) = maybe_packet.expect("Should be here");
        });

        lp.run(stream).unwrap();
    }
}
