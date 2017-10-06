// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use BoxFuture;
use errors::*;
use std::fmt;
use std::fs::File;
use lib_futures::Async::NotReady;
use lib_futures::Async::Ready;
#[cfg(feature = "ssl")]
use lib_futures::{Future, IntoFuture};
use lib_futures::Poll;
use lib_futures::stream;
use io::futures::ConnectingStream;
use io::futures::new_connecting_stream;
use io::futures::new_write_packet;
use io::futures::WritePacket;
#[cfg(feature = "ssl")]
use native_tls::{Certificate, Pkcs12, TlsConnector};
use myc::packets::{PacketParser, ParseResult, RawPacket};
use opts::SslOpts;
use std::cmp;
use std::io;
use std::io::Read;
use std::net::ToSocketAddrs;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::reactor::Handle;
use tokio_io::AsyncRead;
use tokio_io::AsyncWrite;
#[cfg(feature = "ssl")]
use tokio_tls::{TlsConnectorExt, TlsStream};


pub mod futures;


#[derive(Debug)]
enum Endpoint {
    Plain(TcpStream),
    #[cfg(feature = "ssl")]
    Secure(TlsStream<TcpStream>),
}

impl Endpoint {
    pub fn set_keepalive_ms(&self, ms: Option<u32>) -> Result<()> {
        let ms = ms.map(|val| Duration::from_millis(val as u64));
        match *self {
            Endpoint::Plain(ref stream) => stream.set_keepalive(ms)?,
            #[cfg(feature = "ssl")]
            Endpoint::Secure(ref stream) => stream.get_ref().get_ref().set_keepalive(ms)?,
        }
        Ok(())
    }

    #[cfg(feature = "ssl")]
    pub fn make_secure(self, domain: String, ssl_opts: SslOpts) -> BoxFuture<Self> {
        let fut = ::std::fs::File::open(ssl_opts.pkcs12_path())
            .map_err(Error::from)
            .and_then(|mut file| {
                let mut der = vec![];
                file.read_to_end(&mut der)?;
                let identity = Pkcs12::from_der(&*der, ssl_opts.password().unwrap_or(""))
                    .chain_err(|| "Can't parse der")?;
                let mut builder = TlsConnector::builder().chain_err(
                    || "Can't create TlsConnectorBuilder)",
                )?;
                match ssl_opts.root_cert_path() {
                    Some(root_cert_path) => {
                        let mut root_cert_der = vec![];
                        let mut root_cert_file = File::open(root_cert_path)?;
                        root_cert_file.read_to_end(&mut root_cert_der)?;
                        let root_cert = Certificate::from_der(&*root_cert_der)
                            .chain_err(|| "Can't parse root certificate")?;
                        builder.add_root_certificate(root_cert)
                            .chain_err(|| "Can't add root certificate")?;
                    },
                    None => (),
                }
                builder.identity(identity).chain_err(
                    || "Can't set identity for TlsConnectorBuilder",
                )?;
                builder.build().chain_err(
                    || "Can't build TlsConnectorBuilder",
                )
            })
            .into_future()
            .and_then(move |tls_connector| match self {
                Endpoint::Plain(stream) => {
                    let fut = if ssl_opts.skip_domain_validation() {
                        TlsConnectorExt::danger_connect_async_without_providing_domain_for_certificate_verification_and_server_name_indication(&tls_connector, stream)
                    } else {
                        TlsConnectorExt::connect_async(&tls_connector, &*domain, stream)
                    };
                    fut.then(|result| result.chain_err(|| "Can't connect TlsConnector"))
                }
                Endpoint::Secure(_) => unreachable!(),
            })
            .map(|tls_stream| Endpoint::Secure(tls_stream));
        Box::new(fut)
    }
}

impl From<TcpStream> for Endpoint {
    fn from(stream: TcpStream) -> Self {
        Endpoint::Plain(stream)
    }
}

#[cfg(feature = "ssl")]
impl From<TlsStream<TcpStream>> for Endpoint {
    fn from(stream: TlsStream<TcpStream>) -> Self {
        Endpoint::Secure(stream)
    }
}

impl io::Read for Endpoint {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Endpoint::Plain(ref mut stream) => stream.read(buf),
            #[cfg(feature = "ssl")]
            Endpoint::Secure(ref mut stream) => stream.read(buf),
        }
    }
}

impl io::Write for Endpoint {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            Endpoint::Plain(ref mut stream) => stream.write(buf),
            #[cfg(feature = "ssl")]
            Endpoint::Secure(ref mut stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            Endpoint::Plain(ref mut stream) => stream.flush(),
            #[cfg(feature = "ssl")]
            Endpoint::Secure(ref mut stream) => stream.flush(),
        }
    }
}


impl AsyncRead for Endpoint {
    unsafe fn prepare_uninitialized_buffer(&self, _buf: &mut [u8]) -> bool {
        match *self {
            Endpoint::Plain(ref stream) => stream.prepare_uninitialized_buffer(_buf),
            #[cfg(feature = "ssl")]
            Endpoint::Secure(ref stream) => stream.prepare_uninitialized_buffer(_buf),
        }
    }
}

impl AsyncWrite for Endpoint {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match *self {
            Endpoint::Plain(ref mut stream) => stream.shutdown(),
            #[cfg(feature = "ssl")]
            Endpoint::Secure(ref mut stream) => stream.shutdown(),
        }
    }
}

/// Stream connected to MySql server.
pub struct Stream {
    endpoint: Option<Endpoint>,
    closed: bool,
    next_packet: Option<ParseResult>,
    buf: Option<Vec<u8>>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Stream (endpoint={:?})", self.endpoint)
    }
}

impl Stream {
    pub fn connect<S>(addr: S, handle: &Handle) -> ConnectingStream
    where
        S: ToSocketAddrs,
    {
        new_connecting_stream(addr, handle)
    }

    pub fn write_packet(self, data: Vec<u8>, seq_id: u8) -> WritePacket {
        new_write_packet(self, data, seq_id)
    }

    pub fn set_keepalive_ms(&self, ms: Option<u32>) -> Result<()> {
        match self.endpoint {
            Some(ref endpoint) => endpoint.set_keepalive_ms(ms),
            None => unreachable!(),
        }
    }

    #[cfg(not(feature = "ssl"))]
    #[allow(unused)]
    pub fn make_secure(self, domain: String, ssl_opts: SslOpts) -> BoxFuture<Self> {
        panic!("Ssl connection requires `ssl` feature");
    }

    #[cfg(feature = "ssl")]
    pub fn make_secure(mut self, domain: String, ssl_opts: SslOpts) -> BoxFuture<Self> {
        match self.endpoint.take() {
            Some(endpoint) => {
                let fut = endpoint.make_secure(domain, ssl_opts).map(|endpoint| {
                    self.endpoint = Some(endpoint);
                    self
                });
                Box::new(fut)
            }
            None => unreachable!(),
        }
    }
}

impl io::Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.endpoint {
            Some(ref mut endpoint) => endpoint.read(buf),
            None => unreachable!(),
        }
    }
}

impl io::Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.endpoint {
            Some(ref mut endpoint) => endpoint.write(buf),
            None => unreachable!(),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.endpoint {
            Some(ref mut endpoint) => endpoint.flush(),
            None => unreachable!(),
        }
    }
}

impl AsyncRead for Stream {
    unsafe fn prepare_uninitialized_buffer(&self, _buf: &mut [u8]) -> bool {
        match self.endpoint {
            Some(ref endpoint) => endpoint.prepare_uninitialized_buffer(_buf),
            None => unreachable!(),
        }
    }
}

impl AsyncWrite for Stream {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match self.endpoint {
            Some(ref mut endpoint) => endpoint.shutdown(),
            None => unreachable!(),
        }
    }
}

impl stream::Stream for Stream {
    type Item = (RawPacket, u8);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<(RawPacket, u8)>, Error> {
        // should read everything from self.endpoint
        let mut would_block = false;
        if !self.closed {
            let mut buf = [0u8; 4096];
            loop {
                match self.read(&mut buf[..]) {
                    Ok(0) => {
                        break;
                    }
                    Ok(size) => {
                        let buf_handle = self.buf.as_mut().unwrap();
                        buf_handle.extend_from_slice(&buf[..size]);
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        would_block = true;
                        break;
                    }
                    Err(error) => {
                        self.closed = true;
                        return Err(Error::from(error));
                    }
                };
            }
        } else {
            return Ok(Ready(None));
        }

        // need to call again if there is a data in self.buf
        // or data was written to packet parser
        let mut should_poll = false;

        let next_packet = self.next_packet.take().expect(
            "Stream.next_packet should not be None",
        );
        let next_packet = match next_packet {
            ParseResult::Done(packet, seq_id) => {
                self.next_packet = Some(PacketParser::empty().parse());
                return Ok(Ready(Some((packet, seq_id))));
            }
            ParseResult::Incomplete(mut new_packet, needed) => {
                let buf_handle = self.buf.as_mut().unwrap();
                let buf_len = buf_handle.len();

                let to = cmp::min(needed, buf_len);
                new_packet.extend_from_slice(&buf_handle[..to]);
                unsafe {
                    let src = buf_handle.as_ptr().offset(to as isize);
                    let dst = buf_handle.as_mut_ptr();
                    let len = buf_handle.len() - to;
                    ::std::ptr::copy(src, dst, len);
                    buf_handle.set_len(len);
                }

                if buf_len != 0 {
                    should_poll = true;
                }

                new_packet
            }
        };

        self.next_packet = Some(next_packet.parse());

        if should_poll || !would_block {
            self.poll()
        } else {
            Ok(NotReady)
        }
    }
}
