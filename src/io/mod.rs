// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use errors::*;
use io::futures::new_connecting_stream;
use io::futures::new_write_packet;
use io::futures::ConnectingStream;
use io::futures::WritePacket;
#[cfg(not(feature = "ssl"))]
use lib_futures::future::ok;
use lib_futures::{
    stream,
    Async::{NotReady, Ready},
    Poll,
};
#[cfg(feature = "ssl")]
use lib_futures::{Future, IntoFuture};
use myc::packets::{PacketParser, ParseResult, RawPacket};
#[cfg(feature = "ssl")]
use native_tls::{Certificate, Identity, TlsConnector};
use opts::SslOpts;
use std::fmt;
#[cfg(feature = "ssl")]
use std::fs::File;
use std::io;
#[cfg(feature = "ssl")]
use std::io::Read;
use std::net::ToSocketAddrs;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::reactor::Handle;
use tokio_io::AsyncRead;
use tokio_io::AsyncWrite;
use MyFuture;

#[cfg(feature = "ssl")]
mod async_tls;
pub mod futures;

#[derive(Debug)]
pub enum Endpoint {
    Plain(TcpStream),
    #[cfg(feature = "ssl")]
    Secure(self::async_tls::TlsStream<TcpStream>),
}

impl Endpoint {
    #[cfg(feature = "ssl")]
    pub fn is_secure(&self) -> bool {
        if let Endpoint::Secure(_) = self {
            true
        } else {
            false
        }
    }

    #[cfg(not(feature = "ssl"))]
    pub fn is_secure(&self) -> bool {
        false
    }

    pub fn set_keepalive_ms(&self, ms: Option<u32>) -> Result<()> {
        let ms = ms.map(|val| Duration::from_millis(val as u64));
        match *self {
            Endpoint::Plain(ref stream) => stream.set_keepalive(ms)?,
            #[cfg(feature = "ssl")]
            Endpoint::Secure(ref stream) => stream.get_ref().get_ref().set_keepalive(ms)?,
        }
        Ok(())
    }

    pub fn set_tcp_nodelay(&self, val: bool) -> Result<()> {
        match *self {
            Endpoint::Plain(ref stream) => stream.set_nodelay(val)?,
            #[cfg(feature = "ssl")]
            Endpoint::Secure(ref stream) => stream.get_ref().get_ref().set_nodelay(val)?,
        }
        Ok(())
    }

    #[cfg(feature = "ssl")]
    pub fn make_secure(self, domain: String, ssl_opts: SslOpts) -> impl MyFuture<Self> {
        ::std::fs::File::open(ssl_opts.pkcs12_path())
            .map_err(Error::from)
            .and_then(|mut file| {
                let mut der = vec![];
                file.read_to_end(&mut der)?;
                let identity = Identity::from_pkcs12(&*der, ssl_opts.password().unwrap_or(""))
                    .chain_err(|| "Can't parse der")?;
                let mut builder = TlsConnector::builder();
                match ssl_opts.root_cert_path() {
                    Some(root_cert_path) => {
                        let mut root_cert_der = vec![];
                        let mut root_cert_file = File::open(root_cert_path)?;
                        root_cert_file.read_to_end(&mut root_cert_der)?;
                        let root_cert = Certificate::from_der(&*root_cert_der)
                            .chain_err(|| "Can't parse root certificate")?;
                        builder.add_root_certificate(root_cert);
                    }
                    None => (),
                }
                builder.identity(identity);
                builder.danger_accept_invalid_hostnames(ssl_opts.skip_domain_validation());
                builder
                    .build()
                    .chain_err(|| "Can't build TlsConnectorBuilder")
            })
            .into_future()
            .and_then(move |tls_connector| match self {
                Endpoint::Plain(stream) => {
                    let fut = self::async_tls::connect_async(&tls_connector, &*domain, stream);
                    fut.then(|result| result.chain_err(|| "Can't connect TlsConnector"))
                }
                Endpoint::Secure(_) => unreachable!(),
            })
            .map(|tls_stream| Endpoint::Secure(tls_stream))
    }
}

impl From<TcpStream> for Endpoint {
    fn from(stream: TcpStream) -> Self {
        Endpoint::Plain(stream)
    }
}

#[cfg(feature = "ssl")]
impl From<self::async_tls::TlsStream<TcpStream>> for Endpoint {
    fn from(stream: self::async_tls::TlsStream<TcpStream>) -> Self {
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
    parser: Option<PacketParser>,
    packets: ::std::collections::VecDeque<(RawPacket, u8)>,
    buf: Vec<u8>,
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

    pub fn set_tcp_nodelay(&self, val: bool) -> Result<()> {
        match self.endpoint {
            Some(ref endpoint) => endpoint.set_tcp_nodelay(val),
            None => unreachable!(),
        }
    }

    #[cfg(not(feature = "ssl"))]
    #[allow(unused)]
    pub fn make_secure(self, domain: String, ssl_opts: SslOpts) -> impl MyFuture<Self> {
        ok(panic!("Ssl connection requires `ssl` feature"))
    }

    #[cfg(feature = "ssl")]
    pub fn make_secure(mut self, domain: String, ssl_opts: SslOpts) -> impl MyFuture<Self> {
        match self.endpoint.take() {
            Some(endpoint) => endpoint.make_secure(domain, ssl_opts).map(|endpoint| {
                self.endpoint = Some(endpoint);
                self
            }),
            None => unreachable!(),
        }
    }

    pub fn is_secure(&self) -> bool {
        match self.endpoint.as_ref() {
            Some(endpoint) => endpoint.is_secure(),
            _ => false,
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
        // emit pending packets
        if let Some(packet) = self.packets.pop_front() {
            return Ok(Ready(Some(packet)));
        }

        // should read everything from self.endpoint
        let mut buf = [0u8; 4096];
        while !self.closed {
            match self.poll_read(&mut buf[..]) {
                Err(error) => {
                    self.closed = true;
                    bail!(error)
                }
                Ok(Ready(0)) => {
                    self.closed = true;
                }
                Ok(Ready(size)) => {
                    self.buf.extend_from_slice(&buf[..size]);
                }
                Ok(NotReady) => break,
            };
        }

        // parse buffer into packets
        let (packets, parser, buf) = parse_packet(self.parser.take().unwrap(), &self.buf);
        self.packets = packets.into();
        self.parser = Some(parser);
        self.buf = buf;

        if let Some(packet) = self.packets.pop_front() {
            return Ok(Ready(Some(packet)));
        }

        if self.closed {
            bail!(ErrorKind::ConnectionClosed);
        }
        return Ok(NotReady);
    }
}

fn parse_packet(
    mut parser: PacketParser,
    mut buf: &[u8],
) -> (Vec<(RawPacket, u8)>, PacketParser, Vec<u8>) {
    let mut packets = Vec::new();

    loop {
        parser = match parser.parse() {
            ParseResult::Done(packet, seq_id) => {
                packets.push((packet, seq_id));
                PacketParser::empty()
            }
            ParseResult::Incomplete(mut parser, needed) => {
                if buf.len() < needed {
                    return (packets, parser, Vec::from(buf));
                }

                parser.extend_from_slice(&buf[..needed]);
                buf = &buf[needed..];
                parser
            }
        };
    }
}
