// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::errors::*;
use crate::io::futures::{new_connecting_stream, new_write_packet, ConnectingStream, WritePacket};
#[cfg(not(feature = "ssl"))]
use crate::lib_futures::future::ok;
use crate::lib_futures::{stream, Async, Poll};
#[cfg(feature = "ssl")]
use lib_futures::{Future, IntoFuture};
use crate::myc::packets::RawPacket;
#[cfg(feature = "ssl")]
use native_tls::{Certificate, Identity, TlsConnector};
use crate::opts::SslOpts;
#[cfg(feature = "ssl")]
use std::fs::File;
#[cfg(feature = "ssl")]
use std::io::Read;
use std::{fmt, io, net::ToSocketAddrs, time::Duration};
use tokio::net::TcpStream;
use tokio_codec::Framed;
#[cfg(feature = "ssl")]
use tokio_codec::FramedParts;
use tokio_io::{AsyncRead, AsyncWrite};
use crate::MyFuture;

#[cfg(feature = "ssl")]
mod async_tls;
pub mod futures;
mod packet_codec;

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
        (|| {
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
            if let Some(pkcs12_path) = ssl_opts.pkcs12_path() {
                let der = std::fs::read(pkcs12_path)?;
                let identity = Identity::from_pkcs12(&*der, ssl_opts.password().unwrap_or(""))
                    .chain_err(|| "Can't parse der")?;
                builder.identity(identity);
            }
            builder.danger_accept_invalid_hostnames(ssl_opts.skip_domain_validation());
            builder
                .build()
                .chain_err(|| "Can't build TlsConnectorBuilder")
        })()
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
    closed: bool,
    codec: Option<Framed<Endpoint, packet_codec::PacketCodec>>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Stream (endpoint={:?})",
            self.codec.as_ref().unwrap().get_ref()
        )
    }
}

impl Stream {
    pub fn connect<S>(addr: S) -> ConnectingStream
    where
        S: ToSocketAddrs,
    {
        new_connecting_stream(addr)
    }

    pub fn write_packet(self, data: Vec<u8>, seq_id: u8) -> WritePacket {
        new_write_packet(self, data, seq_id)
    }

    pub fn set_keepalive_ms(&self, ms: Option<u32>) -> Result<()> {
        self.codec.as_ref().unwrap().get_ref().set_keepalive_ms(ms)
    }

    pub fn set_tcp_nodelay(&self, val: bool) -> Result<()> {
        self.codec.as_ref().unwrap().get_ref().set_tcp_nodelay(val)
    }

    #[cfg(not(feature = "ssl"))]
    #[allow(unused)]
    pub fn make_secure(self, domain: String, ssl_opts: SslOpts) -> impl MyFuture<Self> {
        ok(panic!("Ssl connection requires `ssl` feature"))
    }

    #[cfg(feature = "ssl")]
    pub fn make_secure(mut self, domain: String, ssl_opts: SslOpts) -> impl MyFuture<Self> {
        let codec = self.codec.take().unwrap();
        let FramedParts { io, codec, .. } = codec.into_parts();
        io.make_secure(domain, ssl_opts).map(move |endpoint| {
            let codec = Framed::new(endpoint, codec);
            self.codec = Some(codec);
            self
        })
    }

    pub fn is_secure(&self) -> bool {
        self.codec.as_ref().unwrap().get_ref().is_secure()
    }
}

impl stream::Stream for Stream {
    type Item = (RawPacket, u8);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<(RawPacket, u8)>, Error> {
        if !self.closed {
            self.codec.as_mut().unwrap().poll().map_err(Error::from)
        } else {
            Ok(Async::Ready(None))
        }
    }
}
