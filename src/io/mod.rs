// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bytes::BufMut;
use futures_core::{ready, stream};
use mysql_common::packets::RawPacket;
use native_tls::{Certificate, Identity, TlsConnector};
use pin_project::{pin_project, project};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::codec::Framed;
use tokio::codec::FramedParts;
use tokio::net::TcpStream;
use tokio::prelude::*;

use std::{fmt, net::ToSocketAddrs, path::Path, time::Duration};
use std::{fs::File, io::Read};

use crate::{
    error::*,
    io::{
        futures::{new_connecting_tcp_stream, new_write_packet, WritePacket},
        socket::Socket,
    },
    opts::SslOpts,
};

mod async_tls;
pub mod futures;
mod packet_codec;
mod socket;

#[pin_project]
#[derive(Debug)]
pub enum Endpoint {
    Plain(#[pin] TcpStream),
    Secure(#[pin] self::async_tls::TlsStream<TcpStream>),
    Socket(#[pin] Socket),
}

impl Endpoint {
    pub fn is_secure(&self) -> bool {
        if let Endpoint::Secure(_) = self {
            true
        } else {
            false
        }
    }

    pub fn set_keepalive_ms(&self, ms: Option<u32>) -> Result<()> {
        let ms = ms.map(|val| Duration::from_millis(u64::from(val)));
        match *self {
            Endpoint::Plain(ref stream) => stream.set_keepalive(ms)?,
            Endpoint::Secure(ref stream) => stream.get_ref().get_ref().set_keepalive(ms)?,
            Endpoint::Socket(_) => (/* inapplicable */),
        }
        Ok(())
    }

    pub fn set_tcp_nodelay(&self, val: bool) -> Result<()> {
        match *self {
            Endpoint::Plain(ref stream) => stream.set_nodelay(val)?,
            Endpoint::Secure(ref stream) => stream.get_ref().get_ref().set_nodelay(val)?,
            Endpoint::Socket(_) => (/* inapplicable */),
        }
        Ok(())
    }

    pub async fn make_secure(self, domain: String, ssl_opts: SslOpts) -> Result<Self> {
        if let Endpoint::Socket(_) = self {
            // inapplicable
            return Ok(self);
        }

        let mut builder = TlsConnector::builder();
        match ssl_opts.root_cert_path() {
            Some(root_cert_path) => {
                let mut root_cert_der = vec![];
                let mut root_cert_file = File::open(root_cert_path)?;
                root_cert_file.read_to_end(&mut root_cert_der)?;
                let root_cert = Certificate::from_der(&*root_cert_der)?;
                builder.add_root_certificate(root_cert);
            }
            None => (),
        }
        if let Some(pkcs12_path) = ssl_opts.pkcs12_path() {
            let der = std::fs::read(pkcs12_path)?;
            let identity = Identity::from_pkcs12(&*der, ssl_opts.password().unwrap_or(""))?;
            builder.identity(identity);
        }
        builder.danger_accept_invalid_hostnames(ssl_opts.skip_domain_validation());
        builder.danger_accept_invalid_certs(ssl_opts.accept_invalid_certs());
        let tls_connector = builder.build()?;
        let tls_stream = match self {
            Endpoint::Plain(stream) => {
                self::async_tls::connect_async(&tls_connector, &*domain, stream).await?
            }
            Endpoint::Secure(_) | Endpoint::Socket(_) => unreachable!(),
        };
        Ok(Endpoint::Secure(tls_stream))
    }
}

impl From<TcpStream> for Endpoint {
    fn from(stream: TcpStream) -> Self {
        Endpoint::Plain(stream)
    }
}

impl From<Socket> for Endpoint {
    fn from(socket: Socket) -> Self {
        Endpoint::Socket(socket)
    }
}

impl From<self::async_tls::TlsStream<TcpStream>> for Endpoint {
    fn from(stream: self::async_tls::TlsStream<TcpStream>) -> Self {
        Endpoint::Secure(stream)
    }
}

impl AsyncRead for Endpoint {
    #[project]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<StdResult<usize, tokio::io::Error>> {
        #[project]
        match self.project() {
            Endpoint::Plain(stream) => stream.poll_read(cx, buf),
            Endpoint::Secure(stream) => stream.poll_read(cx, buf),
            Endpoint::Socket(stream) => stream.poll_read(cx, buf),
        }
    }

    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        match self {
            Endpoint::Plain(stream) => stream.prepare_uninitialized_buffer(buf),
            Endpoint::Secure(stream) => stream.prepare_uninitialized_buffer(buf),
            Endpoint::Socket(stream) => stream.prepare_uninitialized_buffer(buf),
        }
    }

    #[project]
    fn poll_read_buf<B>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut B,
    ) -> Poll<StdResult<usize, tokio::io::Error>>
    where
        B: BufMut,
    {
        #[project]
        match self.project() {
            Endpoint::Plain(stream) => stream.poll_read_buf(cx, buf),
            Endpoint::Secure(stream) => stream.poll_read_buf(cx, buf),
            Endpoint::Socket(stream) => stream.poll_read_buf(cx, buf),
        }
    }
}

impl AsyncWrite for Endpoint {
    #[project]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<StdResult<usize, tokio::io::Error>> {
        #[project]
        match self.project() {
            Endpoint::Plain(stream) => stream.poll_write(cx, buf),
            Endpoint::Secure(stream) => stream.poll_write(cx, buf),
            Endpoint::Socket(stream) => stream.poll_write(cx, buf),
        }
    }

    #[project]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<StdResult<(), tokio::io::Error>> {
        #[project]
        match self.project() {
            Endpoint::Plain(stream) => stream.poll_flush(cx),
            Endpoint::Secure(stream) => stream.poll_flush(cx),
            Endpoint::Socket(stream) => stream.poll_flush(cx),
        }
    }

    #[project]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<StdResult<(), tokio::io::Error>> {
        #[project]
        match self.project() {
            Endpoint::Plain(stream) => stream.poll_shutdown(cx),
            Endpoint::Secure(stream) => stream.poll_shutdown(cx),
            Endpoint::Socket(stream) => stream.poll_shutdown(cx),
        }
    }
}

/// Stream connected to MySql server.
pub struct Stream {
    closed: bool,
    codec: Option<Box<Framed<Endpoint, packet_codec::PacketCodec>>>,
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
    fn new<T: Into<Endpoint>>(endpoint: T) -> Self {
        let endpoint = endpoint.into();

        Self {
            closed: false,
            codec: Box::new(Framed::new(endpoint, packet_codec::PacketCodec::new())).into(),
        }
    }

    pub async fn connect_tcp<S>(addr: S) -> Result<Stream>
    where
        S: ToSocketAddrs,
    {
        new_connecting_tcp_stream(addr).await
    }

    pub async fn connect_socket<P: AsRef<Path>>(path: P) -> Result<Stream> {
        Ok(Stream::new(Socket::new(path).await?))
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

    pub async fn make_secure(mut self, domain: String, ssl_opts: SslOpts) -> Result<Self> {
        let codec = self.codec.take().unwrap();
        let FramedParts { io, codec, .. } = codec.into_parts();
        let endpoint = io.make_secure(domain, ssl_opts).await?;
        let codec = Framed::new(endpoint, codec);
        self.codec = Some(Box::new(codec));
        Ok(self)
    }

    pub fn is_secure(&self) -> bool {
        self.codec.as_ref().unwrap().get_ref().is_secure()
    }
}

impl stream::Stream for Stream {
    type Item = Result<(RawPacket, u8)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.closed {
            Poll::Ready(
                Ok(ready!(Pin::new(self.codec.as_mut().unwrap()).poll_next(cx)).transpose()?)
                    .transpose(),
            )
        } else {
            Poll::Ready(None)
        }
    }
}
