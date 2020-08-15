// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

pub use self::{read_packet::ReadPacket, write_packet::WritePacket};

use bytes::{BufMut, BytesMut};
use futures_core::{ready, stream};
use futures_util::stream::{FuturesUnordered, StreamExt};
use mysql_common::proto::codec::PacketCodec as PacketCodecInner;
use native_tls::{Certificate, Identity, TlsConnector};
use pin_project::pin_project;
use tokio::{io::ErrorKind::Interrupted, net::TcpStream, prelude::*};
use tokio_util::codec::{Decoder, Encoder, Framed, FramedParts};

use std::{
    fmt,
    fs::File,
    future::Future,
    io::{
        self,
        ErrorKind::{NotConnected, Other, UnexpectedEof},
        Read,
    },
    mem::MaybeUninit,
    net::ToSocketAddrs,
    ops::{Deref, DerefMut},
    path::Path,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{error::IoError, io::socket::Socket, opts::SslOpts};

macro_rules! with_interrupted {
    ($e:expr) => {
        loop {
            match $e {
                Poll::Ready(Err(err)) if err.kind() == Interrupted => continue,
                x => break x,
            }
        }
    };
}

mod read_packet;
mod socket;
mod write_packet;

#[derive(Debug, Default)]
pub struct PacketCodec(PacketCodecInner);

impl Deref for PacketCodec {
    type Target = PacketCodecInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PacketCodec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Decoder for PacketCodec {
    type Item = Vec<u8>;
    type Error = IoError;

    fn decode(&mut self, src: &mut BytesMut) -> std::result::Result<Option<Self::Item>, IoError> {
        Ok(self.0.decode(src)?)
    }
}

impl Encoder<Vec<u8>> for PacketCodec {
    type Error = IoError;

    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> std::result::Result<(), IoError> {
        Ok(self.0.encode(item, dst)?)
    }
}

#[pin_project(project = EndpointProj)]
#[derive(Debug)]
pub(crate) enum Endpoint {
    Plain(Option<TcpStream>),
    Secure(#[pin] tokio_tls::TlsStream<TcpStream>),
    Socket(#[pin] Socket),
}

/// This future will check that TcpStream is live.
///
/// This check is similar to a one, implemented by GitHub team for the go-sql-driver/mysql.
#[derive(Debug)]
struct CheckTcpStream<'a>(&'a mut TcpStream);

impl Future for CheckTcpStream<'_> {
    type Output = io::Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let buf = &mut [0_u8];
        match self.0.poll_peek(cx, buf) {
            Poll::Ready(Ok(0)) => Poll::Ready(Err(io::Error::new(UnexpectedEof, "broken pipe"))),
            Poll::Ready(Ok(_)) => Poll::Ready(Err(io::Error::new(Other, "unexpected read"))),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Ready(Ok(())),
        }
    }
}

impl Endpoint {
    /// Checks, that connection is alive.
    async fn check(&mut self) -> std::result::Result<(), IoError> {
        match self {
            Endpoint::Plain(Some(stream)) => {
                CheckTcpStream(stream).await?;
                Ok(())
            }
            Endpoint::Secure(tls_stream) => {
                CheckTcpStream(tls_stream.get_mut()).await?;
                Ok(())
            }
            Endpoint::Socket(socket) => {
                socket.write(&[]).await?;
                Ok(())
            }
            Endpoint::Plain(None) => unreachable!(),
        }
    }

    pub fn is_secure(&self) -> bool {
        if let Endpoint::Secure(_) = self {
            true
        } else {
            false
        }
    }

    pub fn set_keepalive_ms(&self, ms: Option<u32>) -> io::Result<()> {
        let ms = ms.map(|val| Duration::from_millis(u64::from(val)));
        match *self {
            Endpoint::Plain(Some(ref stream)) => stream.set_keepalive(ms)?,
            Endpoint::Plain(None) => unreachable!(),
            Endpoint::Secure(ref stream) => stream.get_ref().set_keepalive(ms)?,
            Endpoint::Socket(_) => (/* inapplicable */),
        }
        Ok(())
    }

    pub fn set_tcp_nodelay(&self, val: bool) -> io::Result<()> {
        match *self {
            Endpoint::Plain(Some(ref stream)) => stream.set_nodelay(val)?,
            Endpoint::Plain(None) => unreachable!(),
            Endpoint::Secure(ref stream) => stream.get_ref().set_nodelay(val)?,
            Endpoint::Socket(_) => (/* inapplicable */),
        }
        Ok(())
    }

    pub async fn make_secure(
        &mut self,
        domain: String,
        ssl_opts: SslOpts,
    ) -> std::result::Result<(), IoError> {
        if let Endpoint::Socket(_) = self {
            // inapplicable
            return Ok(());
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
        let tls_connector: tokio_tls::TlsConnector = builder.build()?.into();

        *self = match self {
            Endpoint::Plain(stream) => {
                let stream = stream.take().unwrap();
                let tls_stream = tls_connector.connect(&*domain, stream).await?;
                Endpoint::Secure(tls_stream)
            }
            Endpoint::Secure(_) | Endpoint::Socket(_) => unreachable!(),
        };

        Ok(())
    }
}

impl From<TcpStream> for Endpoint {
    fn from(stream: TcpStream) -> Self {
        Endpoint::Plain(Some(stream))
    }
}

impl From<Socket> for Endpoint {
    fn from(socket: Socket) -> Self {
        Endpoint::Socket(socket)
    }
}

impl From<tokio_tls::TlsStream<TcpStream>> for Endpoint {
    fn from(stream: tokio_tls::TlsStream<TcpStream>) -> Self {
        Endpoint::Secure(stream)
    }
}

impl AsyncRead for Endpoint {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<std::result::Result<usize, tokio::io::Error>> {
        let mut this = self.project();
        with_interrupted!(match this {
            EndpointProj::Plain(ref mut stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_read(cx, buf)
            }
            EndpointProj::Secure(ref mut stream) => stream.as_mut().poll_read(cx, buf),
            EndpointProj::Socket(ref mut stream) => stream.as_mut().poll_read(cx, buf),
        })
    }

    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        match self {
            Endpoint::Plain(Some(stream)) => stream.prepare_uninitialized_buffer(buf),
            Endpoint::Plain(None) => unreachable!(),
            Endpoint::Secure(stream) => stream.prepare_uninitialized_buffer(buf),
            Endpoint::Socket(stream) => stream.prepare_uninitialized_buffer(buf),
        }
    }

    fn poll_read_buf<B>(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut B,
    ) -> Poll<std::result::Result<usize, tokio::io::Error>>
    where
        B: BufMut,
    {
        let mut this = self.project();
        with_interrupted!(match this {
            EndpointProj::Plain(ref mut stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_read_buf(cx, buf)
            }
            EndpointProj::Secure(ref mut stream) => stream.as_mut().poll_read_buf(cx, buf),
            EndpointProj::Socket(ref mut stream) => stream.as_mut().poll_read_buf(cx, buf),
        })
    }
}

impl AsyncWrite for Endpoint {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, tokio::io::Error>> {
        let mut this = self.project();
        with_interrupted!(match this {
            EndpointProj::Plain(ref mut stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_write(cx, buf)
            }
            EndpointProj::Secure(ref mut stream) => stream.as_mut().poll_write(cx, buf),
            EndpointProj::Socket(ref mut stream) => stream.as_mut().poll_write(cx, buf),
        })
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        let mut this = self.project();
        with_interrupted!(match this {
            EndpointProj::Plain(ref mut stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_flush(cx)
            }
            EndpointProj::Secure(ref mut stream) => stream.as_mut().poll_flush(cx),
            EndpointProj::Socket(ref mut stream) => stream.as_mut().poll_flush(cx),
        })
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        let mut this = self.project();
        with_interrupted!(match this {
            EndpointProj::Plain(ref mut stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_shutdown(cx)
            }
            EndpointProj::Secure(ref mut stream) => stream.as_mut().poll_shutdown(cx),
            EndpointProj::Socket(ref mut stream) => stream.as_mut().poll_shutdown(cx),
        })
    }
}

/// A Stream, connected to MySql server.
pub struct Stream {
    closed: bool,
    pub(crate) codec: Option<Box<Framed<Endpoint, PacketCodec>>>,
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
            codec: Box::new(Framed::new(endpoint, PacketCodec::default())).into(),
        }
    }

    pub(crate) async fn connect_tcp<S>(addr: S) -> io::Result<Stream>
    where
        S: ToSocketAddrs,
    {
        match addr.to_socket_addrs() {
            Ok(addresses) => {
                let mut streams = FuturesUnordered::new();

                for address in addresses {
                    streams.push(TcpStream::connect(address));
                }

                let mut err = None;
                while let Some(stream) = streams.next().await {
                    match stream {
                        Err(e) => {
                            err = Some(e);
                        }
                        Ok(stream) => {
                            return Ok(Stream {
                                closed: false,
                                codec: Box::new(Framed::new(stream.into(), PacketCodec::default()))
                                    .into(),
                            });
                        }
                    }
                }

                if let Some(e) = err {
                    Err(e.into())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "could not resolve to any address",
                    )
                    .into())
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    pub(crate) async fn connect_socket<P: AsRef<Path>>(path: P) -> io::Result<Stream> {
        Ok(Stream::new(Socket::new(path).await?))
    }

    pub(crate) fn set_keepalive_ms(&self, ms: Option<u32>) -> io::Result<()> {
        self.codec.as_ref().unwrap().get_ref().set_keepalive_ms(ms)
    }

    pub(crate) fn set_tcp_nodelay(&self, val: bool) -> io::Result<()> {
        self.codec.as_ref().unwrap().get_ref().set_tcp_nodelay(val)
    }

    pub(crate) async fn make_secure(
        &mut self,
        domain: String,
        ssl_opts: SslOpts,
    ) -> crate::error::Result<()> {
        let codec = self.codec.take().unwrap();
        let FramedParts { mut io, codec, .. } = codec.into_parts();
        io.make_secure(domain, ssl_opts).await?;
        let codec = Framed::new(io, codec);
        self.codec = Some(Box::new(codec));
        Ok(())
    }

    pub(crate) fn is_secure(&self) -> bool {
        self.codec.as_ref().unwrap().get_ref().is_secure()
    }

    pub(crate) fn reset_seq_id(&mut self) {
        if let Some(codec) = self.codec.as_mut() {
            codec.codec_mut().reset_seq_id();
        }
    }

    pub(crate) fn sync_seq_id(&mut self) {
        if let Some(codec) = self.codec.as_mut() {
            codec.codec_mut().sync_seq_id();
        }
    }

    pub(crate) fn set_max_allowed_packet(&mut self, max_allowed_packet: usize) {
        if let Some(codec) = self.codec.as_mut() {
            codec.codec_mut().max_allowed_packet = max_allowed_packet;
        }
    }

    pub(crate) fn compress(&mut self, level: crate::Compression) {
        if let Some(codec) = self.codec.as_mut() {
            codec.codec_mut().compress(level);
        }
    }

    /// Checks, that connection is alive.
    pub(crate) async fn check(&mut self) -> std::result::Result<(), IoError> {
        if let Some(codec) = self.codec.as_mut() {
            codec.get_mut().check().await?;
        }
        Ok(())
    }

    pub(crate) async fn close(mut self) -> std::result::Result<(), IoError> {
        self.closed = true;
        if let Some(mut codec) = self.codec {
            use futures_sink::Sink;
            futures_util::future::poll_fn(|cx| match Pin::new(&mut *codec).poll_close(cx) {
                Poll::Ready(Err(IoError::Io(err))) if err.kind() == NotConnected => {
                    Poll::Ready(Ok(()))
                }
                x => x,
            })
            .await?;
        }
        Ok(())
    }
}

impl stream::Stream for Stream {
    type Item = std::result::Result<Vec<u8>, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.closed {
            let item = ready!(Pin::new(self.codec.as_mut().unwrap()).poll_next(cx)).transpose()?;
            Poll::Ready(Ok(item).transpose())
        } else {
            Poll::Ready(None)
        }
    }
}
