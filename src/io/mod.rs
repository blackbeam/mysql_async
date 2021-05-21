// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

pub use self::{read_packet::ReadPacket, write_packet::WritePacket};

use bytes::BytesMut;
use futures_core::{ready, stream};
use futures_util::stream::{FuturesUnordered, StreamExt};
use mio::net::{TcpKeepalive, TcpSocket};
use mysql_common::proto::codec::PacketCodec as PacketCodecInner;
use native_tls::{Certificate, Identity, TlsConnector};
use pin_project::pin_project;
#[cfg(unix)]
use tokio::io::AsyncWriteExt;
use tokio::{
    io::{AsyncRead, AsyncWrite, ErrorKind::Interrupted, ReadBuf},
    net::TcpStream,
};
use tokio_util::codec::{Decoder, Encoder, Framed, FramedParts};

#[cfg(unix)]
use std::path::Path;
use std::{
    fmt,
    fs::File,
    future::Future,
    io::{
        self,
        ErrorKind::{BrokenPipe, NotConnected, Other},
        Read,
    },
    mem::replace,
    net::{SocketAddr, ToSocketAddrs},
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{buffer_pool::PooledBuf, error::IoError, opts::SslOpts};

#[cfg(unix)]
use crate::io::socket::Socket;

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

#[derive(Debug)]
pub struct PacketCodec {
    inner: PacketCodecInner,
    decode_buf: PooledBuf,
}

impl Default for PacketCodec {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            decode_buf: crate::BUFFER_POOL.get(),
        }
    }
}

impl Deref for PacketCodec {
    type Target = PacketCodecInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PacketCodec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Decoder for PacketCodec {
    type Item = PooledBuf;
    type Error = IoError;

    fn decode(&mut self, src: &mut BytesMut) -> std::result::Result<Option<Self::Item>, IoError> {
        if self.inner.decode(src, self.decode_buf.as_mut())? {
            let new_buf = crate::BUFFER_POOL.get();
            Ok(Some(replace(&mut self.decode_buf, new_buf)))
        } else {
            Ok(None)
        }
    }
}

impl Encoder<PooledBuf> for PacketCodec {
    type Error = IoError;

    fn encode(&mut self, item: PooledBuf, dst: &mut BytesMut) -> std::result::Result<(), IoError> {
        Ok(self.inner.encode(&mut item.as_ref(), dst)?)
    }
}

#[pin_project(project = EndpointProj)]
#[derive(Debug)]
pub(crate) enum Endpoint {
    Plain(Option<TcpStream>),
    Secure(#[pin] tokio_native_tls::TlsStream<TcpStream>),
    #[cfg(unix)]
    Socket(#[pin] Socket),
}

/// This future will check that TcpStream is live.
///
/// This check is similar to a one, implemented by GitHub team for the go-sql-driver/mysql.
#[derive(Debug)]
struct CheckTcpStream<'a>(&'a mut TcpStream);

impl Future for CheckTcpStream<'_> {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.0.poll_read_ready(cx) {
            Poll::Ready(Ok(())) => {
                // stream is readable
                let mut buf = [0_u8; 1];
                match self.0.try_read(&mut buf) {
                    Ok(0) => Poll::Ready(Err(io::Error::new(BrokenPipe, "broken pipe"))),
                    Ok(_) => Poll::Ready(Err(io::Error::new(Other, "stream should be empty"))),
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => Poll::Ready(Ok(())),
                    Err(err) => Poll::Ready(Err(err)),
                }
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Ready(Ok(())),
        }
    }
}

impl Endpoint {
    /// Checks, that connection is alive.
    async fn check(&mut self) -> std::result::Result<(), IoError> {
        //return Ok(());
        match self {
            Endpoint::Plain(Some(stream)) => {
                CheckTcpStream(stream).await?;
                Ok(())
            }
            Endpoint::Secure(tls_stream) => {
                CheckTcpStream(tls_stream.get_mut().get_mut().get_mut()).await?;
                Ok(())
            }
            #[cfg(unix)]
            Endpoint::Socket(socket) => {
                socket.write(&[]).await?;
                Ok(())
            }
            Endpoint::Plain(None) => unreachable!(),
        }
    }

    pub fn is_secure(&self) -> bool {
        matches!(self, Endpoint::Secure(_))
    }

    pub fn set_tcp_nodelay(&self, val: bool) -> io::Result<()> {
        match *self {
            Endpoint::Plain(Some(ref stream)) => stream.set_nodelay(val)?,
            Endpoint::Plain(None) => unreachable!(),
            Endpoint::Secure(ref stream) => {
                stream.get_ref().get_ref().get_ref().set_nodelay(val)?
            }
            #[cfg(unix)]
            Endpoint::Socket(_) => (/* inapplicable */),
        }
        Ok(())
    }

    pub async fn make_secure(
        &mut self,
        domain: String,
        ssl_opts: SslOpts,
    ) -> std::result::Result<(), IoError> {
        #[cfg(unix)]
        if let Endpoint::Socket(_) = self {
            // inapplicable
            return Ok(());
        }

        let mut builder = TlsConnector::builder();
        if let Some(root_cert_path) = ssl_opts.root_cert_path() {
            let mut root_cert_data = vec![];
            let mut root_cert_file = File::open(root_cert_path)?;
            root_cert_file.read_to_end(&mut root_cert_data)?;

            let root_certs = Certificate::from_der(&*root_cert_data)
                .map(|x| vec![x])
                .or_else(|_| {
                    pem::parse_many(&*root_cert_data)
                        .iter()
                        .map(pem::encode)
                        .map(|s| Certificate::from_pem(s.as_bytes()))
                        .collect()
                })?;

            for root_cert in root_certs {
                builder.add_root_certificate(root_cert);
            }
        }
        if let Some(pkcs12_path) = ssl_opts.pkcs12_path() {
            let der = std::fs::read(pkcs12_path)?;
            let identity = Identity::from_pkcs12(&*der, ssl_opts.password().unwrap_or(""))?;
            builder.identity(identity);
        }
        builder.danger_accept_invalid_hostnames(ssl_opts.skip_domain_validation());
        builder.danger_accept_invalid_certs(ssl_opts.accept_invalid_certs());
        let tls_connector: tokio_native_tls::TlsConnector = builder.build()?.into();

        *self = match self {
            Endpoint::Plain(stream) => {
                let stream = stream.take().unwrap();
                let tls_stream = tls_connector.connect(&*domain, stream).await?;
                Endpoint::Secure(tls_stream)
            }
            Endpoint::Secure(_) => unreachable!(),
            #[cfg(unix)]
            Endpoint::Socket(_) => unreachable!(),
        };

        Ok(())
    }
}

impl From<TcpStream> for Endpoint {
    fn from(stream: TcpStream) -> Self {
        Endpoint::Plain(Some(stream))
    }
}

#[cfg(unix)]
impl From<Socket> for Endpoint {
    fn from(socket: Socket) -> Self {
        Endpoint::Socket(socket)
    }
}

impl From<tokio_native_tls::TlsStream<TcpStream>> for Endpoint {
    fn from(stream: tokio_native_tls::TlsStream<TcpStream>) -> Self {
        Endpoint::Secure(stream)
    }
}

impl AsyncRead for Endpoint {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        let mut this = self.project();
        with_interrupted!(match this {
            EndpointProj::Plain(ref mut stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_read(cx, buf)
            }
            EndpointProj::Secure(ref mut stream) => stream.as_mut().poll_read(cx, buf),
            #[cfg(unix)]
            EndpointProj::Socket(ref mut stream) => stream.as_mut().poll_read(cx, buf),
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
            #[cfg(unix)]
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
            #[cfg(unix)]
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
            #[cfg(unix)]
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
    #[cfg(unix)]
    fn new<T: Into<Endpoint>>(endpoint: T) -> Self {
        let endpoint = endpoint.into();

        Self {
            closed: false,
            codec: Box::new(Framed::new(endpoint, PacketCodec::default())).into(),
        }
    }

    pub(crate) async fn connect_tcp<S>(addr: S, keepalive: Option<Duration>) -> io::Result<Stream>
    where
        S: ToSocketAddrs,
    {
        // TODO: Use tokio to setup keepalive (see tokio-rs/tokio#3082)
        async fn connect_stream(
            addr: SocketAddr,
            keepalive_opts: Option<TcpKeepalive>,
        ) -> io::Result<TcpStream> {
            let socket = if addr.is_ipv6() {
                TcpSocket::new_v6()?
            } else {
                TcpSocket::new_v4()?
            };

            if let Some(keepalive_opts) = keepalive_opts {
                socket.set_keepalive_params(keepalive_opts)?;
            }

            let stream = tokio::task::spawn_blocking(move || {
                let mut stream = socket.connect(addr)?;
                let mut poll = mio::Poll::new()?;
                let mut events = mio::Events::with_capacity(1024);

                poll.registry()
                    .register(&mut stream, mio::Token(0), mio::Interest::WRITABLE)?;

                loop {
                    poll.poll(&mut events, None)?;

                    for event in &events {
                        if event.token() == mio::Token(0) && event.is_error() {
                            return Err(io::Error::new(
                                io::ErrorKind::ConnectionRefused,
                                "Connection refused",
                            ));
                        }

                        if event.token() == mio::Token(0) && event.is_writable() {
                            // The socket connected (probably, it could still be a spurious
                            // wakeup)
                            return Ok::<_, io::Error>(stream);
                        }
                    }
                }
            })
            .await??;

            #[cfg(unix)]
            let std_stream = unsafe {
                use std::os::unix::prelude::*;
                let fd = stream.into_raw_fd();
                std::net::TcpStream::from_raw_fd(fd)
            };

            #[cfg(windows)]
            let std_stream = unsafe {
                use std::os::windows::prelude::*;
                let fd = stream.into_raw_socket();
                std::net::TcpStream::from_raw_socket(fd)
            };

            Ok(TcpStream::from_std(std_stream)?)
        }

        let keepalive_opts = keepalive.map(|time| TcpKeepalive::new().with_time(time));

        match addr.to_socket_addrs() {
            Ok(addresses) => {
                let mut streams = FuturesUnordered::new();

                for address in addresses {
                    streams.push(connect_stream(address, keepalive_opts.clone()));
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
                    Err(e)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "could not resolve to any address",
                    ))
                }
            }
            Err(err) => Err(err),
        }
    }

    #[cfg(unix)]
    pub(crate) async fn connect_socket<P: AsRef<Path>>(path: P) -> io::Result<Stream> {
        Ok(Stream::new(Socket::new(path).await?))
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
    type Item = std::result::Result<PooledBuf, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.closed {
            let item = ready!(Pin::new(self.codec.as_mut().unwrap()).poll_next(cx)).transpose()?;
            Poll::Ready(Ok(item).transpose())
        } else {
            Poll::Ready(None)
        }
    }
}

#[cfg(test)]
mod test {
    #[cfg(unix)] // no sane way to retrieve current keepalive value on windows
    #[tokio::test]
    async fn should_connect_with_keepalive() {
        use crate::{test_misc::get_opts, Conn};

        let opts = get_opts()
            .tcp_keepalive(Some(42_000_u32))
            .prefer_socket(false);
        let mut conn: Conn = Conn::new(opts).await.unwrap();
        let stream = conn.stream_mut().unwrap();
        let endpoint = stream.codec.as_mut().unwrap().get_ref();
        let stream = match endpoint {
            super::Endpoint::Plain(Some(stream)) => stream,
            super::Endpoint::Secure(tls_stream) => tls_stream.get_ref().get_ref().get_ref(),
            _ => unreachable!(),
        };
        let sock = unsafe {
            use std::os::unix::prelude::*;
            let raw = stream.as_raw_fd();
            socket2::Socket::from_raw_fd(raw)
        };

        assert_eq!(
            sock.keepalive_time().unwrap(),
            std::time::Duration::from_millis(42_000),
        );

        std::mem::forget(sock);

        conn.disconnect().await.unwrap();
    }
}
