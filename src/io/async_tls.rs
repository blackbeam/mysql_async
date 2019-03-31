// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! This module implements async TLS streams and source
//! of this module is mostly copyed from tokio_tls crate.

use futures::{Async, Future, Poll};
use native_tls::{self, Error, HandshakeError, TlsConnector};
use tokio_io::{AsyncRead, AsyncWrite};

use std::io::{self, Read, Write};

/// A wrapper around an underlying raw stream which implements the TLS or SSL
/// protocol.
///
/// A `TlsStream<S>` represents a handshake that has been completed successfully
/// and both the server and the client are ready for receiving and sending
/// data. Bytes read from a `TlsStream` are decrypted from `S` and bytes written
/// to a `TlsStream` are encrypted when passing through to `S`.
#[derive(Debug)]
pub struct TlsStream<S> {
    inner: native_tls::TlsStream<S>,
}

impl<S> TlsStream<S> {
    /// Get access to the internal `native_tls::TlsStream` stream which also
    /// transitively allows access to `S`.
    pub fn get_ref(&self) -> &native_tls::TlsStream<S> {
        &self.inner
    }
}

impl<S: Read + Write> Read for TlsStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let result = self.inner.read(buf);
        match &result {
            Ok(0) => panic!("{:?} READ {:?}", ::std::thread::current().id(), result),
            _ => (),
        }
        result
    }
}

impl<S: Read + Write> Write for TlsStream<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<S: AsyncRead + AsyncWrite> AsyncRead for TlsStream<S> {
    fn poll_read(&mut self, buf: &mut [u8]) -> Poll<usize, io::Error> {
        println!("{:?} POLL READ", ::std::thread::current().id());
        use std::io::Read;

        match self.read(buf) {
            Ok(t) => {
                if t == 0 {
                    println!("{:?} STREAM DONE", ::std::thread::current().id());
                }
                Ok(Async::Ready(t))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(Async::NotReady),
            Err(e) => return Err(e.into()),
        }
    }
}

impl<S: AsyncRead + AsyncWrite> AsyncWrite for TlsStream<S> {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match self.inner.shutdown() {
            Ok(t) => t,
            Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                return Ok(futures::Async::NotReady);
            }
            Err(e) => return Err(e.into()),
        }
        self.inner.get_mut().shutdown()
    }
}

pub struct ConnectAsync<S> {
    inner: MidHandshake<S>,
}

impl<S: Read + Write> Future for ConnectAsync<S> {
    type Item = TlsStream<S>;
    type Error = Error;

    fn poll(&mut self) -> Poll<TlsStream<S>, Error> {
        self.inner.poll()
    }
}

struct MidHandshake<S> {
    inner: Option<Result<native_tls::TlsStream<S>, HandshakeError<S>>>,
}

impl<S: Read + Write> Future for MidHandshake<S> {
    type Item = TlsStream<S>;
    type Error = Error;

    fn poll(&mut self) -> Poll<TlsStream<S>, Error> {
        match self.inner.take().expect("cannot poll MidHandshake twice") {
            Ok(stream) => Ok(TlsStream { inner: stream }.into()),
            Err(HandshakeError::Failure(e)) => Err(e),
            Err(HandshakeError::WouldBlock(s)) => match s.handshake() {
                Ok(stream) => Ok(TlsStream { inner: stream }.into()),
                Err(HandshakeError::Failure(e)) => Err(e),
                Err(HandshakeError::WouldBlock(s)) => {
                    self.inner = Some(Err(HandshakeError::WouldBlock(s)));
                    Ok(Async::NotReady)
                }
            },
        }
    }
}

/// Connects the provided stream with this connector, assuming the provided
/// domain.
///
/// This function will internally call `TlsConnector::connect` to connect
/// the stream and returns a future representing the resolution of the
/// connection operation. The returned future will resolve to either
/// `TlsStream<S>` or `Error` depending if it's successful or not.
///
/// This is typically used for clients who have already established, for
/// example, a TCP connection to a remote server. That stream is then
/// provided here to perform the client half of a connection to a
/// TLS-powered server.
///
/// # Compatibility notes
///
/// Note that this method currently requires `S: Read + Write` but it's
/// highly recommended to ensure that the object implements the `AsyncRead`
/// and `AsyncWrite` traits as well, otherwise this function will not work
/// properly.
pub fn connect_async<S>(connector: &TlsConnector, domain: &str, stream: S) -> ConnectAsync<S>
where
    S: Read + Write,
{
    ConnectAsync {
        inner: MidHandshake {
            inner: Some(connector.connect(domain, stream)),
        },
    }
}
