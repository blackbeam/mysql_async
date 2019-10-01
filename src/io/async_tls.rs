// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

//! This module implements async TLS streams and source
//! of this module is mostly copyed from tokio_tls crate.

use bytes::buf::BufMut;
use native_tls::{Error, TlsConnector};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::Error as IoError;
use tokio::prelude::*;
use tokio_tls::{self};

/// A wrapper around an underlying raw stream which implements the TLS or SSL
/// protocol.
///
/// A `TlsStream<S>` represents a handshake that has been completed successfully
/// and both the server and the client are ready for receiving and sending
/// data. Bytes read from a `TlsStream` are decrypted from `S` and bytes written
/// to a `TlsStream` are encrypted when passing through to `S`.
#[pin_project]
#[derive(Debug)]
pub struct TlsStream<S> {
    #[pin]
    inner: tokio_tls::TlsStream<S>,
}

impl<S> TlsStream<S> {
    /// Get access to the internal `tokio_tls::TlsStream` stream which also
    /// transitively allows access to `S`.
    pub fn get_ref(&self) -> &tokio_tls::TlsStream<S> {
        &self.inner
    }
}

impl<S> AsyncRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, IoError>> {
        self.project().inner.poll_read(cx, buf)
    }

    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        self.inner.prepare_uninitialized_buffer(buf)
    }

    fn poll_read_buf<B>(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut B,
    ) -> Poll<Result<usize, IoError>>
    where
        B: BufMut,
    {
        self.project().inner.poll_read_buf(cx, buf)
    }
}

impl<S> AsyncWrite for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, IoError>> {
        self.project().inner.poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), IoError>> {
        self.project().inner.poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), IoError>> {
        self.project().inner.poll_shutdown(cx)
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
pub async fn connect_async<S>(
    connector: &TlsConnector,
    domain: &str,
    stream: S,
) -> Result<TlsStream<S>, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let connector = tokio_tls::TlsConnector::from(connector.clone());
    let connect = connector.connect(domain, stream);
    Ok(TlsStream {
        inner: connect.await?,
    })
}
