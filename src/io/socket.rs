// Copyright (c) 2019 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bytes::BufMut;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::Error;
use tokio::prelude::*;

use std::{io, path::Path};

/// Unix domain socket connection on unix, or named pipe connection on windows.
#[cfg(unix)]
#[pin_project]
#[derive(Debug)]
pub struct Socket {
    #[pin]
    inner: tokio::net::unix::UnixStream,
}

#[cfg(windows)]
#[pin_project]
#[derive(Debug)]
pub struct Socket {
    #[pin]
    inner: tokio_named_pipes::NamedPipe,
}

impl Socket {
    /// Connects a new socket.
    #[cfg(unix)]
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Socket, io::Error> {
        Ok(Socket {
            inner: tokio::net::unix::UnixStream::connect(path).await?,
        })
    }

    /// Connects a new socket.
    #[cfg(windows)]
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Socket, io::Error> {
        use tokio_named_pipes::NamedPipe;
        let pipe = NamedPipe::new(path.as_ref()).await?;
        pipe.connect().await?;
        Ok(Socket { inner: pipe })
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Error>> {
        self.project().inner.poll_read(cx, buf)
    }

    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        self.inner.prepare_uninitialized_buffer(buf)
    }

    fn poll_read_buf<B>(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut B,
    ) -> Poll<Result<usize, Error>>
    where
        B: BufMut,
    {
        self.project().inner.poll_read_buf(cx, buf)
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        self.project().inner.poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        self.project().inner.poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        self.project().inner.poll_shutdown(cx)
    }
}
