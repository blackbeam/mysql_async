// Copyright (c) 2019 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bytes::BufMut;
use pin_project::pin_project;
use tokio::{
    io::{Error, ErrorKind::Interrupted},
    prelude::*,
};

use std::{
    io,
    mem::MaybeUninit,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

/// Unix domain socket connection on unix, or named pipe connection on windows.
#[pin_project]
#[derive(Debug)]
pub(crate) struct Socket {
    #[pin]
    #[cfg(unix)]
    inner: tokio::net::UnixStream,
    #[pin]
    #[cfg(windows)]
    inner: tokio::io::PollEvented<mio_named_pipes::NamedPipe>,
}

impl Socket {
    /// Connects a new socket.
    #[cfg(unix)]
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Socket, io::Error> {
        Ok(Socket {
            inner: tokio::net::UnixStream::connect(path).await?,
        })
    }

    /// Connects a new socket.
    #[cfg(windows)]
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Socket, io::Error> {
        let pipe = mio_named_pipes::NamedPipe::new(path.as_ref())?;
        pipe.connect()?;
        Ok(Socket {
            inner: tokio::io::PollEvented::new(pipe)?,
        })
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Error>> {
        let mut this = self.project();
        with_interrupted!(this.inner.as_mut().poll_read(cx, buf))
    }

    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        self.inner.prepare_uninitialized_buffer(buf)
    }

    fn poll_read_buf<B>(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut B,
    ) -> Poll<Result<usize, Error>>
    where
        B: BufMut,
    {
        let mut this = self.project();
        with_interrupted!(this.inner.as_mut().poll_read_buf(cx, buf))
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let mut this = self.project();
        with_interrupted!(this.inner.as_mut().poll_write(cx, buf))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        let mut this = self.project();
        with_interrupted!(this.inner.as_mut().poll_flush(cx))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        let mut this = self.project();
        with_interrupted!(this.inner.as_mut().poll_shutdown(cx))
    }
}
