// Copyright (c) 2019 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bytes::{Buf, BufMut};
use futures::{future::Future, Async};
use tokio::io::{AsyncRead, AsyncWrite};

use std::{
    io::{self, Read, Write},
    path::Path,
};

/// Unix domain socket connection on unix, or named pipe connection on windows.
#[derive(Debug)]
pub struct Socket {
    #[cfg(unix)]
    inner: tokio_uds::UnixStream,
    #[cfg(windows)]
    inner: tokio_named_pipes::NamedPipe,
}

impl Socket {
    /// Connects a new socket.
    #[cfg(unix)]
    pub fn new<P: AsRef<Path>>(path: P) -> impl Future<Item = Socket, Error = io::Error> {
        tokio_uds::UnixStream::connect(path).map(|socket| Socket { inner: socket })
    }

    /// Connects a new socket.
    #[cfg(windows)]
    pub fn new<P: AsRef<Path>>(path: P) -> impl Future<Item = Socket, Error = io::Error> {
        use futures::future::IntoFuture;
        use tokio_named_pipes::NamedPipe;

        let handle = tokio::reactor::Handle::default();
        NamedPipe::new(path.as_ref(), &handle)
            .and_then(|pipe| {
                pipe.connect()?;
                Ok(Socket { inner: pipe })
            })
            .into_future()
    }
}

impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.inner.read(buf)
    }
}

impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.inner.flush()
    }
}

impl AsyncRead for Socket {
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        self.inner.prepare_uninitialized_buffer(buf)
    }

    fn read_buf<B: BufMut>(&mut self, buf: &mut B) -> Result<Async<usize>, io::Error>
    where
        Self: Sized,
    {
        self.inner.read_buf(buf)
    }
}

impl AsyncWrite for Socket {
    fn shutdown(&mut self) -> Result<Async<()>, io::Error> {
        AsyncWrite::shutdown(&mut self.inner)
    }

    fn write_buf<B: Buf>(&mut self, buf: &mut B) -> Result<Async<usize>, io::Error>
    where
        Self: Sized,
    {
        self.inner.write_buf(buf)
    }
}
