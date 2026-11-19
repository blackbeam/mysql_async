// Copyright (c) 2019 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

#![cfg(any(unix, windows))]

use tokio::io::{Error, ErrorKind::Interrupted, ReadBuf};

use std::{
    io,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};

/// Unix domain socket connection on unix, or named pipe connection on windows.
#[derive(Debug)]
pub(crate) struct Socket {
    #[cfg(unix)]
    inner: tokio::net::UnixStream,
    #[cfg(windows)]
    inner: tokio::net::windows::named_pipe::NamedPipeClient,
}

impl Socket {
    /// Connects a new socket.
    #[cfg(unix)]
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Socket, io::Error> {
        Ok(Socket {
            inner: tokio::net::UnixStream::connect(path).await?,
        })
    }

    /// Connects a new named pipe.
    ///
    /// `path` is either a full pipe path (`\\.\pipe\MySQL`, `\\server\pipe\MySQL`)
    /// or a bare pipe name (`MySQL`), as the server reports it in `@@socket`,
    /// which names a pipe on the local machine.
    #[cfg(windows)]
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Socket, io::Error> {
        use std::{
            ffi::OsString,
            fs::OpenOptions,
            os::windows::{fs::OpenOptionsExt, io::IntoRawHandle},
            time::Duration,
        };
        use tokio::{
            net::windows::named_pipe::NamedPipeClient,
            time::{sleep, Instant},
        };

        // The access MySQL's own client asks for. The server grants clients outside
        // `named_pipe_full_access_group` only these, so `GENERIC_WRITE` (which includes
        // the right to create pipe instances) is refused.
        const FILE_READ_DATA: u32 = 0x0001;
        const FILE_WRITE_DATA: u32 = 0x0002;
        const FILE_READ_ATTRIBUTES: u32 = 0x0080;
        const FILE_WRITE_ATTRIBUTES: u32 = 0x0100;
        const FILE_FLAG_OVERLAPPED: u32 = 0x4000_0000;
        const SECURITY_IDENTIFICATION: u32 = 0x0001_0000;
        /// All pipe instances are busy.
        const ERROR_PIPE_BUSY: i32 = 231;
        /// How long to wait for a busy pipe to free an instance.
        const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

        let path = path.as_ref().as_os_str();
        let name = if path.to_string_lossy().starts_with(r"\\") {
            path.to_owned()
        } else {
            let mut name = OsString::from(r"\\.\pipe\");
            name.push(path);
            name
        };

        // The server creates a new pipe instance for each accepted connection,
        // so a busy pipe frees one shortly.
        let deadline = Instant::now() + BUSY_TIMEOUT;
        let file = loop {
            let result = OpenOptions::new()
                .access_mode(
                    FILE_READ_DATA | FILE_WRITE_DATA | FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES,
                )
                .share_mode(0)
                .custom_flags(FILE_FLAG_OVERLAPPED)
                .security_qos_flags(SECURITY_IDENTIFICATION)
                .open(&name);
            match result {
                Ok(file) => break file,
                Err(err)
                    if err.raw_os_error() == Some(ERROR_PIPE_BUSY) && Instant::now() < deadline => {
                }
                Err(err) => return Err(err),
            }
            sleep(Duration::from_millis(50)).await;
        };

        // SAFETY: the handle is a named pipe opened for overlapped I/O, and is owned
        // by nothing else once taken out of `file`.
        let inner = unsafe { NamedPipeClient::from_raw_handle(file.into_raw_handle())? };
        Ok(Socket { inner })
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), Error>> {
        with_interrupted!(Pin::new(&mut self.inner).poll_read(cx, buf))
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        with_interrupted!(Pin::new(&mut self.inner).poll_write(cx, buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        with_interrupted!(Pin::new(&mut self.inner).poll_flush(cx))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        with_interrupted!(Pin::new(&mut self.inner).poll_shutdown(cx))
    }
}
