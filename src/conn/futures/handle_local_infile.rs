// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use conn::Conn;
use conn::futures::WritePacket;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Failed;
use lib_futures::failed;
use lib_futures::Future;
use lib_futures::Poll;
use opts::LocalInfileHandler;
use std::sync::Arc;
use tokio_io::AsyncRead;
use tokio_io::io::read;
use tokio_io::io::Read;

enum Step {
    Failed(Failed<(), Error>),
    ReadData(Read<Box<AsyncRead>, Vec<u8>>),
    WritePacket(WritePacket),
    Done(WritePacket),
}

enum Out {
    ReadData((Box<AsyncRead>, Vec<u8>, usize)),
    WritePacket(Conn),
    Done(Conn),
}

pub struct HandleLocalInfile {
    step: Step,
    conn: Option<Conn>,
    reader: Option<Box<AsyncRead>>,
}

impl HandleLocalInfile {
    fn either_poll(&mut self) -> Result<Async<Out>> {
        match self.step {
            Step::Failed(ref mut fut) => {
                try_ready!(fut.poll());
                unreachable!()
            },
            Step::ReadData(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::ReadData(val)))
            },
            Step::WritePacket(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::WritePacket(val)))
            },
            Step::Done(ref mut fut) => {
                let val = try_ready!(fut.poll());
                Ok(Ready(Out::Done(val)))
            },
        }
    }
}

pub fn new(conn: Conn,
           filename: &[u8],
           handler: Option<Arc<LocalInfileHandler>>)
           -> HandleLocalInfile {
    match handler {
        Some(handler) => match handler.handle(filename) {
            Ok(reader) => {
                let mut buf = Vec::with_capacity(4096);
                unsafe { buf.set_len(4096); }
                HandleLocalInfile {
                    step: Step::ReadData(read(reader, buf)),
                    reader: None,
                    conn: Some(conn),
                }
            },
            Err(err) => HandleLocalInfile {
                step: Step::Failed(failed(err.into())),
                reader: None,
                conn: None,
            },
        },
        None => HandleLocalInfile {
            step: Step::Failed(failed(ErrorKind::NoLocalInfileHandler.into())),
            reader: None,
            conn: None,
        }
    }
}

impl Future for HandleLocalInfile {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::ReadData((reader, mut buf, count)) => {
                unsafe { buf.set_len(count); }
                let conn = self.conn.take().expect("Should be here");
                let write_packet = conn.write_packet(&buf[..count]);
                self.step = if count > 0 {
                    self.reader = Some(reader);
                    Step::WritePacket(write_packet)
                } else {
                    Step::Done(write_packet)
                };
                self.poll()
            },
            Out::WritePacket(conn) => {
                self.conn = Some(conn);
                let reader = self.reader.take().expect("Should be here");
                let mut buf = Vec::with_capacity(4096);
                unsafe { buf.set_len(4096); }
                self.step = Step::ReadData(read(reader, buf));
                self.poll()
            },
            Out::Done(conn) => Ok(Ready(conn)),
        }
    }
}
