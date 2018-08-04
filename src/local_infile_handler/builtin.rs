// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use super::LocalInfileHandler;
use errors::*;
use lib_futures::{oneshot, Future, IntoFuture};
use mio::{Evented, Poll, PollOpt, Ready, Registration, Token};
use std::collections::HashSet;
use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;
use std::str::from_utf8;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::thread;
use tokio::reactor::{Handle, PollEvented, Remote};
use tokio_io::AsyncRead;
use BoxFuture;

#[derive(Debug)]
enum Message {
    FillBuf(usize),
    BufFilled(io::Result<Vec<u8>>),
    Done,
}

/// ReadAsync wrapper for `fs::File`.
pub struct File {
    to_thread: Sender<Message>,
    from_thread: Receiver<Message>,
    registration: Registration,
    waiting_for_readiness: bool,
}

impl File {
    pub fn new<T>(path: T) -> File
    where
        T: Into<PathBuf>,
    {
        let (to_main, from_thread) = channel();
        let (to_thread, from_main) = channel();
        let (registration, set_readiness) = Registration::new2();
        let path = path.into();

        thread::spawn(move || {
            let mut file = match fs::File::open(path) {
                Ok(file) => file,
                Err(err) => {
                    to_main.send(Message::BufFilled(Err(err))).unwrap();
                    set_readiness.set_readiness(Ready::readable()).unwrap();
                    return;
                }
            };
            set_readiness.set_readiness(Ready::readable()).unwrap();

            loop {
                match from_main.recv() {
                    Ok(Message::FillBuf(size)) => {
                        let mut buf = Vec::with_capacity(size);
                        unsafe {
                            buf.set_len(size);
                        }
                        let result = file.read(&mut buf[..]).map(|count| {
                            unsafe {
                                buf.set_len(count);
                            }
                            buf
                        });
                        to_main.send(Message::BufFilled(result)).unwrap();
                        set_readiness.set_readiness(Ready::readable()).unwrap();
                    }
                    Ok(Message::Done) | Err(_) => {
                        break;
                    }
                    Ok(x) => panic!("Unexpected message for file io thread: {:?}", x),
                }
            }
        });

        File {
            to_thread,
            from_thread,
            registration,
            waiting_for_readiness: false,
        }
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.from_thread.try_recv() {
            Ok(Message::BufFilled(data_buf)) => match data_buf {
                Ok(data_buf) => {
                    self.waiting_for_readiness = false;
                    if data_buf.len() == 0 && !buf.len() == 0 {
                        let _ = self.to_thread.send(Message::Done);
                    }
                    (&data_buf[..]).read(buf)
                }
                Err(err) => {
                    let _ = self.to_thread.send(Message::Done);
                    Err(err)
                }
            },
            Err(TryRecvError::Empty) => {
                if !self.waiting_for_readiness {
                    self.waiting_for_readiness = true;
                    match self.to_thread.send(Message::FillBuf(buf.len())) {
                        Ok(_) => (),
                        Err(_) => {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Read thread disconnected",
                            ))
                        }
                    }
                }
                Err(io::ErrorKind::WouldBlock.into())
            }
            Err(TryRecvError::Disconnected) => Err(io::Error::new(
                io::ErrorKind::Other,
                "Read thread disconnected",
            )),
            _ => panic!("Unexpected message"),
        }
    }
}

impl Evented for File {
    fn register(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.registration.register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        self.registration.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        Evented::deregister(&self.registration, poll)
    }
}

/// Handles local infile requests from filesystem using explicit path white list.
#[derive(Clone, Debug)]
pub struct WhiteListFsLocalInfileHandler {
    white_list: HashSet<PathBuf>,
    handle: Remote,
}

impl WhiteListFsLocalInfileHandler {
    pub fn new<A, B>(white_list: B, handle: &Handle) -> WhiteListFsLocalInfileHandler
    where
        A: Into<PathBuf>,
        B: IntoIterator<Item = A>,
    {
        let mut white_list_set = HashSet::new();
        for path in white_list.into_iter() {
            white_list_set.insert(Into::<PathBuf>::into(path));
        }
        WhiteListFsLocalInfileHandler {
            white_list: white_list_set,
            handle: handle.remote().clone(),
        }
    }
}

impl LocalInfileHandler for WhiteListFsLocalInfileHandler {
    fn handle(&self, file_name: &[u8]) -> BoxFuture<Box<AsyncRead>> {
        let path: PathBuf = match from_utf8(file_name) {
            Ok(path_str) => path_str.into(),
            Err(_) => return Box::new(Err("Invalid file name".into()).into_future()),
        };
        if self.white_list.contains(&path) {
            match self.handle.handle() {
                Some(handle) => {
                    let fut = PollEvented::new(File::new(path), &handle)
                        .map_err(Into::into)
                        .map(|poll_evented| Box::new(poll_evented) as Box<AsyncRead>)
                        .into_future();
                    Box::new(fut) as BoxFuture<Box<AsyncRead>>
                }
                None => {
                    let (tx, rx) = oneshot();
                    self.handle.spawn(|handle| {
                        let poll_evented_res = PollEvented::new(File::new(path), &handle);
                        let _ = tx.send(poll_evented_res.map_err(Error::from));
                        Ok(())
                    });
                    let fut = rx
                        .map_err(|_| Error::from("Future Canceled"))
                        .and_then(|r| r.into_future())
                        .map(|poll_evented| Box::new(poll_evented) as Box<AsyncRead>);
                    Box::new(fut) as BoxFuture<Box<AsyncRead>>
                }
            }
        } else {
            let err_msg = format!("Path `{}' is not in white list", path.display());
            return Box::new(Err(err_msg.into()).into_future());
        }
    }
}
