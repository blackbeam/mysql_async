// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures::{
    failed,
    future::{select_ok, SelectOk},
    try_ready,
    Async::{self, Ready},
    Failed, Future, Poll,
};
use tokio::net::{tcp::ConnectFuture, TcpStream};
use tokio_codec::Framed;

use std::{io, net::ToSocketAddrs};

use crate::{
    error::*,
    io::{packet_codec::PacketCodec, Stream},
};

steps! {
    ConnectingTcpStream {
        WaitForStream(SelectOk<ConnectFuture>),
        Fail(Failed<(), Error>),
    }
}

/// Future that resolves to a `Stream` connected to a MySql server.
pub struct ConnectingTcpStream {
    step: Step,
}

pub fn new<S>(addr: S) -> ConnectingTcpStream
where
    S: ToSocketAddrs,
{
    match addr.to_socket_addrs() {
        Ok(addresses) => {
            let mut streams = Vec::new();

            for address in addresses {
                streams.push(TcpStream::connect(&address));
            }

            if streams.len() > 0 {
                ConnectingTcpStream {
                    step: Step::WaitForStream(select_ok(streams)),
                }
            } else {
                let err = io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve to any address",
                );
                ConnectingTcpStream {
                    step: Step::Fail(failed(err.into())),
                }
            }
        }
        Err(err) => ConnectingTcpStream {
            step: Step::Fail(failed(err.into())),
        },
    }
}

impl Future for ConnectingTcpStream {
    type Item = Stream;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::WaitForStream((stream, _)) => Ok(Ready(Stream {
                closed: false,
                codec: Box::new(Framed::new(stream.into(), PacketCodec::new())).into(),
            })),
            Out::Fail(_) => unreachable!(),
        }
    }
}
