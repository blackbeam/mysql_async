// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use errors::*;
use io::Stream;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Failed;
use lib_futures::failed;
use lib_futures::Future;
use lib_futures::Poll;
use lib_futures::future::select_ok;
use lib_futures::future::SelectOk;
use myc::packets::PacketParser;
use net2::TcpBuilder;
use std::io;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use tokio::net::TcpStream;
use tokio::net::ConnectFuture;
use tokio::reactor::Handle;


steps! {
    ConnectingStream {
        WaitForStream(SelectOk<ConnectFuture>),
        Fail(Failed<(), Error>),
    }
}

// Taken from https://github.com/hyperium/hyper/commit/27b8db3af8852ba8280a2868f703d3230a1db85e
fn connect(addr: &SocketAddr, handle: &Handle) -> io::Result<ConnectFuture> {
    let builder = match addr {
        &SocketAddr::V4(_) => TcpBuilder::new_v4()?,
        &SocketAddr::V6(_) => TcpBuilder::new_v6()?,
    };

    if cfg!(windows) {
        // Windows requires a socket be bound before calling connect
        let any: SocketAddr = match addr {
            &SocketAddr::V4(_) => {
                ([0, 0, 0, 0], 0).into()
            },
            &SocketAddr::V6(_) => {
                ([0, 0, 0, 0, 0, 0, 0, 0], 0).into()
            }
        };
        builder.bind(any)?;
    }

    Ok(TcpStream::connect_std(builder.to_tcp_stream()?, addr, handle))
}

/// Future that resolves to a `Stream` connected to a MySql server.
pub struct ConnectingStream {
    step: Step,
}

pub fn new<S>(addr: S, handle: &Handle) -> ConnectingStream
where
    S: ToSocketAddrs,
{
    match addr.to_socket_addrs() {
        Ok(addresses) => {
            let mut streams = Vec::new();

            for address in addresses {
                if let Ok(stream) = connect(&address, handle) {
                    streams.push(stream);
                }
            }

            if streams.len() > 0 {
                ConnectingStream { step: Step::WaitForStream(select_ok(streams)) }
            } else {
                let err = io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve to any address",
                );
                ConnectingStream { step: Step::Fail(failed(err.into())) }
            }
        }
        Err(err) => ConnectingStream { step: Step::Fail(failed(err.into())) },
    }
}

impl Future for ConnectingStream {
    type Item = Stream;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            Out::WaitForStream((stream, _)) => {
                Ok(Ready(Stream {
                    closed: false,
                    next_packet: Some(PacketParser::empty().parse()),
                    buf: Some(Vec::new()),
                    endpoint: Some(stream.into()),
                }))
            }
            Out::Fail(_) => unreachable!(),
        }
    }
}
