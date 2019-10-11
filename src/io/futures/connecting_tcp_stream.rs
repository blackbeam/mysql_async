// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::codec::Framed;
use tokio::net::TcpStream;

use std::{io, net::ToSocketAddrs};

use crate::{
    error::*,
    io::{PacketCodec, Stream},
};

pub async fn new<S>(addr: S) -> Result<Stream>
where
    S: ToSocketAddrs,
{
    match addr.to_socket_addrs() {
        Ok(addresses) => {
            let mut streams = FuturesUnordered::new();

            for address in addresses {
                streams.push(TcpStream::connect(address));
            }

            let mut err = None;
            while let Some(stream) = streams.next().await {
                match stream {
                    Err(e) => {
                        err = Some(e);
                    }
                    Ok(stream) => {
                        return Ok(Stream {
                            closed: false,
                            codec: Box::new(Framed::new(stream.into(), PacketCodec::default())).into(),
                        });
                    }
                }
            }

            if let Some(e) = err {
                Err(e.into())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve to any address",
                )
                .into())
            }
        }
        Err(err) => Err(err.into()),
    }
}
