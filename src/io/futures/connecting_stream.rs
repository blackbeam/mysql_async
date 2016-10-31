use errors::*;
use io::Stream;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Failed;
use lib_futures::failed;
use lib_futures::Future;
use lib_futures::Poll;
use proto::NewPacket;
use std::collections::VecDeque;
use std::io;
use std::net::ToSocketAddrs;
use tokio::net::TcpStream;
use tokio::net::TcpStreamNew;
use tokio::reactor::Handle;


enum Step {
    WaitForStream(TcpStreamNew),
    Fail(Failed<TcpStream, Error>),
}

/// Future that resolves to a `Stream` connected to a MySql server.
pub struct ConnectingStream
{
    step: Step,
}

pub fn new<S>(addr: S, handle: &Handle) -> ConnectingStream
where S: ToSocketAddrs,
{
    match addr.to_socket_addrs() {
        Ok(addrs) => {
            for addr in addrs {
                let future = TcpStream::connect(&addr, handle);
                return ConnectingStream {
                    step: Step::WaitForStream(future),
                }
            }
            let err = io::Error::new(io::ErrorKind::InvalidInput,
                                     "could not resolve to any address");
            ConnectingStream {
                step: Step::Fail(failed(Error::from(err))),
            }
        },
        Err(err) => ConnectingStream {
            step: Step::Fail(failed(Error::from(err))),
        }
    }
}

impl ConnectingStream {
    fn either_poll(&mut self) -> Result<Async<TcpStream>> {
        match self.step {
            Step::WaitForStream(ref mut fut) => Ok(Ready(try_ready!(fut.poll()))),
            Step::Fail(ref mut fut) => Ok(Ready(try_ready!(fut.poll()))),
        }
    }
}

impl Future for ConnectingStream
{
    type Item = Stream;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match try_ready!(self.either_poll()) {
            stream => {
                Ok(Ready(Stream {
                    closed: false,
                    next_packet: Some(NewPacket::empty().parse()),
                    buf: Some(VecDeque::new()),
                    endpoint: Some(stream)
                }))
            }
        }
    }
}