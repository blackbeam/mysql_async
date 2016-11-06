use conn::pool::Pool;
use errors::*;
use lib_futures::Async::NotReady;
use lib_futures::Async::Ready;
use lib_futures::Future;
use lib_futures::Poll;

pub struct DisconnectPool {
    pool: Pool,
}

pub fn new(pool: Pool) -> DisconnectPool {
    DisconnectPool {
        pool: pool,
    }
}

impl Future for DisconnectPool {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try!(self.pool.handle_futures());

        let new_len = self.pool.inner_ref().new.len();
        let dropping_len = self.pool.inner_ref().dropping.len();
        let disconnecting_len = self.pool.inner_ref().disconnecting.len();
        if (new_len, dropping_len, disconnecting_len) == (0, 0, 0) {
            Ok(Ready(()))
        } else {
            Ok(NotReady)
        }
    }
}
