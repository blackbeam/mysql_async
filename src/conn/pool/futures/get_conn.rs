use conn::Conn;
use conn::pool::Pool;
use errors::*;
use lib_futures::Future;
use lib_futures::Poll;


/// This future will take connection from a pool and resolve to `Conn`.
pub struct GetConn {
    pool: Pool,
}

pub fn new(pool: &Pool) -> GetConn {
    GetConn {
        pool: pool.clone(),
    }
}

impl Future for GetConn {
    type Item = Conn;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.pool.poll()
    }
}
