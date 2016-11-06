use conn::Conn;
use conn::futures::Disconnect;
use conn::futures::DropResult;
use conn::futures::NewConn;
use errors::*;
use lib_futures::Async;
use lib_futures::Async::Ready;
use lib_futures::Async::NotReady;
use lib_futures::Future;
use std::fmt;
use opts::Opts;
use self::futures::*;
use std::cell::Ref;
use std::cell::RefCell;
use std::cell::RefMut;
use std::rc::Rc;
use tokio::reactor::Handle;


pub mod futures;


pub struct Inner {
    closed: bool,
    new: Vec<NewConn>,
    idle: Vec<Conn>,
    disconnecting: Vec<Disconnect>,
    dropping: Vec<DropResult>,
}

#[derive(Clone)]
/// Asynchronous pool of MySql connections.
pub struct Pool {
    handle: Handle,
    opts: Opts,
    inner: Rc<RefCell<Inner>>,
    min: usize,
    max: usize,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Pool")
         .field("min", &self.min)
         .field("max", &self.max)
         .field("new connections count", &self.inner_ref().new.len())
         .field("idle connections count", &self.inner_ref().idle.len())
         .field("disconnecting connections count", &self.inner_ref().disconnecting.len())
         .finish()
    }
}

impl Pool {
    /// Creates new pool of connections.
    pub fn new<O: Into<Opts>>(opts: O, handle: &Handle) -> Pool {
        let opts = opts.into();
        let pool_min = opts.get_pool_min();
        let pool_max = opts.get_pool_max();
        let mut new_conns = Vec::with_capacity(pool_min);
        for _ in 0..pool_min {
            let new_conn = Conn::new(opts.clone(), handle);
            new_conns.push(new_conn);
        }
        let pool = Pool {
            handle: handle.clone(),
            opts: opts,
            inner: Rc::new(RefCell::new(Inner {
                closed: false,
                new: new_conns,
                idle: Vec::new(),
                disconnecting: Vec::new(),
                dropping: Vec::new(),
            })),
            min: pool_min,
            max: pool_max,
        };

        pool
    }

    /// Returns future that resolves to `Conn`.
    pub fn get_conn(&self) -> GetConn {
        new_get_conn(self)
    }

    /// Returns future that disconnects this pool from server and resolves to `()`.
    ///
    /// Active connections taken from this pool should be disconnected manually.
    /// Also all pending and new `GetConn`'s will resolve to error.
    pub fn disconnect(mut self) -> DisconnectPool {
        if ! self.inner_ref().closed {
            self.inner_mut().closed = true;
            while let Some(conn) = self.take_conn() {
                self.inner_mut().disconnecting.push(conn.disconnect());
            }
        }
        new_disconnect_pool(self)
    }

    /// A way to take connection from a pool.
    fn take_conn(&mut self) -> Option<Conn> {
        let maybe_conn = self.inner_mut().idle.pop();
        maybe_conn.map(|mut conn| {
            // TODO: Add some kind of health check
            // TODO: Execute init after reconnect
            conn.pool = Some(self.clone());
            conn
        })
    }

    /// A way to return connection taken from a pool.
    fn return_conn(&mut self, conn: Conn) {
        if self.inner_ref().closed {
            return;
        }
        if conn.has_result.is_some() {
            self.inner_mut().dropping.push(conn.drop_result());
        } else {
            if self.inner_ref().idle.len() >= self.min {
                self.inner_mut().disconnecting.push(conn.disconnect());
            } else {
                self.inner_mut().idle.push(conn);
            }
        }
    }

    fn inner_ref(&self) -> Ref<Inner> {
        self.inner.borrow()
    }

    fn inner_mut(&mut self) -> RefMut<Inner> {
        self.inner.borrow_mut()
    }

    /// Will manage lifetime of futures stored in a pool.
    fn handle_futures(&mut self) -> Result<()> {
        macro_rules! handle {
            ($vec:ident { $($p:pat => $b:block,)+ }) => ({
                let len = self.inner_ref().$vec.len();
                let mut done_fut_idxs = Vec::new();
                for i in 0..len {
                    let result = self.inner_mut().$vec.get_mut(i).unwrap().poll();
                    if let Ok(Ready(_)) = result {
                        done_fut_idxs.push(i);
                    }
                    match result {
                        $($p => $b),+
                        _ => (),
                    }
                }
                while let Some(i) = done_fut_idxs.pop() {
                    self.inner_mut().$vec.swap_remove(i);
                }
            });
        }

        let mut handled = false;

        // Handle closing connections.
        handle!(disconnecting {
            Ok(Ready(_)) => {},
            Err(_) => {},
        });

        // Handle dirty connections.
        handle!(dropping {
            Ok(Ready(conn)) => {
                if self.inner_ref().closed {
                    self.inner_mut().disconnecting.push(conn.disconnect());
                } else {
                    self.return_conn(conn);
                }
                handled = true;
            },
            Err(_) => {},
        });

        // Handle connecting connections.
        handle!(new {
            Ok(Ready(conn)) => {
                if self.inner_ref().closed {
                    self.inner_mut().disconnecting.push(conn.disconnect());
                } else {
                    self.return_conn(conn);
                }
                handled = true;
            },
            Err(err) => {
                if ! self.inner_ref().closed {
                    return Err(err);
                }
            },
        });

        if handled {
            self.handle_futures()
        } else {
            Ok(())
        }
    }

    /// Will poll pool for connection.
    fn poll(&mut self) -> Result<Async<Conn>> {
        if self.inner_ref().closed {
            return Err(ErrorKind::PoolDisconnected.into());
        }

        try!(self.handle_futures());

        match self.take_conn() {
            Some(conn) => Ok(Ready(conn)),
            None => if self.inner_ref().new.len() == 0 && self.inner_ref().idle.len() < self.max {
                let new_conn = Conn::new(self.opts.clone(), &self.handle);
                self.inner_mut().new.push(new_conn);
                self.poll()
            } else {
                Ok(NotReady)
            }
        }
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        if let Some(mut pool) = self.pool.take() {
            let conn = self.take();
            pool.return_conn(conn)
        }
    }
}

#[cfg(test)]
mod test {
    use conn::pool::Pool;
    use lib_futures::Future;
    use test_misc::DATABASE_URL;
    use tokio::reactor::Core;

    #[test]
    fn should_connect() {
        let mut lp = Core::new().unwrap();

        let pool = Pool::new(&**DATABASE_URL, &lp.handle());
        let fut = pool.get_conn().and_then(|conn| conn.ping().map(|_| ())).and_then(|_| {
            pool.disconnect()
        });

        lp.run(fut).unwrap();
    }

    #[test]
    fn should_hold_bounds() {
        let mut lp = Core::new().unwrap();

        let pool = Pool::new(format!("{}?pool_min=1&pool_max=2", &**DATABASE_URL), &lp.handle());
        let pool_clone = pool.clone();
        let fut = pool.get_conn().join(pool.get_conn()).and_then(|(mut conn1, conn2)| {
            let new_conn = pool_clone.get_conn();
            conn1.pool.as_mut().unwrap().handle_futures().unwrap();
            assert_eq!(conn1.pool.as_ref().unwrap().inner_ref().new.len(), 0);
            assert_eq!(conn1.pool.as_ref().unwrap().inner_ref().idle.len(), 0);
            assert_eq!(conn2.pool.as_ref().unwrap().inner_ref().disconnecting.len(), 0);
            assert_eq!(conn2.pool.as_ref().unwrap().inner_ref().dropping.len(), 0);
            new_conn
        }).and_then(|conn1| {
            assert_eq!(conn1.pool.as_ref().unwrap().inner_ref().new.len(), 0);
            assert_eq!(conn1.pool.as_ref().unwrap().inner_ref().idle.len(), 0);
            assert_eq!(conn1.pool.as_ref().unwrap().inner_ref().disconnecting.len(), 0);
            assert_eq!(conn1.pool.as_ref().unwrap().inner_ref().dropping.len(), 0);
            Ok(())
        }).and_then(|_| {
            assert_eq!(pool.inner_ref().new.len(), 0);
            assert_eq!(pool.inner_ref().idle.len(), 1);
            assert_eq!(pool.inner_ref().disconnecting.len(), 0);
            assert_eq!(pool.inner_ref().dropping.len(), 0);
            pool.disconnect()
        });

        lp.run(fut).unwrap();
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;
        use conn::pool::Pool;
        use tokio::reactor::Core;
        use lib_futures::Future;
        use test_misc::DATABASE_URL;

        #[bench]
        fn connect(bencher: &mut test::Bencher) {
            let mut lp = Core::new().unwrap();
            let pool = Pool::new(&**DATABASE_URL, &lp.handle());

            bencher.iter(|| {
                let fut = pool.get_conn().and_then(|conn| { conn.ping() });
                lp.run(fut).unwrap();
            })
        }
    }
}
