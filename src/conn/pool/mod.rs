// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use self::futures::*;
use conn::Conn;
use errors::*;
use lib_futures::task::{self, Task};
use lib_futures::Async;
use lib_futures::Async::NotReady;
use lib_futures::Async::Ready;
use lib_futures::Future;
use opts::Opts;
use queryable::transaction::{Transaction, TransactionOptions};
use queryable::Queryable;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use BoxFuture;
use MyFuture;

pub mod futures;

pub struct Inner {
    closed: bool,
    new: Vec<BoxFuture<Conn>>,
    idle: Vec<Conn>,
    disconnecting: Vec<BoxFuture<()>>,
    dropping: Vec<BoxFuture<Conn>>,
    rollback: Vec<BoxFuture<Conn>>,
    ongoing: usize,
    tasks: Vec<Task>,
}

impl Inner {
    fn conn_count(&self) -> usize {
        self.new.len()
            + self.idle.len()
            + self.disconnecting.len()
            + self.dropping.len()
            + self.rollback.len()
            + self.ongoing
    }
}

#[derive(Clone)]
/// Asynchronous pool of MySql connections.
pub struct Pool {
    opts: Opts,
    inner: Arc<Mutex<Inner>>,
    min: usize,
    max: usize,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (new_len, idle_len, disconnecing_len, dropping_len, rollback_len, ongoing, tasks_len) =
            self.with_inner(|inner| {
                (
                    inner.new.len(),
                    inner.idle.len(),
                    inner.disconnecting.len(),
                    inner.dropping.len(),
                    inner.rollback.len(),
                    inner.ongoing,
                    inner.tasks.len(),
                )
            });
        f.debug_struct("Pool")
            .field("min", &self.min)
            .field("max", &self.max)
            .field("new connections count", &new_len)
            .field("idle connections count", &idle_len)
            .field("disconnecting connections count", &disconnecing_len)
            .field("dropping connections count", &dropping_len)
            .field("rollback connections count", &rollback_len)
            .field("ongoing connections count", &ongoing)
            .field("tasks count", &tasks_len)
            .finish()
    }
}

impl Pool {
    /// Creates new pool of connections.
    pub fn new<O: Into<Opts>>(opts: O) -> Pool {
        let opts = opts.into();
        let pool_min = opts.get_pool_min();
        let pool_max = opts.get_pool_max();
        let pool = Pool {
            opts: opts,
            inner: Arc::new(Mutex::new(Inner {
                closed: false,
                new: Vec::with_capacity(pool_min),
                idle: Vec::new(),
                disconnecting: Vec::new(),
                dropping: Vec::new(),
                rollback: Vec::new(),
                ongoing: 0,
                tasks: Vec::new(),
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

    /// Shortcut for `get_conn` followed by `start_transaction`.
    pub fn start_transaction(
        &self,
        options: TransactionOptions,
    ) -> impl MyFuture<Transaction<Conn>> {
        self.get_conn()
            .and_then(|conn| Queryable::start_transaction(conn, options))
    }

    /// Returns future that disconnects this pool from server and resolves to `()`.
    ///
    /// Active connections taken from this pool should be disconnected manually.
    /// Also all pending and new `GetConn`'s will resolve to error.
    pub fn disconnect(mut self) -> DisconnectPool {
        let become_closed = self.with_inner(|mut inner| {
            if !inner.closed {
                inner.closed = true;
                return true;
            } else {
                return false;
            }
        });
        if become_closed {
            while let Some(conn) = self.take_conn() {
                self.with_inner(move |mut inner| inner.disconnecting.push(conn.disconnect()));
            }
        }
        new_disconnect_pool(self)
    }

    /// Returns true if futures is in queue.
    fn in_queue(&self) -> bool {
        self.with_inner(|inner| {
            let count = inner.new.len()
                + inner.disconnecting.len()
                + inner.dropping.len()
                + inner.rollback.len();
            count > 0
        })
    }

    /// A way to take connection from a pool.
    fn take_conn(&mut self) -> Option<Conn> {
        if self.in_queue() {
            // Do not return connection until queue is empty
            return None;
        }
        self.with_inner(|mut inner| {
            while let Some(mut conn) = inner.idle.pop() {
                if conn.expired() {
                    inner.disconnecting.push(conn.disconnect());
                } else {
                    conn.pool = Some(self.clone());
                    inner.ongoing += 1;
                    return Some(conn);
                }
            }
            None
        })
    }

    /// A way to return connection taken from a pool.
    fn return_conn(&mut self, conn: Conn) {
        let min = self.min;

        self.with_inner(|mut inner| {
            if inner.closed {
                return;
            }

            if conn.has_result.is_some() {
                inner.dropping.push(Box::new(conn.drop_result()));
            } else if conn.in_transaction {
                inner.rollback.push(Box::new(conn.rollback_transaction()));
            } else {
                if inner.idle.len() >= min {
                    inner.disconnecting.push(conn.disconnect());
                } else {
                    inner.ongoing -= 1;
                    inner.idle.push(conn);
                }
            }

            while let Some(task) = inner.tasks.pop() {
                task.notify()
            }
        });
    }

    fn with_inner<F, T>(&self, fun: F) -> T
    where
        F: FnOnce(MutexGuard<Inner>) -> T,
        T: 'static,
    {
        fun(self.inner.lock().unwrap())
    }

    /// Will manage lifetime of futures stored in a pool.
    fn handle_futures(&mut self) -> Result<()> {
        if !self.in_queue() {
            // There is no futures in queue
            return Ok(());
        }

        macro_rules! handle {
            ($vec:ident { $($p:pat => $b:block,)+ }) => ({
                let len = self.with_inner(|inner| inner.$vec.len());
                let mut done_fut_idxs = Vec::new();
                for i in 0..len {
                    let result = self.with_inner(|mut inner| inner.$vec.get_mut(i).unwrap().poll());
                    match result {
                        Ok(Ready(_)) | Err(_) => done_fut_idxs.push(i),
                        _ => (),
                    }

                    let out: Result<()> = match result {
                        $($p => $b),+
                        _ => {
                            Ok(())
                        }
                    };

                    match out {
                        Err(err) => {
                            // early return in case of error
                            while let Some(i) = done_fut_idxs.pop() {
                                let _ = self.with_inner(|mut inner| inner.$vec.swap_remove(i));
                            }
                            return Err(err)
                        }
                        _ => (),
                    }
                }

                while let Some(i) = done_fut_idxs.pop() {
                    let _ = self.with_inner(|mut inner| inner.$vec.swap_remove(i));
                }
            });
        }

        let mut handled = false;

        // Handle closing connections.
        handle!(disconnecting {
            Ok(Ready(_)) => {
                handled = true;
                Ok(())
            },
            Err(_) => { Ok(()) },
        });

        // Handle dirty connections.
        handle!(dropping {
            Ok(Ready(conn)) => {
                let closed = self.with_inner(|inner| inner.closed);
                if closed {
                    self.with_inner(|mut inner| inner.disconnecting.push(conn.disconnect()));
                } else {
                    self.return_conn(conn);
                }
                handled = true;
                Ok(())
            },
            Err(_) => { Ok(()) },
        });

        // Handle in-transaction connections
        handle!(rollback {
            Ok(Ready(conn)) => {
                let closed = self.with_inner(|inner| inner.closed);
                if closed {
                    self.with_inner(|mut inner| inner.disconnecting.push(conn.disconnect()));
                } else {
                    self.return_conn(conn);
                }
                handled = true;
                Ok(())
            },
            Err(_) => { Ok(()) },
        });

        // Handle connecting connections.
        handle!(new {
            Ok(Ready(conn)) => {
                let closed = self.with_inner(|inner| inner.closed);
                if closed {
                    self.with_inner(|mut inner| inner.disconnecting.push(conn.disconnect()));
                } else {
                    self.with_inner(|mut inner| inner.ongoing += 1);
                    self.return_conn(conn);
                }
                handled = true;
                Ok(())
            },
            Err(err) => {
                if ! self.with_inner(|inner| inner.closed) {
                    Err(err)
                } else {
                    Ok(())
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
        if self.with_inner(|inner| inner.closed) {
            return Err(ErrorKind::PoolDisconnected.into());
        }

        self.handle_futures()?;

        match self.take_conn() {
            Some(conn) => Ok(Ready(conn)),
            None => {
                let new_conn_created = self.with_inner(|mut inner| {
                    if inner.new.len() == 0 && inner.conn_count() < self.max {
                        let new_conn = Conn::new(self.opts.clone());
                        inner.new.push(Box::new(new_conn));
                        true
                    } else {
                        inner.tasks.push(task::current());
                        false
                    }
                });
                if new_conn_created {
                    self.poll()
                } else {
                    Ok(NotReady)
                }
            }
        }
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        if let Some(mut pool) = self.pool.take() {
            let conn = self.take();
            if conn.stream.is_some() {
                pool.return_conn(conn)
            } // drop incomplete connection
        }
    }
}

#[cfg(test)]
mod test {
    use conn::pool::Pool;
    use lib_futures::Future;
    use queryable::Queryable;
    use test_misc::DATABASE_URL;
    use tokio;
    use TransactionOptions;

    /// Same as `tokio::run`, but will panic if future panics and will return the result
    /// of future execution.
    fn run<F, T, U>(future: F) -> Result<T, U>
    where
        F: Future<Item = T, Error = U> + Send + 'static,
        T: Send + 'static,
        U: Send + 'static,
    {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(future);
        runtime.shutdown_on_idle().wait().unwrap();
        result
    }

    #[test]
    fn should_connect() {
        let pool = Pool::new(&**DATABASE_URL);
        let fut = pool
            .get_conn()
            .and_then(|conn| conn.ping().map(|_| ()))
            .and_then(|_| pool.disconnect());

        run(fut).unwrap();
    }

    #[test]
    fn should_start_transaction() {
        let pool = Pool::new(format!("{}?pool_min=1&pool_max=1", &**DATABASE_URL));
        let fut = pool
            .get_conn()
            .and_then(|conn| conn.drop_query("CREATE TABLE IF NOT EXISTS tmp(id int)"))
            .and_then({
                let pool = pool.clone();
                move |_| pool.start_transaction(TransactionOptions::default())
            })
            .and_then(|transaction| {
                transaction.batch_exec("INSERT INTO tmp (id) VALUES (?)", vec![(1,), (2,)])
            })
            .and_then(|transaction| transaction.prep_exec("SELECT * FROM tmp", ()))
            .map(|_| ())
            .and_then({
                let pool = pool.clone();
                move |_| pool.get_conn()
            })
            .and_then(|conn| conn.first("SELECT COUNT(*) FROM tmp"))
            .and_then(|(_, row_opt)| {
                assert_eq!(row_opt, Some((0u8,)));
                pool.get_conn()
                    .and_then(|conn| conn.drop_query("DROP TABLE tmp"))
                    .and_then(move |_| pool.disconnect())
            });

        run(fut).unwrap();
    }

    #[test]
    fn should_hold_bounds() {
        let pool = Pool::new(format!("{}?pool_min=1&pool_max=2", &**DATABASE_URL));
        let pool_clone = pool.clone();
        let fut = pool
            .get_conn()
            .join(pool.get_conn())
            .and_then(move |(mut conn1, conn2)| {
                let new_conn = pool_clone.get_conn();
                conn1.pool.as_mut().unwrap().handle_futures().unwrap();
                assert_eq!(
                    conn1
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.new.len()),
                    0
                );
                assert_eq!(
                    conn1
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.idle.len()),
                    0
                );
                assert_eq!(
                    conn2
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.disconnecting.len()),
                    0
                );
                assert_eq!(
                    conn2
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.dropping.len()),
                    0
                );
                new_conn
            })
            .and_then(|conn1| {
                assert_eq!(
                    conn1
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.new.len()),
                    0
                );
                assert_eq!(
                    conn1
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.idle.len()),
                    0
                );
                assert_eq!(
                    conn1
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.disconnecting.len()),
                    0
                );
                assert_eq!(
                    conn1
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.dropping.len()),
                    0
                );
                Ok(())
            })
            .and_then(|_| {
                assert_eq!(pool.with_inner(|inner| inner.new.len()), 0);
                assert_eq!(pool.with_inner(|inner| inner.idle.len()), 1);
                assert_eq!(pool.with_inner(|inner| inner.disconnecting.len()), 0);
                assert_eq!(pool.with_inner(|inner| inner.dropping.len()), 0);
                pool.disconnect()
            });

        run(fut).unwrap();
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use conn::pool::Pool;
        use lib_futures::Future;
        use queryable::Queryable;
        use test;
        use test_misc::DATABASE_URL;
        use tokio;

        #[bench]
        fn connect(bencher: &mut test::Bencher) {
            let mut runtime = tokio::executor::current_thread::CurrentThread::new();
            let pool = Pool::new(&**DATABASE_URL);

            bencher.iter(|| {
                let fut = pool.get_conn().and_then(|conn| conn.ping());
                runtime.block_on(fut).unwrap();
            });

            runtime.block_on(pool.disconnect()).unwrap();
        }
    }
}
