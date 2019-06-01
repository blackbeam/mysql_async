// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use ::futures::{
    task::{self, Task},
    Async::{self, NotReady, Ready},
    Future,
};

use std::{
    fmt,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    conn::{pool::futures::*, Conn},
    error::*,
    opts::{Opts, PoolConstraints},
    queryable::{
        transaction::{Transaction, TransactionOptions},
        Queryable,
    },
    BoxFuture, MyFuture,
};

pub mod futures;

pub struct Inner {
    closed: bool,
    new: Vec<BoxFuture<Conn>>,
    queue: Vec<BoxFuture<Conn>>,
    idle: Vec<Conn>,
    ongoing: usize,
    tasks: Vec<Task>,
}

impl Inner {
    fn conn_count(&self) -> usize {
        self.new.len() + self.idle.len() + self.queue.len() + self.ongoing
    }
}

#[derive(Clone)]
/// Asynchronous pool of MySql connections.
pub struct Pool {
    opts: Opts,
    inner: Arc<Mutex<Inner>>,
    pool_constraints: PoolConstraints,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (new_len, idle_len, queue_len, ongoing, tasks_len) = self.with_inner(|inner| {
            (
                inner.new.len(),
                inner.idle.len(),
                inner.queue.len(),
                inner.ongoing,
                inner.tasks.len(),
            )
        });
        f.debug_struct("Pool")
            .field("pool_constraints", &self.pool_constraints)
            .field("new connections count", &new_len)
            .field("idle connections count", &idle_len)
            .field("queue length", &queue_len)
            .field("ongoing connections count", &ongoing)
            .field("tasks count", &tasks_len)
            .finish()
    }
}

impl Pool {
    /// Creates new pool of connections.
    pub fn new<O: Into<Opts>>(opts: O) -> Pool {
        let opts = opts.into();
        let pool_constraints = opts.get_pool_constraints().clone();
        let pool = Pool {
            opts,
            inner: Arc::new(Mutex::new(Inner {
                closed: false,
                new: Vec::with_capacity(pool_constraints.min()),
                idle: Vec::new(),
                queue: Vec::new(),
                ongoing: 0,
                tasks: Vec::new(),
            })),
            pool_constraints,
        };

        pool
    }

    /// Creates new pool of connections.
    pub fn from_url<T: AsRef<str>>(url: T) -> Result<Pool> {
        let opts = Opts::from_str(url.as_ref())?;
        Ok(Pool::new(opts))
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
                crate::conn::disconnect(conn);
            }
        }
        new_disconnect_pool(self)
    }

    /// Returns true if futures is in queue.
    fn in_queue(&self) -> bool {
        self.with_inner(|inner| {
            let count = inner.new.len() + inner.queue.len();
            count > 0
        })
    }

    /// A way to take connection from a pool.
    fn take_conn(&mut self) -> Option<Conn> {
        self.with_inner(|mut inner| {
            while let Some(mut conn) = inner.idle.pop() {
                if conn.expired() {
                    crate::conn::disconnect(conn);
                } else {
                    conn.inner.pool = Some(self.clone());
                    inner.ongoing += 1;
                    return Some(conn);
                }
            }
            None
        })
    }

    /// A way to return connection taken from a pool.
    fn return_conn(&mut self, conn: Conn) {
        let min = self.pool_constraints.min();

        self.with_inner(|mut inner| {
            inner.ongoing -= 1;

            if inner.closed {
                return;
            }

            if conn.inner.stream.is_none() {
                // drop incomplete connection
                return;
            }

            if conn.inner.in_transaction || conn.inner.has_result.is_some() {
                inner.queue.push(conn.cleanup());
            } else {
                if inner.idle.len() >= min {
                    crate::conn::disconnect(conn);
                } else {
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
        F: FnOnce(MutexGuard<'_, Inner>) -> T,
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

        let mut handled = false;

        let mut returned_conns: Vec<Conn> = vec![];

        self.with_inner(|mut inner| {
            macro_rules! handle {
                ($vec:ident { $($p:pat => $b:block,)+ }) => ({
                    let len = inner.$vec.len();
                    let mut done_fut_idxs = Vec::new();

                    for i in 0..len {
                        let result = inner.$vec.get_mut(i).unwrap().poll();
                        match result {
                            Ok(Ready(_)) | Err(_) => done_fut_idxs.push(i),
                            _ => (),
                        }

                        let out: Result<()> = match result {
                            Ok(Ready(conn)) => {
                                if inner.closed {
                                    crate::conn::disconnect(conn);
                                } else {
                                    inner.ongoing += 1;
                                    returned_conns.push(conn);
                                }
                                handled = true;
                                Ok(())
                            }
                            $($p => $b),+
                            _ => {
                                Ok(())
                            }
                        };

                        match out {
                            Err(err) => {
                                // early return in case of error
                                while let Some(i) = done_fut_idxs.pop() {
                                    inner.$vec.swap_remove(i);
                                }
                                return Err(err)
                            }
                            _ => (),
                        }
                    }

                    while let Some(i) = done_fut_idxs.pop() {
                        inner.$vec.swap_remove(i);
                    }
                });
            }

            // Handle dirty connections.
            handle!(queue {
                // Drop it in case of error.
                Err(_) => { Ok(()) },
            });

            // Handle connecting connections.
            handle!(new {
                Err(err) => {
                    if !inner.closed {
                        Err(err)
                    } else {
                        Ok(())
                    }
                },
            });

            Ok(())
        })?;

        for conn in returned_conns {
            self.return_conn(conn);
        }

        if handled {
            self.handle_futures()
        } else {
            Ok(())
        }
    }

    /// Will poll pool for connection.
    fn poll(&mut self) -> Result<Async<Conn>> {
        if self.with_inner(|inner| inner.closed) {
            return Err(DriverError::PoolDisconnected.into());
        }

        self.handle_futures()?;

        match self.take_conn() {
            Some(conn) => Ok(Ready(conn)),
            None => {
                let new_conn_created = self.with_inner(|mut inner| {
                    if inner.new.len() == 0 && inner.conn_count() < self.pool_constraints.max() {
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
        if let Some(mut pool) = self.inner.pool.take() {
            pool.return_conn(self.take());
        } else if self.inner.stream.is_some() && !self.inner.disconnected {
            crate::conn::disconnect(self.take());
        }
    }
}

#[cfg(test)]
mod test {
    use futures::collect;
    use futures::Future;

    use crate::{
        conn::pool::Pool, queryable::Queryable, test_misc::DATABASE_URL, TransactionOptions,
    };

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
    fn should_hold_bounds2() {
        use std::cmp::max;

        const POOL_MIN: usize = 5;
        const POOL_MAX: usize = 10;

        let url = format!(
            "{}?pool_min={}&pool_max={}",
            &**DATABASE_URL, POOL_MIN, POOL_MAX
        );

        // Clean
        let pool = Pool::new(url.clone());
        let pool_clone = pool.clone();
        let conns = (0..POOL_MAX).map(|_| pool.get_conn()).collect::<Vec<_>>();

        let fut = ::futures::future::join_all(conns)
            .map(move |mut conns| {
                let mut popped = 0;
                assert_eq!(pool_clone.inner.lock().unwrap().conn_count(), POOL_MAX);

                while let Some(_) = conns.pop().map(drop) {
                    popped += 1;
                    assert_eq!(
                        pool_clone.inner.lock().unwrap().conn_count(),
                        POOL_MAX + POOL_MIN - max(popped, POOL_MIN)
                    );
                }

                pool_clone
            })
            .and_then(|pool| pool.disconnect());

        run(fut).unwrap();

        // Dirty
        let pool = Pool::new(url.clone());
        let pool_clone = pool.clone();
        let conns = (0..POOL_MAX)
            .map(|_| {
                pool.get_conn()
                    .and_then(|conn| conn.start_transaction(TransactionOptions::new()))
            })
            .collect::<Vec<_>>();

        let fut = ::futures::future::join_all(conns).map(move |mut conns| {
            assert_eq!(pool_clone.inner.lock().unwrap().conn_count(), POOL_MAX);

            while let Some(_) = conns.pop().map(drop) {
                assert_eq!(pool_clone.inner.lock().unwrap().conn_count(), POOL_MAX);
            }
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
                conn1.inner.pool.as_mut().unwrap().handle_futures().unwrap();
                assert_eq!(
                    conn1
                        .inner
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.new.len()),
                    0
                );
                assert_eq!(
                    conn1
                        .inner
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.idle.len()),
                    0
                );
                assert_eq!(
                    conn2
                        .inner
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.queue.len()),
                    0
                );
                new_conn
            })
            .and_then(|conn1| {
                assert_eq!(
                    conn1
                        .inner
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.new.len()),
                    0
                );
                assert_eq!(
                    conn1
                        .inner
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.idle.len()),
                    0
                );
                assert_eq!(
                    conn1
                        .inner
                        .pool
                        .as_ref()
                        .unwrap()
                        .with_inner(|inner| inner.queue.len()),
                    0
                );
                Ok(())
            })
            .and_then(|_| {
                assert_eq!(pool.with_inner(|inner| inner.new.len()), 0);
                assert_eq!(pool.with_inner(|inner| inner.idle.len()), 1);
                assert_eq!(pool.with_inner(|inner| inner.queue.len()), 0);
                pool.disconnect()
            });

        run(fut).unwrap();
    }

    #[test]
    fn should_not_panic_if_dropped_without_tokio_runtime() {
        let pool = Pool::new(&**DATABASE_URL);
        run(collect(
            (0..10).map(|_| pool.get_conn()).collect::<Vec<_>>(),
        ))
        .unwrap();
        // pool will drop here
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use futures::Future;
        use tokio::runtime::Runtime;

        use crate::{conn::pool::Pool, queryable::Queryable, test_misc::DATABASE_URL};

        #[bench]
        fn connect(bencher: &mut test::Bencher) {
            let mut runtime = Runtime::new().expect("3");
            let pool = Pool::new(&**DATABASE_URL);

            bencher.iter(|| {
                let fut = pool.get_conn().and_then(|conn| conn.ping());
                runtime.block_on(fut).expect("1");
            });

            runtime.block_on(pool.disconnect()).unwrap();
        }
    }
}
