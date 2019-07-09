// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use ::futures::stream::futures_unordered::FuturesUnordered;
use ::futures::{
    task::{self, Task},
    try_ready, Async, Future, Poll, Stream,
};

use tokio_sync::mpsc;

use std::{
    fmt,
    str::FromStr,
    sync::{atomic, Arc, Mutex},
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

// this is a really unfortunate name for a module
pub mod futures;

struct Recycler {
    inner: Arc<Inner>,
    discard: FuturesUnordered<BoxFuture<()>>,
    discarded: usize,
    cleaning: FuturesUnordered<BoxFuture<Conn>>,
    dropped: mpsc::UnboundedReceiver<Conn>,
    min: usize,
    exiting: bool,
}

impl Future for Recycler {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut readied = 0;

        macro_rules! conn_decision {
            ($self:ident, $readied:ident, $conn:ident) => {
                if $conn.inner.stream.is_none() || $conn.inner.disconnected {
                    // drop unestablished connection
                    $self.discard.push(Box::new(::futures::future::ok(())));
                } else if $conn.inner.in_transaction || $conn.inner.has_result.is_some() {
                    $self.cleaning.push($conn.cleanup());
                } else if $conn.expired()
                    || $self.inner.idle.len() >= $self.min
                    || $self.inner.close.load(atomic::Ordering::Acquire)
                {
                    $self.discard.push(Box::new($conn.close()));
                } else {
                    $self
                        .inner
                        .idle
                        .push($conn)
                        .expect("more connections than max");
                    $readied += 1;
                }
            };
        }

        while !self.exiting {
            // see if there are more connections for us to recycle
            match self.dropped.poll().unwrap() {
                Async::Ready(Some(conn)) => {
                    conn_decision!(self, readied, conn);
                }
                Async::Ready(None) => {
                    // no more connections are coming -- time to exit!
                    self.exiting = true;
                }
                Async::NotReady => {
                    // nope -- but let's still make progress on the ones we have
                    break;
                }
            }
        }

        // if we're exiting, reclaim any idle connections
        if self.exiting {
            while let Ok(conn) = self.inner.idle.pop() {
                conn_decision!(self, readied, conn);
            }
        }

        // are any dirty connections ready for us to reclaim?
        loop {
            match self.cleaning.poll() {
                Ok(Async::NotReady) | Ok(Async::Ready(None)) => break,
                Ok(Async::Ready(Some(conn))) => conn_decision!(self, readied, conn),
                Err(e) => {
                    // an error occurred while cleaning a connection.
                    // what do we do? replace it with a new connection?
                    self.discarded += 1;
                    // NOTE: we're discarding the error here
                    let _ = e;
                }
            }
        }

        // are there any torn-down connections for us to deal with?
        loop {
            match self.discard.poll() {
                Ok(Async::NotReady) | Ok(Async::Ready(None)) => break,
                Ok(Async::Ready(Some(()))) => {
                    // yes! count it.
                    self.discarded += 1
                }
                Err(e) => {
                    // an error occurred while closing a connection.
                    // what do we do? we still replace it with a new connection..
                    self.discarded += 1;
                    // NOTE: we're discarding the error here
                    let _ = e;
                }
            }
        }

        if self.discarded != 0 {
            // we need to open up slots for new connctions to be established!
            self.inner
                .exist
                .fetch_sub(self.discarded, atomic::Ordering::AcqRel);
            readied += self.discarded;
            self.discarded = 0;
        }

        if self.exiting && self.cleaning.is_empty() && self.discard.is_empty() {
            // we know that all Pool handles have been dropped (self.dropped.poll returned None).

            // if this assertion fails, where are the remaining connections?
            assert_eq!(self.inner.exist.load(atomic::Ordering::Acquire), 0);

            // NOTE: it is _necessary_ that we set this _before_ we call .wake
            // otherwise, the following may happen to the DisconnectPool future:
            //
            //  - We wake all in .wake
            //  - DisconnectPool::poll adds to .wake
            //  - DisconnectPool::poll reads .closed == false
            //  - We set .closed = true
            //
            // At this point, DisconnectPool::poll will never be notified again.
            self.inner.closed.store(true, atomic::Ordering::AcqRel);
        }

        self.inner.wake(readied);

        if self.inner.closed.load(atomic::Ordering::Acquire) {
            // since there are no more Pools, we also know that no-one is waiting anymore,
            // so we don't have to worry about calling wake more times
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

struct Inner {
    close: atomic::AtomicBool,
    closed: atomic::AtomicBool,
    idle: crossbeam::queue::ArrayQueue<Conn>,
    wake: crossbeam::queue::SegQueue<Task>,
    exist: atomic::AtomicUsize,
    extra_wakeups: atomic::AtomicUsize,

    // only used to spawn the recycler the first time we're in async context
    maker: Mutex<Option<mpsc::UnboundedReceiver<Conn>>>,
}

impl Inner {
    fn wake(&self, mut readied: usize) {
        while let Ok(task) = self.wake.pop() {
            task.notify();
            readied -= 1;
            if readied == 0 {
                if self.close.load(atomic::Ordering::Acquire) {
                    // wake up as many as we can -- they should all error
                    readied = usize::max_value();
                    continue;
                }

                // no point in waking up more, since we don't have anything for them
                // there _may_ be some tasks that weren't _really_ waiting though, and we need to
                // make sure that those notifications go to someone who cares about them.
                let extra = self.extra_wakeups.swap(0, atomic::Ordering::AcqRel);
                if extra == 0 {
                    break;
                }

                // one thing is worth noting here -- if there aren't enough waiting tasks in .wake
                // to account for the value in extra, that is _okay_. those extra tasks we "would
                // have" notified will instead see that they can proceed directly when they call
                // .poll_new_conn(), or alternatively will be woken up directly by the place that
                // increments .extra_wakeups in the first place
                readied = extra;
            }
        }
    }
}

#[derive(Clone)]
/// Asynchronous pool of MySql connections.
pub struct Pool {
    opts: Opts,
    inner: Arc<Inner>,
    pool_constraints: PoolConstraints,
    drop: mpsc::UnboundedSender<Conn>,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("opts", &self.opts)
            .field("pool_constraints", &self.pool_constraints)
            .finish()
    }
}

impl Pool {
    /// Creates new pool of connections.
    pub fn new<O: Into<Opts>>(opts: O) -> Pool {
        let opts = opts.into();
        let pool_constraints = opts.get_pool_constraints().clone();
        let (tx, rx) = mpsc::unbounded_channel();
        Pool {
            opts,
            inner: Arc::new(Inner {
                close: false.into(),
                closed: false.into(),
                idle: crossbeam::queue::ArrayQueue::new(pool_constraints.max()),
                wake: crossbeam::queue::SegQueue::new(),
                exist: 0.into(),
                extra_wakeups: 0.into(),
                maker: Mutex::new(Some(rx)),
            }),
            drop: tx,
            pool_constraints,
        }
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
        let was_closed = self.inner.close.swap(true, atomic::Ordering::AcqRel);
        if !was_closed {
            // eagerly purge active connections
            while let Ok(conn) = self.inner.idle.pop() {
                self.drop
                    .try_send(conn)
                    .expect("recycler is active as long as any Pool is");
            }
        }
        new_disconnect_pool(self)
    }

    /// A way to return connection taken from a pool.
    fn return_conn(&mut self, conn: Conn) {
        // NOTE: we're not in async context here, so we can't block or return NotReady
        // any and all cleanup work _has_ to be done in the spawned recycler

        // fast-path for when the connection is immediately ready to be reused
        if conn.inner.stream.is_some()
            && !conn.inner.disconnected
            && !conn.expired()
            && !conn.inner.in_transaction
            && conn.inner.has_result.is_none()
            && !self.inner.close.load(atomic::Ordering::Acquire)
            && self.inner.idle.len() < self.pool_constraints.min()
        {
            self.inner
                .idle
                .push(conn)
                .expect("more connections than max");
            self.inner.wake(1);
        } else {
            self.drop
                .try_send(conn)
                .expect("recycler is active as long as any Pool is");
        }
    }

    /// Poll the pool for an available connection.
    fn poll_new_conn(&mut self) -> Result<Async<GetConn>> {
        self.poll_new_conn_inner(false)
    }

    fn poll_new_conn_inner(&mut self, retrying: bool) -> Result<Async<GetConn>> {
        if self.inner.close.load(atomic::Ordering::AcqRel) {
            return Err(Error::Driver(DriverError::PoolDisconnected));
        }

        loop {
            match self.inner.idle.pop() {
                Err(crossbeam::queue::PopError) => break,
                Ok(conn) => {
                    if conn.expired() {
                        self.return_conn(conn);
                        continue;
                    }

                    return Ok(Async::Ready(GetConn {
                        inner: GetConnInner::Done(Some(conn)),
                    }));
                }
            }
        }

        // we didn't _immediately_ get one -- try to make one
        // we first try to just do a load so we don't do an unnecessary add then sub
        let exist = self.inner.exist.load(atomic::Ordering::Acquire);
        if exist < self.pool_constraints.max() {
            // we may be allowed to make a new one!
            let exist = self.inner.exist.fetch_add(1, atomic::Ordering::AcqRel);
            if exist == 0 {
                // we may have to start the recycler.
                let mut lock = self.inner.maker.lock().unwrap();
                if let Some(dropped) = lock.take() {
                    // we're the first connection!
                    tokio::spawn(Recycler {
                        inner: self.inner.clone(),
                        discard: FuturesUnordered::new(),
                        discarded: 0,
                        cleaning: FuturesUnordered::new(),
                        dropped,
                        min: self.pool_constraints.min(),
                        exiting: false,
                    });
                }
            }

            if exist < self.pool_constraints.max() {
                // we're allowed to make a new connection
                return Ok(Async::Ready(GetConn {
                    inner: GetConnInner::Connecting(Box::new(Conn::new(self.opts.clone()))),
                }));
            }

            let exist = self.inner.exist.fetch_sub(1, atomic::Ordering::AcqRel);
            if exist < self.pool_constraints.max() {
                // we'd _now_ be allowed to make a connection
                return self.poll_new_conn_inner(retrying);
            }
        }

        if !retrying {
            // no go -- we have to wait
            self.inner.wake.push(task::current());

            // there's a potential race here -- imagine another task releases a connection after we
            // try to poll .idle or check .exist, but before we push our task onto .wake. In that
            // case, we might never be woken up again! so, we need to make those checks again here
            // after we've scheduled ourselves for wakeup.
            //
            // an alternative strategy would be to _always_ push to .wake and then do the checks,
            // but that would lead to a large number of spurious notifications/wakeups, as well as
            // needless contention on .wake.
            let conn = try_ready!(self.poll_new_conn_inner(true));

            // this is a tricky case. we already registered ourselves as wanting to be woken up,
            // but we now have a connection, so we won't be waiting. this means that _if_ we were
            // to be woken up, that notification _really_ should have gone to some _other_ task,
            // which now _won't_ be woken up.
            //
            // thew way we're going to fix that is to deal with both possible cases:
            //
            //  - someone _will_ try to wake us up
            //  - someone has _already_ tried to wake us up
            //
            // we do this by requesting an "extra" wakeup next time someone is waking people up,
            // and also waking someone up (perhaps spuriously) in case we have already been
            // notified.
            if let Ok(task) = self.inner.wake.pop() {
                if task.will_notify_current() {
                    // phew -- we got out of that one easy!
                    return Ok(Async::Ready(conn));
                }

                // if we _haven't_ been notified yet, someone else may be deciding who to wake up
                // _right now_. if they choose us, that's wasted. so, let's make sure they wake up
                // at least one other task.
                self.inner
                    .extra_wakeups
                    .fetch_add(1, atomic::Ordering::AcqRel);

                // if someone has not yet notified us, the +1 above will make sure that they wake
                // up at least one task that's not us. that candidate set has to include the task
                // we just pulled off the queue.
                self.inner.wake.push(task.clone());

                // if someone _did_ already choose to notify us, we want to pass that on.
                // but we also need to notify the task we took for a more subtle reason.
                // consider this task0, and two other tasks, task1 and task2:
                //
                //  - task1 pushed to wake queue
                //  - task0 pushed to wake queue
                //  - task0 pops task1 from wake queue
                //  - task0 increments extra_wakeups
                //  - task2 tries to do a wakeup -- wakes only task0 (task1 not on the queue yet)
                //  - task0 pushes task1 onto the queue
                //
                // in this case, task1 might never be awoken again, which is not okay.
                // hence:
                task.notify();
            } else {
                // someone tried to notify us, but also, no-one else is waiting,
                // so there's no-one to "forward" that wake-up to.
            }

            return Ok(Async::Ready(conn));
        }

        Ok(Async::NotReady)
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
    use std::sync::atomic;

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
    fn can_handle_the_pressure() {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let pool = Pool::new(&**DATABASE_URL);
        for _ in 0..10 {
            use futures::{Sink, Stream};
            let (tx, rx) = futures::sync::mpsc::unbounded();
            for i in 0..10_000 {
                let pool = pool.clone();
                let tx = tx.clone();
                runtime.spawn(futures::future::lazy(move || {
                    pool.get_conn()
                        .map_err(|e| unreachable!("{:?}", e))
                        .and_then(move |_| tx.send(i).map_err(|e| unreachable!("{:?}", e)))
                        .map(|_| ())
                }));
            }
            drop(tx);
            runtime.block_on(rx.fold(0, |_, _i| Ok(0))).unwrap();
        }
        drop(pool);
        runtime.shutdown_on_idle().wait().unwrap();
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
                assert_eq!(
                    pool_clone.inner.exist.load(atomic::Ordering::SeqCst),
                    POOL_MAX
                );

                while let Some(_) = conns.pop().map(drop) {
                    popped += 1;
                    assert_eq!(
                        pool_clone.inner.exist.load(atomic::Ordering::SeqCst),
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
            assert_eq!(
                pool_clone.inner.exist.load(atomic::Ordering::SeqCst),
                POOL_MAX
            );

            while let Some(_) = conns.pop().map(drop) {
                assert_eq!(
                    pool_clone.inner.exist.load(atomic::Ordering::SeqCst),
                    POOL_MAX
                );
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
            .and_then(move |(conn1, _conn2)| {
                let new_conn = pool_clone.get_conn();
                assert_eq!(
                    conn1
                        .inner
                        .pool
                        .as_ref()
                        .unwrap()
                        .inner
                        .exist
                        .load(atomic::Ordering::SeqCst),
                    2
                );
                assert_eq!(conn1.inner.pool.as_ref().unwrap().inner.idle.len(), 0);
                // NOTE: conn1 and conn2 are both dropped here
                new_conn
            })
            .and_then(|conn1| {
                // only one of conn1 and conn2 should have gone to idle,
                // and should have immediately been picked up by new_conn (now conn1)
                assert_eq!(conn1.inner.pool.as_ref().unwrap().inner.idle.len(), 0);
                // NOTE: new_conn (now conn1) is dropped here
                Ok(())
            })
            .and_then(|_| {
                // the connection should be returned to idle
                // (but may not have been returned _yet_)
                assert!(pool.inner.idle.len() <= 1);
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
