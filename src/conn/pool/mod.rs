// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::ready;
use futures_util::future::{ready, FutureExt};
use futures_util::stream::StreamExt;
use tokio::sync::mpsc;
use tokio::timer::Interval;

use std::{
    fmt,
    pin::Pin,
    str::FromStr,
    sync::{atomic, Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use crate::{
    conn::{pool::futures::*, Conn},
    error::*,
    opts::{Opts, PoolOptions},
    queryable::{
        transaction::{Transaction, TransactionOptions},
        Queryable,
    },
};

mod recycler;
// this is a really unfortunate name for a module
pub mod futures;

/// Connection that is idling in the pool.
#[derive(Debug)]
struct IdlingConn {
    /// The connection is idling since this `Instant`.
    since: Instant,
    /// Idling connection.
    conn: Conn,
}

impl IdlingConn {
    /// Returns duration elapsed since this connection is idling.
    fn elapsed(&self) -> Duration {
        self.since.elapsed()
    }
}

impl From<Conn> for IdlingConn {
    fn from(conn: Conn) -> Self {
        Self {
            since: Instant::now(),
            conn,
        }
    }
}

pub struct Inner {
    close: atomic::AtomicBool,
    closed: atomic::AtomicBool,
    idle: crossbeam::queue::ArrayQueue<IdlingConn>,
    wake: crossbeam::queue::SegQueue<Waker>,
    exist: atomic::AtomicUsize,
    extra_wakeups: atomic::AtomicUsize,

    // only used to spawn the recycler the first time we're in async context
    maker: Mutex<Option<(mpsc::UnboundedReceiver<Option<Conn>>, PoolOptions)>>,
}

impl Inner {
    /// This function will spawn the recycler for this pool
    /// as well as the ttl check interval if `inactive_connection_ttl` isn't `0`.
    fn spawn_futures_if_needed(self: Arc<Self>) {
        let mut lock = self.maker.lock().unwrap();
        if let Some((dropped, pool_options)) = lock.take() {
            // Spawn the Recycler.
            tokio::spawn(recycler::Recycler::new(
                pool_options.clone(),
                self.clone(),
                dropped,
            ));

            // Spawn the ttl check interval
            // The purpose of this interval is to remove idling connections that both:
            // * overflows min bound of the pool;
            // * idles longer then `inactive_connection_ttl`.
            if pool_options.inactive_connection_ttl() > Duration::from_secs(0) {
                let inner = self.clone();
                tokio::spawn(
                    Interval::new_interval(pool_options.ttl_check_interval()).for_each(move |_| {
                        let num_idling = inner.idle.len();
                        let mut num_to_drop =
                            num_idling.saturating_sub(pool_options.constraints().min());
                        let mut num_returned = 0;

                        for _ in 0..num_idling {
                            match inner.idle.pop() {
                                Ok(idling_conn) => {
                                    if idling_conn.elapsed()
                                        > pool_options.inactive_connection_ttl()
                                    {
                                        tokio::spawn(idling_conn.conn.disconnect().map(drop));
                                        inner.exist.fetch_sub(1, atomic::Ordering::AcqRel);
                                        if num_to_drop == 1 {
                                            break;
                                        } else {
                                            num_to_drop -= 1;
                                        }
                                    } else {
                                        inner.push_to_idle(idling_conn);
                                        num_returned += 1;
                                    }
                                }
                                Err(_) => break,
                            }
                        }

                        if num_returned > 0 {
                            inner.wake(num_returned);
                        }

                        ready(())
                    }),
                );
            }
        }
    }

    fn push_to_idle<T: Into<IdlingConn>>(&self, conn: T) {
        self.idle
            .push(conn.into())
            .expect("more connections than max")
    }

    fn wake(&self, mut readied: usize) {
        if readied == 0 {
            return;
        }

        while let Ok(task) = self.wake.pop() {
            task.wake();
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
    drop: mpsc::UnboundedSender<Option<Conn>>,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool").field("opts", &self.opts).finish()
    }
}

impl Pool {
    /// Creates new pool of connections.
    pub fn new<O: Into<Opts>>(opts: O) -> Pool {
        let opts = opts.into();
        let pool_options = opts.get_pool_options().clone();
        let (tx, rx) = mpsc::unbounded_channel();
        Pool {
            opts,
            inner: Arc::new(Inner {
                close: false.into(),
                closed: false.into(),
                idle: crossbeam::queue::ArrayQueue::new(pool_options.constraints().max()),
                wake: crossbeam::queue::SegQueue::new(),
                exist: 0.into(),
                extra_wakeups: 0.into(),
                maker: Mutex::new(Some((rx, pool_options))),
            }),
            drop: tx,
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
    pub async fn start_transaction(
        &self,
        options: TransactionOptions,
    ) -> Result<Transaction<Conn>> {
        Queryable::start_transaction(self.get_conn().await?, options).await
    }

    /// Returns future that disconnects this pool from server and resolves to `()`.
    ///
    /// Active connections taken from this pool should be disconnected manually.
    /// Also all pending and new `GetConn`'s will resolve to error.
    pub fn disconnect(mut self) -> DisconnectPool {
        let was_closed = self.inner.close.swap(true, atomic::Ordering::AcqRel);
        if !was_closed {
            // make sure we wake up the Recycler.
            //
            // note the lack of an .expect() here, because the Recycler may decide that there are
            // no connections to wait for and exit quickly!
            let _ = self.drop.try_send(None).is_ok();
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
            && self.inner.idle.len() < self.opts.get_pool_options().active_bound()
        {
            self.inner.push_to_idle(conn);
            self.inner.wake(1);
        } else {
            self.drop
                .try_send(Some(conn))
                .expect("recycler is active as long as any Pool is");
        }
    }

    /// Indicate that a connection failed to be created and release it.
    ///
    /// Decreases the exist counter since a broken or dropped connection should not count towards
    /// the total.
    fn cancel_connection(&self) {
        let prev = self.inner.exist.fetch_sub(1, atomic::Ordering::AcqRel);
        // NB: Wrapping around here would only be due to a programming error.
        assert!(prev > 0, "exist must not wrap around");
    }

    /// Poll the pool for an available connection.
    fn poll_new_conn(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<GetConn>> {
        self.poll_new_conn_inner(false, cx)
    }

    fn poll_new_conn_inner(
        mut self: Pin<&mut Self>,
        retrying: bool,
        cx: &mut Context<'_>,
    ) -> Poll<Result<GetConn>> {
        if self.inner.close.load(atomic::Ordering::Acquire) {
            return Err(Error::Driver(DriverError::PoolDisconnected)).into();
        }

        loop {
            match self.inner.idle.pop() {
                Err(crossbeam::queue::PopError) => break,
                Ok(IdlingConn { conn, .. }) => {
                    if conn.expired() {
                        self.return_conn(conn);
                        continue;
                    }

                    return Poll::Ready(Ok(GetConn {
                        pool: Some(self.clone()),
                        inner: GetConnInner::Done(Some(conn)),
                    }));
                }
            }
        }

        // we didn't _immediately_ get one -- try to make one
        // we first try to just do a load so we don't do an unnecessary add then sub
        let exist = self.inner.exist.load(atomic::Ordering::Acquire);
        if exist < self.opts.get_pool_options().constraints().max() {
            // we may be allowed to make a new one!
            let exist = self.inner.exist.fetch_add(1, atomic::Ordering::AcqRel);
            if exist == 0 {
                // we may have to start the recycler.
                self.inner.clone().spawn_futures_if_needed();
            }

            if exist < self.opts.get_pool_options().constraints().max() {
                // we're allowed to make a new connection

                // note, however, that there is a race here:
                // imagine that Pool::disconnect was _just_ called. that is, after we checked at
                // the start of this method. the Recycler checks .exist right _before_ we increment
                // it, and notices that it is 0, and thus believes it is allowed to exit. if we
                // continue to make a new connection here, that connection would not have a way to
                // be dropped as part of the pool.
                //
                // so, we check .close again here (after the increment). if it is now true, we know
                // that the Recycler may have exited, and we give up. if it is false, we _know_
                // that the Recyler _must_ see our +1 before it decides to exit.
                if self.inner.close.load(atomic::Ordering::Acquire) {
                    self.inner.exist.fetch_sub(1, atomic::Ordering::AcqRel);
                    // make sure we notify the Recycler in case it was waiting for our +1
                    self.drop
                        .try_send(None)
                        .expect("recycler is active as long as any Pool is");
                    return Err(Error::Driver(DriverError::PoolDisconnected)).into();
                }

                return Poll::Ready(Ok(GetConn {
                    pool: Some(self.clone()),
                    inner: GetConnInner::Connecting(Box::pin(Conn::new(self.opts.clone()))),
                }));
            }

            let exist = self.inner.exist.fetch_sub(1, atomic::Ordering::AcqRel);
            if exist < self.opts.get_pool_options().constraints().max() {
                // we'd _now_ be allowed to make a connection
                return self.poll_new_conn_inner(retrying, cx);
            }
        }

        if !retrying {
            // no go -- we have to wait
            self.inner.wake.push(cx.waker().clone());

            // there's a potential race here -- imagine another task releases a connection after we
            // try to poll .idle or check .exist, but before we push our task onto .wake. In that
            // case, we might never be woken up again! so, we need to make those checks again here
            // after we've scheduled ourselves for wakeup.
            //
            // an alternative strategy would be to _always_ push to .wake and then do the checks,
            // but that would lead to a large number of spurious notifications/wakeups, as well as
            // needless contention on .wake.
            let conn = ready!(self.as_mut().poll_new_conn_inner(true, cx))?;

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
                if task.will_wake(cx.waker()) {
                    // phew -- we got out of that one easy!
                    return Poll::Ready(Ok(conn));
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
                task.wake();
            } else {
                // someone tried to notify us, but also, no-one else is waiting,
                // so there's no-one to "forward" that wake-up to.
            }

            return Poll::Ready(Ok(conn));
        }

        Poll::Pending
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
    use futures_util::stream::StreamExt;

    use std::sync::atomic;
    use std::time::Duration;

    use crate::{
        conn::pool::Pool, opts::PoolOptions, queryable::Queryable, test_misc::get_opts,
        PoolConstraints, TransactionOptions,
    };

    #[test]
    fn should_not_hang() -> super::Result<()> {
        pub struct Database {
            pool: Pool,
        }

        impl Database {
            pub async fn disconnect(self) -> super::Result<()> {
                self.pool.disconnect().await?;
                Ok(())
            }
        }

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let database = Database {
            pool: Pool::new(get_opts()),
        };
        runtime.block_on(database.disconnect())
    }

    #[tokio::test]
    async fn should_track_conn_if_disconnected_outside_of_a_pool() -> super::Result<()> {
        let pool = Pool::new(get_opts());
        let conn = pool.get_conn().await?;
        conn.disconnect().await?;
        pool.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn should_connect() -> super::Result<()> {
        let pool = Pool::new(get_opts());
        pool.get_conn().await?.ping().await?;
        pool.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn can_handle_the_pressure() {
        let pool = Pool::new(get_opts());
        for _ in 0..10i32 {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            for i in 0..10_000 {
                let pool = pool.clone();
                let mut tx = tx.clone();
                tokio::spawn(async move {
                    let _ = pool.get_conn().await.unwrap();
                    tx.try_send(i).unwrap();
                });
            }
            drop(tx);
            // see that all the tx's eventually complete
            while let Some(_) = rx.next().await {}
        }
        drop(pool);
    }

    #[tokio::test]
    async fn should_start_transaction() -> super::Result<()> {
        let mut opts = get_opts();
        opts.pool_options(PoolOptions::with_constraints(
            PoolConstraints::new(1, 1).unwrap(),
        ));
        let pool = Pool::new(opts);
        pool.get_conn()
            .await?
            .drop_query("CREATE TABLE IF NOT EXISTS tmp(id int)")
            .await?;
        let _ = pool
            .start_transaction(TransactionOptions::default())
            .await?
            .batch_exec("INSERT INTO tmp (id) VALUES (?)", vec![(1,), (2,)])
            .await?
            .prep_exec("SELECT * FROM tmp", ())
            .await?;
        let row_opt = pool
            .get_conn()
            .await?
            .first("SELECT COUNT(*) FROM tmp")
            .await?
            .1;
        assert_eq!(row_opt, Some((0u8,)));
        pool.get_conn().await?.drop_query("DROP TABLE tmp").await?;
        pool.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn should_hold_bounds2() -> super::Result<()> {
        use std::cmp::min;

        const POOL_MIN: usize = 5;
        const POOL_MAX: usize = 10;
        const INACTIVE_CONNECTION_TTL: Duration = Duration::from_millis(500);
        const TTL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

        let constraints = PoolConstraints::new(POOL_MIN, POOL_MAX).unwrap();
        let pool_options =
            PoolOptions::new(constraints, INACTIVE_CONNECTION_TTL, TTL_CHECK_INTERVAL);

        // Clean
        let mut opts = get_opts();
        opts.pool_options(pool_options);

        let pool = Pool::new(opts);
        let pool_clone = pool.clone();
        let conns = (0..POOL_MAX).map(|_| pool.get_conn()).collect::<Vec<_>>();

        let mut conns = futures_util::try_future::try_join_all(conns).await?;

        // we want to continuously drop connections
        // and check that they are _actually_ dropped until we reach POOL_MIN
        assert_eq!(
            pool_clone.inner.exist.load(atomic::Ordering::SeqCst),
            POOL_MAX
        );

        while !conns.is_empty() {
            // first, drop a connection
            let _ = conns.pop();

            // then, wait for a bit to let the connection be reclaimed
            tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_millis(50))
                .await;

            // now check that we have the expected # of connections
            // this may look a little funky, but think of it this way:
            //
            //  - if we hold all 10 connections, we expect 10
            //  - if we drop one,  we still expect 10, because POOL_MIN limits
            //    the number of _idle_ connections (of which there is only 1)
            //  - once we've dropped 5, there are now 5 idle connections. thus,
            //    if we drop one more, we _now_ expect there to be only 9
            //    connections total (no more connections should be pushed to
            //    idle).
            let dropped = POOL_MAX - conns.len();
            let idle = min(dropped, POOL_MIN);
            let expected = conns.len() + idle;

            if dropped > POOL_MIN {
                // check that connection is still in the pool because of inactive_connection_ttl
                let have = pool_clone.inner.exist.load(atomic::Ordering::SeqCst);
                assert_eq!(have, expected + 1);

                // then, wait for ttl_check_interval
                tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_secs(1))
                    .await;
            }

            // check that we have the expected number of connections
            let have = pool_clone.inner.exist.load(atomic::Ordering::SeqCst);
            assert_eq!(have, expected);
        }

        pool.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn should_hold_bounds1() -> super::Result<()> {
        let mut opts = get_opts();
        opts.pool_options(PoolOptions::with_constraints(
            PoolConstraints::new(1, 2).unwrap(),
        ));
        let pool = Pool::new(opts);
        let pool_clone = pool.clone();

        let (conn1, conn2) = futures_util::try_future::try_join(pool.get_conn(), pool.get_conn())
            .await
            .unwrap();

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

        drop(conn1);
        drop(conn2);
        // only one of conn1 and conn2 should have gone to idle,
        // and should have immediately been picked up by new_conn (now conn1)
        let conn1 = pool_clone.get_conn().await?;
        assert_eq!(conn1.inner.pool.as_ref().unwrap().inner.idle.len(), 0);

        drop(conn1);

        // the connection should be returned to idle
        // (but may not have been returned _yet_)
        assert!(pool.inner.idle.len() <= 1);
        pool.disconnect().await?;
        Ok(())
    }

    // Test that connections which err do not count towards the connection count in the pool.
    #[tokio::test]
    async fn should_hold_bounds_on_error() -> super::Result<()> {
        // Should not be possible to connect to broadcast address.
        let pool = Pool::new(String::from("mysql://255.255.255.255"));

        assert!(
            futures_util::try_future::try_join(pool.get_conn(), pool.get_conn())
                .await
                .is_err()
        );
        assert_eq!(pool.inner.exist.load(atomic::Ordering::SeqCst), 0);
        Ok(())
    }

    /*
    #[test]
    fn should_hold_bounds_on_get_conn_drop() {
        let pool = Pool::new(format!("{}?pool_min=1&pool_max=2", get_opts()));
        let mut runtime = tokio::runtime::Runtime::new().unwrap();

        // This test is a bit more intricate: we need to poll the connection future once to get the
        // pool to set it up, then drop it and make sure that the `exist` count is updated.
        //
        // We wrap all of it in a lazy future to get us into the tokio context that deals with
        // setting up tasks. There might be a better way to do this but I don't remember right
        // now. Besides, std::future is just around the corner making this obsolete.
        //
        // It depends on implementation details of GetConn, but that should be fine.
        runtime
            .block_on(future::lazy(move || {
                let mut conn = pool.get_conn();
                assert_eq!(pool.inner.exist.load(atomic::Ordering::SeqCst), 0);
                let result = conn.poll().expect("successful first poll");
                assert!(result.is_not_ready(), "not ready after first poll");
                assert_eq!(pool.inner.exist.load(atomic::Ordering::SeqCst), 1);
                drop(conn);
                assert_eq!(pool.inner.exist.load(atomic::Ordering::SeqCst), 0);
                Ok::<(), ()>(())
            }))
            .unwrap();
    }
    */

    #[tokio::test]
    async fn droptest() -> super::Result<()> {
        let pool = Pool::new(get_opts());
        let conns = futures_util::try_future::try_join_all((0..10).map(|_| pool.get_conn()))
            .await
            .unwrap();
        drop(conns);
        drop(pool);
        Ok(())
    }

    /*
    #[test]
    #[ignore]
    fn should_not_panic_if_dropped_without_tokio_runtime() {
        // NOTE: this test does not work anymore, since the runtime won't be idle until either
        //
        //  - all Pools and Conns are dropped; OR
        //  - Pool::disconnect is called; OR
        //  - Runtime::shutdown_now is called
        //
        // none of these are true in this test, which is why it's been ignored
        let pool = Pool::new(get_opts());
        run(collect(
            (0..10).map(|_| pool.get_conn()).collect::<Vec<_>>(),
        ))
        .unwrap();
        // pool will drop here
    }
    */

    #[cfg(feature = "nightly")]
    mod bench {
        use futures_util::future::FutureExt;
        use futures_util::try_future::TryFutureExt;
        use tokio::runtime::Runtime;

        use crate::prelude::Queryable;
        use crate::test_misc::get_opts;
        use crate::{Pool, PoolConstraints};

        #[bench]
        fn connect(bencher: &mut test::Bencher) {
            let runtime = Runtime::new().unwrap();
            let pool = Pool::new(get_opts());

            bencher.iter(|| {
                let fut = pool.get_conn().and_then(|conn| conn.ping());
                runtime.block_on(fut).unwrap();
            });

            runtime.block_on(pool.disconnect()).unwrap();
        }

        #[bench]
        fn new_conn_on_pool_soft_boundary(bencher: &mut test::Bencher) {
            let runtime = Runtime::new().unwrap();

            let mut opts = get_opts();
            opts.pool_constraints(PoolConstraints::new(0, 1));

            let pool = Pool::new(opts);

            bencher.iter(|| {
                let fut = pool.get_conn().map(drop);
                runtime.block_on(fut);
            });

            runtime.block_on(pool.disconnect()).unwrap();
        }
    }
}
