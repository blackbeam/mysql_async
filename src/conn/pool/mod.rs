// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use tokio::sync::mpsc;

use std::{
    collections::VecDeque,
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
mod ttl_check_inerval;

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

/// The exchange is where we track all connections as they come and go.
///
/// It is held under a single, non-asynchronous lock.
/// This is fine as long as we never do expensive work while holding the lock!
struct Exchange {
    waiting: VecDeque<Waker>,
    available: VecDeque<IdlingConn>,
    exist: usize,
    // only used to spawn the recycler the first time we're in async context
    recycler: Option<(mpsc::UnboundedReceiver<Option<Conn>>, PoolOptions)>,
}

impl Exchange {
    /// This function will spawn the recycler for this pool
    /// as well as the ttl check interval if `inactive_connection_ttl` isn't `0`.
    fn spawn_futures_if_needed(&mut self, inner: &Arc<Inner>) {
        use recycler::Recycler;
        use ttl_check_inerval::TtlCheckInterval;
        if let Some((dropped, pool_options)) = self.recycler.take() {
            // Spawn the Recycler.
            tokio::spawn(Recycler::new(pool_options.clone(), inner.clone(), dropped));

            // Spawn the ttl check interval if `inactive_connection_ttl` isn't `0`
            if pool_options.inactive_connection_ttl() > Duration::from_secs(0) {
                tokio::spawn(TtlCheckInterval::new(pool_options, inner.clone()));
            }
        }
    }
}

pub struct Inner {
    close: atomic::AtomicBool,
    closed: atomic::AtomicBool,
    exchange: Mutex<Exchange>,
}

#[derive(Clone)]
/// Asynchronous pool of MySql connections.
///
/// Note that you will probably want to await `Pool::disconnect` before dropping the runtime, as
/// otherwise you may end up with a number of connections that are not cleanly terminated.
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
                exchange: Mutex::new(Exchange {
                    available: VecDeque::with_capacity(pool_options.constraints().max()),
                    waiting: VecDeque::new(),
                    exist: 0,
                    recycler: Some((rx, pool_options)),
                }),
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
    pub fn disconnect(self) -> DisconnectPool {
        let was_closed = self.inner.close.swap(true, atomic::Ordering::AcqRel);
        if !was_closed {
            // make sure we wake up the Recycler.
            //
            // note the lack of an .expect() here, because the Recycler may decide that there are
            // no connections to wait for and exit quickly!
            let _ = self.drop.send(None).is_ok();
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
        {
            let mut exchange = self.inner.exchange.lock().unwrap();
            if exchange.available.len() < self.opts.get_pool_options().active_bound() {
                exchange.available.push_back(conn.into());
                if let Some(w) = exchange.waiting.pop_front() {
                    w.wake();
                }
                return;
            }
        }

        if let Err(conn) = self.drop.send(Some(conn)) {
            let conn = conn.0.unwrap();

            // This _probably_ means that the Runtime is shutting down, and that the Recycler was
            // dropped rather than allowed to exit cleanly.
            if !self.inner.closed.load(atomic::Ordering::SeqCst) {
                // Yup, Recycler was forcibly dropped!
                // All we can do here is try the non-pool drop path for Conn.
                assert!(conn.inner.pool.is_none());
                drop(conn);
            } else {
                unreachable!("Recycler exited while connections still exist");
            }
        }
    }

    /// Indicate that a connection failed to be created and release it.
    ///
    /// Decreases the exist counter since a broken or dropped connection should not count towards
    /// the total.
    fn cancel_connection(&self) {
        let mut exchange = self.inner.exchange.lock().unwrap();
        exchange.exist -= 1;
        // we just enabled the creation of a new connection!
        if let Some(w) = exchange.waiting.pop_front() {
            w.wake();
        }
    }

    /// Poll the pool for an available connection.
    fn poll_new_conn(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<GetConn>> {
        self.poll_new_conn_inner(cx)
    }

    fn poll_new_conn_inner(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<GetConn>> {
        let mut exchange = self.inner.exchange.lock().unwrap();

        // NOTE: this load must happen while we hold the lock,
        // otherwise the recycler may choose to exit, see that .exist == 0, and then exit,
        // and then we decide to create a new connection, which would then never be torn down.
        if self.inner.close.load(atomic::Ordering::Acquire) {
            return Err(Error::Driver(DriverError::PoolDisconnected)).into();
        }

        exchange.spawn_futures_if_needed(&self.inner);

        if let Some(IdlingConn { conn, .. }) = exchange.available.pop_back() {
            return Poll::Ready(Ok(GetConn {
                pool: Some(self.clone()),
                inner: GetConnInner::Done(Some(conn)),
            }));
        }

        // we didn't _immediately_ get one -- try to make one
        // we first try to just do a load so we don't do an unnecessary add then sub
        if exchange.exist < self.opts.get_pool_options().constraints().max() {
            // we are allowed to make a new connection, so we will!
            exchange.exist += 1;

            return Poll::Ready(Ok(GetConn {
                pool: Some(self.clone()),
                inner: GetConnInner::Connecting(Box::pin(Conn::new(self.opts.clone()))),
            }));
        }

        // no go -- we have to wait
        exchange.waiting.push_back(cx.waker().clone());
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
    use futures_util::{future::try_join_all, stream::StreamExt, try_join};

    use std::time::Duration;

    use crate::{
        conn::pool::Pool, opts::PoolOptions, queryable::Queryable, test_misc::get_opts,
        PoolConstraints, TransactionOptions,
    };

    macro_rules! conn_ex_field {
        ($conn:expr, $field:tt) => {
            ex_field!($conn.inner.pool.as_ref().unwrap(), $field)
        };
    }

    macro_rules! ex_field {
        ($pool:expr, $field:tt) => {
            $pool.inner.exchange.lock().unwrap().$field
        };
    }

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

        let mut runtime = tokio::runtime::Runtime::new().unwrap();
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
                let tx = tx.clone();
                tokio::spawn(async move {
                    let _ = pool.get_conn().await.unwrap();
                    tx.send(i).unwrap();
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

        let mut conns = try_join_all(conns).await?;

        // we want to continuously drop connections
        // and check that they are _actually_ dropped until we reach POOL_MIN
        assert_eq!(ex_field!(pool_clone, exist), POOL_MAX);

        while !conns.is_empty() {
            // first, drop a connection
            let _ = conns.pop();

            // then, wait for a bit to let the connection be reclaimed
            tokio::time::delay_for(std::time::Duration::from_millis(50)).await;

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
                let have = ex_field!(pool_clone, exist);
                assert_eq!(have, expected + 1);

                // then, wait for ttl_check_interval
                tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
            }

            // check that we have the expected number of connections
            let have = ex_field!(pool_clone, exist);
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

        let (conn1, conn2) = try_join!(pool.get_conn(), pool.get_conn()).unwrap();

        assert_eq!(conn_ex_field!(conn1, exist), 2);
        assert_eq!(conn_ex_field!(conn1, available).len(), 0);

        drop(conn1);
        drop(conn2);
        // only one of conn1 and conn2 should have gone to idle,
        // and should have immediately been picked up by new_conn (now conn1)
        let conn1 = pool_clone.get_conn().await?;
        assert_eq!(conn_ex_field!(conn1, available).len(), 0);

        drop(conn1);

        // the connection should be returned to idle
        // (but may not have been returned _yet_)
        assert!(ex_field!(pool, available).len() <= 1);
        pool.disconnect().await?;
        Ok(())
    }

    // Test that connections which err do not count towards the connection count in the pool.
    #[tokio::test]
    async fn should_hold_bounds_on_error() -> super::Result<()> {
        // Should not be possible to connect to broadcast address.
        let pool = Pool::new(String::from("mysql://255.255.255.255"));

        assert!(try_join!(pool.get_conn(), pool.get_conn()).is_err());
        assert_eq!(ex_field!(pool, exist), 0);
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
        let conns = try_join_all((0..10).map(|_| pool.get_conn()))
            .await
            .unwrap();
        drop(conns);
        drop(pool);
        Ok(())
    }

    #[test]
    fn drop_impl_for_conn_should_not_panic_within_unwind() {
        use tokio::runtime;

        const PANIC_MESSAGE: &str = "ORIGINAL_PANIC";

        let result = std::panic::catch_unwind(|| {
            runtime::Builder::new()
                .basic_scheduler()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let pool = Pool::new(get_opts());
                    let _conn = pool.get_conn().await.unwrap();
                    panic!(PANIC_MESSAGE);
                });
        });

        assert_eq!(
            *result.unwrap_err().downcast::<&str>().unwrap(),
            PANIC_MESSAGE,
        );
    }

    #[test]
    fn should_not_panic_on_unclean_shutdown() {
        // run more than once to trigger different drop orders
        for _ in 0..10 {
            let mut rt = tokio::runtime::Runtime::new().unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel();
            rt.block_on(async move {
                let pool = Pool::new(get_opts());
                let c = pool.get_conn().await.unwrap();
                tokio::spawn(async move {
                    let _ = rx.await;
                    let _ = c.drop_query("SELECT 1").await;
                });
            });
            drop(rt);
            // c is still active here, so if anything it's been forcibly dropped
            let _ = tx.send(());
        }
    }

    #[test]
    fn should_perform_clean_shutdown() {
        // run more than once to trigger different drop orders
        for _ in 0..10 {
            let mut rt = tokio::runtime::Runtime::new().unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let jh = rt.spawn(async move {
                let pool = Pool::new(get_opts());
                let c = pool.get_conn().await.unwrap();
                tokio::spawn(async move {
                    let _ = rx.await;
                    let _ = c.drop_query("SELECT 1").await;
                });
                let _ = pool.disconnect().await;
            });
            let _ = tx.send(());
            rt.block_on(jh).unwrap();
        }
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
        use futures_util::{future::FutureExt, try_future::TryFutureExt};
        use tokio::runtime::Runtime;

        use crate::{prelude::Queryable, test_misc::get_opts, Pool, PoolConstraints, PoolOptions};
        use std::time::Duration;

        #[bench]
        fn connect(bencher: &mut test::Bencher) {
            let mut runtime = Runtime::new().unwrap();
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
            let mut pool_opts = PoolOptions::with_constraints(PoolConstraints::new(0, 1).unwrap());
            pool_opts.set_inactive_connection_ttl(Duration::from_secs(1));
            opts.pool_options(pool_opts);

            let pool = Pool::new(opts);

            bencher.iter(|| {
                let fut = pool.get_conn().map(drop);
                runtime.block_on(fut);
            });

            runtime.block_on(pool.disconnect()).unwrap();
        }
    }
}
