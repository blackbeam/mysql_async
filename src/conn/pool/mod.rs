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
    opts::{Opts, PoolOpts},
    queryable::transaction::TxStatus,
    BoxFuture,
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
#[derive(Debug)]
struct Exchange {
    waiting: VecDeque<Waker>,
    available: VecDeque<IdlingConn>,
    exist: usize,
    // only used to spawn the recycler the first time we're in async context
    recycler: Option<(mpsc::UnboundedReceiver<Option<Conn>>, PoolOpts)>,
}

impl Exchange {
    /// This function will spawn the recycler for this pool
    /// as well as the ttl check interval if `inactive_connection_ttl` isn't `0`.
    fn spawn_futures_if_needed(&mut self, inner: &Arc<Inner>) {
        use recycler::Recycler;
        use ttl_check_inerval::TtlCheckInterval;
        if let Some((dropped, pool_opts)) = self.recycler.take() {
            // Spawn the Recycler.
            tokio::spawn(Recycler::new(pool_opts.clone(), inner.clone(), dropped));

            // Spawn the ttl check interval if `inactive_connection_ttl` isn't `0`
            if pool_opts.inactive_connection_ttl() > Duration::from_secs(0) {
                tokio::spawn(TtlCheckInterval::new(pool_opts, inner.clone()));
            }
        }
    }
}

#[derive(Debug)]
pub struct Inner {
    close: atomic::AtomicBool,
    closed: atomic::AtomicBool,
    exchange: Mutex<Exchange>,
}

#[derive(Clone)]
/// Asynchronous pool of MySql connections.
///
/// Note that you will probably want to await [`Pool::disconnect`] before dropping the runtime, as
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
    /// Creates a new pool of connections.
    pub fn new<O: Into<Opts>>(opts: O) -> Pool {
        let opts = opts.into();
        let pool_opts = opts.pool_opts().clone();
        let (tx, rx) = mpsc::unbounded_channel();
        Pool {
            opts,
            inner: Arc::new(Inner {
                close: false.into(),
                closed: false.into(),
                exchange: Mutex::new(Exchange {
                    available: VecDeque::with_capacity(pool_opts.constraints().max()),
                    waiting: VecDeque::new(),
                    exist: 0,
                    recycler: Some((rx, pool_opts)),
                }),
            }),
            drop: tx,
        }
    }

    /// Creates a new pool of connections.
    pub fn from_url<T: AsRef<str>>(url: T) -> Result<Pool> {
        let opts = Opts::from_str(url.as_ref())?;
        Ok(Pool::new(opts))
    }

    /// Returns a future that resolves to [`Conn`].
    pub fn get_conn(&self) -> GetConn {
        GetConn::new(self)
    }

    /// Returns a future that disconnects this pool from the server and resolves to `()`.
    ///
    /// **Note:** This Future won't resolve until all active connections, taken from it,
    /// are dropped or disonnected. Also all pending and new `GetConn`'s will resolve to error.
    pub fn disconnect(self) -> DisconnectPool {
        let was_closed = self.inner.close.swap(true, atomic::Ordering::AcqRel);
        if !was_closed {
            // make sure we wake up the Recycler.
            //
            // note the lack of an .expect() here, because the Recycler may decide that there are
            // no connections to wait for and exit quickly!
            let _ = self.drop.send(None).is_ok();
        }

        DisconnectPool::new(self)
    }

    /// A way to return connection taken from a pool.
    fn return_conn(&mut self, conn: Conn) {
        // NOTE: we're not in async context here, so we can't block or return NotReady
        // any and all cleanup work _has_ to be done in the spawned recycler

        // fast-path for when the connection is immediately ready to be reused
        if conn.inner.stream.is_some()
            && !conn.inner.disconnected
            && !conn.expired()
            && conn.inner.tx_status == TxStatus::None
            && conn.inner.has_result.is_none()
            && !self.inner.close.load(atomic::Ordering::Acquire)
        {
            let mut exchange = self.inner.exchange.lock().unwrap();
            if exchange.available.len() < self.opts.pool_opts().active_bound() {
                exchange.available.push_back(conn.into());
                if let Some(w) = exchange.waiting.pop_front() {
                    w.wake();
                }
                return;
            }
        }

        self.send_to_recycler(conn);
    }

    fn send_to_recycler(&self, conn: Conn) {
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

        loop {
            if let Some(IdlingConn { mut conn, .. }) = exchange.available.pop_back() {
                if !conn.expired() {
                    return Poll::Ready(Ok(GetConn {
                        pool: Some(self.clone()),
                        inner: GetConnInner::Checking(BoxFuture(Box::pin(async move {
                            conn.stream_mut().check().await?;
                            Ok(conn)
                        }))),
                    }));
                } else {
                    self.send_to_recycler(conn);
                }
            } else {
                break;
            }
        }

        // we didn't _immediately_ get one -- try to make one
        // we first try to just do a load so we don't do an unnecessary add then sub
        if exchange.exist < self.opts.pool_opts().constraints().max() {
            // we are allowed to make a new connection, so we will!
            exchange.exist += 1;

            return Poll::Ready(Ok(GetConn {
                pool: Some(self.clone()),
                inner: GetConnInner::Connecting(BoxFuture(Box::pin(Conn::new(self.opts.clone())))),
            }));
        }

        // no go -- we have to wait
        exchange.waiting.push_back(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }

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
        conn::pool::Pool, opts::PoolOpts, queryable::Queryable, test_misc::get_opts,
        PoolConstraints, TxOpts,
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
    async fn should_reconnect() -> super::Result<()> {
        let mut master = crate::Conn::new(get_opts()).await?;

        async fn test(master: &mut crate::Conn, opts: crate::OptsBuilder) -> super::Result<()> {
            const NUM_CONNS: usize = 5;
            let pool = Pool::new(opts);

            // create some conns..
            let connections = (0..NUM_CONNS).map(|_| {
                crate::BoxFuture(Box::pin(async {
                    let mut conn = pool.get_conn().await?;
                    conn.ping().await?;
                    Ok(conn)
                }))
            });

            // collect ids..
            let ids = try_join_all(connections)
                .await?
                .into_iter()
                .map(|conn| (conn.id(),))
                .collect::<Vec<_>>();

            // get_conn should work if connection is available and alive
            pool.get_conn().await?;

            // now we'll kill connections..
            master.exec_batch("KILL ?", ids).await?;

            // now check, that they're still in the pool..
            assert_eq!(ex_field!(pool, available).len(), NUM_CONNS);

            tokio::time::delay_for(std::time::Duration::from_millis(500)).await;

            // now get new connection..
            let _conn = pool.get_conn().await?;

            // now check, that broken connections are dropped
            assert_eq!(ex_field!(pool, available).len(), 0);

            drop(_conn);
            pool.disconnect().await
        }

        println!("Check socket/pipe..");
        test(&mut master, get_opts()).await?;

        println!("Check tcp..");
        test(&mut master, get_opts().prefer_socket(false)).await?;

        master.disconnect().await
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
        let constraints = PoolConstraints::new(1, 1).unwrap();
        let opts = get_opts().pool_opts(PoolOpts::default().with_constraints(constraints));
        let pool = Pool::new(opts);
        pool.get_conn()
            .await?
            .query_drop("CREATE TABLE IF NOT EXISTS tmp(id int)")
            .await?;
        let mut conn = pool.get_conn().await?;
        let mut tx = conn.start_transaction(TxOpts::default()).await?;
        tx.exec_batch("INSERT INTO tmp (id) VALUES (?)", vec![(1_u8,), (2_u8,)])
            .await?;
        tx.exec_drop("SELECT * FROM tmp", ()).await?;
        drop(tx);
        drop(conn);
        let row_opt = pool
            .get_conn()
            .await?
            .query_first("SELECT COUNT(*) FROM tmp")
            .await?;
        assert_eq!(row_opt, Some((0u8,)));
        pool.get_conn().await?.query_drop("DROP TABLE tmp").await?;
        pool.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn should_check_inactive_connection_ttl() -> super::Result<()> {
        const POOL_MIN: usize = 5;
        const POOL_MAX: usize = 10;

        const INACTIVE_CONNECTION_TTL: Duration = Duration::from_millis(500);
        const TTL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

        let constraints = PoolConstraints::new(POOL_MIN, POOL_MAX).unwrap();
        let pool_opts = PoolOpts::default()
            .with_constraints(constraints)
            .with_inactive_connection_ttl(INACTIVE_CONNECTION_TTL)
            .with_ttl_check_interval(TTL_CHECK_INTERVAL);

        let pool = Pool::new(get_opts().pool_opts(pool_opts));
        let pool_clone = pool.clone();
        let conns = (0..POOL_MAX).map(|_| pool.get_conn()).collect::<Vec<_>>();

        let conns = try_join_all(conns).await?;

        assert_eq!(ex_field!(pool_clone, exist), POOL_MAX);
        drop(conns);

        // wait for a bit to let the connections be reclaimed
        tokio::time::delay_for(std::time::Duration::from_millis(100)).await;

        // check that connections are still in the pool because of inactive_connection_ttl
        assert_eq!(ex_field!(pool_clone, available).len(), POOL_MAX);

        // then, wait for ttl_check_interval
        tokio::time::delay_for(TTL_CHECK_INTERVAL).await;

        // check that we have the expected number of connections
        assert_eq!(ex_field!(pool_clone, available).len(), POOL_MIN);

        Ok(())
    }

    #[tokio::test]
    async fn aa_should_hold_bounds2() -> super::Result<()> {
        use std::cmp::min;

        const POOL_MIN: usize = 5;
        const POOL_MAX: usize = 10;

        let constraints = PoolConstraints::new(POOL_MIN, POOL_MAX).unwrap();
        let pool_opts = PoolOpts::default().with_constraints(constraints);

        let pool = Pool::new(get_opts().pool_opts(pool_opts));
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

            // check that we have the expected number of connections
            let have = ex_field!(pool_clone, exist);
            assert_eq!(have, expected);
        }

        pool.disconnect().await?;
        Ok(())
    }

    #[tokio::test]
    async fn should_hold_bounds1() -> super::Result<()> {
        let constraints = PoolConstraints::new(1, 2).unwrap();
        let opts = get_opts().pool_opts(PoolOpts::default().with_constraints(constraints));
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

    #[tokio::test]
    async fn zz_should_check_wait_timeout_on_get_conn() -> super::Result<()> {
        let pool = Pool::new(get_opts());

        let mut conn = pool.get_conn().await?;
        let wait_timeout_orig: Option<usize> = conn.query_first("SELECT @@wait_timeout").await?;
        conn.query_drop("SET GLOBAL wait_timeout = 3").await?;
        conn.disconnect().await?;

        let mut conn = pool.get_conn().await?;
        let wait_timeout: Option<usize> = conn.query_first("SELECT @@wait_timeout").await?;
        let id1: Option<usize> = conn.query_first("SELECT CONNECTION_ID()").await?;
        drop(conn);

        assert_eq!(wait_timeout, Some(3));
        assert_eq!(ex_field!(pool, exist), 1);

        tokio::time::delay_for(std::time::Duration::from_secs(6)).await;

        let mut conn = pool.get_conn().await?;
        let id2: Option<usize> = conn.query_first("SELECT CONNECTION_ID()").await?;
        assert_eq!(ex_field!(pool, exist), 1);
        assert_ne!(id1, id2);

        conn.exec_drop("SET GLOBAL wait_timeout = ?", (wait_timeout_orig,))
            .await?;
        drop(conn);

        pool.disconnect().await?;

        Ok(())
    }

    #[tokio::test]
    async fn droptest() -> super::Result<()> {
        let pool = Pool::new(get_opts());
        let conns = try_join_all((0..10).map(|_| pool.get_conn()))
            .await
            .unwrap();
        drop(conns);
        drop(pool);

        let pool = Pool::new(get_opts());
        let conns = try_join_all((0..10).map(|_| pool.get_conn()))
            .await
            .unwrap();
        drop(pool);
        drop(conns);
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
                let mut c = pool.get_conn().await.unwrap();
                tokio::spawn(async move {
                    let _ = rx.await;
                    let _ = c.query_drop("SELECT 1").await;
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
                let mut c = pool.get_conn().await.unwrap();
                tokio::spawn(async move {
                    let _ = rx.await;
                    let _ = c.query_drop("SELECT 1").await;
                });
                let _ = pool.disconnect().await;
            });
            let _ = tx.send(());
            rt.block_on(jh).unwrap();
        }
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use futures_util::future::{FutureExt, TryFutureExt};
        use tokio::runtime::Runtime;

        use crate::{prelude::Queryable, test_misc::get_opts, Pool, PoolConstraints, PoolOpts};
        use std::time::Duration;

        #[bench]
        fn get_conn(bencher: &mut test::Bencher) {
            let mut runtime = Runtime::new().unwrap();
            let pool = Pool::new(get_opts());

            bencher.iter(|| {
                let fut = pool
                    .get_conn()
                    .and_then(|mut conn| async { conn.ping().await.map(|_| conn) });
                runtime.block_on(fut).unwrap();
            });

            runtime.block_on(pool.disconnect()).unwrap();
        }

        #[bench]
        fn new_conn_on_pool_soft_boundary(bencher: &mut test::Bencher) {
            let mut runtime = Runtime::new().unwrap();

            let mut opts = get_opts();
            let mut pool_opts = PoolOpts::with_constraints(PoolConstraints::new(0, 1).unwrap());
            pool_opts.set_inactive_connection_ttl(Duration::from_secs(1));
            opts.pool_opts(pool_opts);

            let pool = Pool::new(opts);

            bencher.iter(|| {
                let fut = pool.get_conn().map(drop);
                runtime.block_on(fut);
            });

            runtime.block_on(pool.disconnect()).unwrap();
        }
    }
}
