// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_util::FutureExt;
use tokio::sync::mpsc;

use crate::{
    conn::{pool::futures::*, Conn},
    error::*,
    opts::{Opts, PoolOpts},
    queryable::transaction::{Transaction, TxOpts},
};
use std::sync::atomic::Ordering;
use std::{
    future::Future,
    str::FromStr,
    sync::{atomic, Arc, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use in_pool_connections::InPoolConnections;
pub use metrics::Metrics;
use waitlist::{QueueId, Waitlist};

mod recycler;
// this is a really unfortunate name for a module
pub mod futures;
mod in_pool_connections;
mod metrics;
mod ttl_check_inerval;
mod waitlist;

/// Connection that is idling in the pool.
#[derive(Debug)]
struct IdlingConn {
    /// The connection is idling since this `Instant`.
    since: Instant,
    /// Idling connection.
    conn: Conn,
}

impl IdlingConn {
    /// Returns true when this connection has a TTL and it elapsed.
    fn expired(&self) -> bool {
        self.conn
            .inner
            .ttl_deadline
            .map(|t| Instant::now() > t)
            .unwrap_or_default()
    }

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
    waiting: Waitlist,
    available: in_pool_connections::InPoolConnections,
    exist: usize,
    // Wakes the recycler when an external slot release brings `exist` to zero during shutdown.
    recycler_waker: Option<Waker>,
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

            // Spawn the ttl check interval if `inactive_connection_ttl` isn't `0` or
            // connections have an absolute TTL.
            if pool_opts.inactive_connection_ttl() > Duration::ZERO
                || pool_opts.abs_conn_ttl().is_some()
            {
                tokio::spawn(TtlCheckInterval::new(pool_opts, inner.clone()));
            }
        }
    }

    /// Pops the head waiter if available capacity exceeds eligible popped waiters.
    fn pop_waker_if_needed(&mut self, max: usize) -> Option<Waker> {
        let capacity = self.available.len() + max.saturating_sub(self.exist);
        if capacity > 0 && self.waiting.has_fewer_eligible_woken(capacity) {
            self.waiting.pop_waker()
        } else {
            None
        }
    }
}

/// Connection pool data.
#[derive(Debug)]
pub struct Inner {
    metrics: Arc<Metrics>,
    close: atomic::AtomicBool,
    closed: atomic::AtomicBool,
    exchange: Mutex<Exchange>,
}

impl Inner {
    /// Decrements the existing connection count and wakes the next waiter.
    ///
    /// Also wakes the recycler when `exist` reaches zero so shutdown or pool drop can finish.
    pub(super) fn release_connection_slot(&self) {
        let (waiter_waker, recycler_waker) = {
            let mut exchange = self.exchange.lock().unwrap();
            exchange.exist -= 1;
            self.metrics
                .connection_count
                .store(exchange.exist, atomic::Ordering::Relaxed);
            let recycler_waker = if exchange.exist == 0 {
                exchange.recycler_waker.take()
            } else {
                None
            };
            let waiter_waker = exchange.waiting.pop_waker();
            (waiter_waker, recycler_waker)
        };
        if let Some(waker) = waiter_waker {
            waker.wake();
        }
        if let Some(waker) = recycler_waker {
            waker.wake();
        }
    }
}

/// Keeps a pool slot charged until the checked connection is returned or its socket is closed.
struct CheckingConnGuard {
    conn: Option<Conn>,
    inner: Option<Arc<Inner>>,
}

impl CheckingConnGuard {
    fn new(conn: Conn, inner: Arc<Inner>) -> Self {
        Self {
            conn: Some(conn),
            inner: Some(inner),
        }
    }

    fn conn_mut(&mut self) -> &mut Conn {
        self.conn
            .as_mut()
            .expect("CheckingConnGuard still owns the connection")
    }

    fn take_conn(mut self) -> Conn {
        self.inner = None;
        self.conn
            .take()
            .expect("CheckingConnGuard still owns the connection")
    }

    fn disarm_slot_release(&mut self) {
        // Disarm slot release because `get_conn` calls `cancel_connection` on check errors.
        // `Drop` still closes the socket synchronously before `cancel_connection` frees the slot.
        self.inner = None;
    }
}

impl Drop for CheckingConnGuard {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            // Close the socket synchronously before releasing the slot so open sockets
            // stay within the pool maximum.
            conn.inner.disconnected = true;
            drop(conn);
        }
        if let Some(inner) = self.inner.take() {
            inner.release_connection_slot();
        }
    }
}

/// Asynchronous pool of MySql connections.
///
/// Actually `Pool` is a shared reference, i.e. every clone will lead to the same instance
/// created with [`Pool::new`]. Also `Pool` satisfies `Send` and `Sync`, so you don't have to wrap
/// it into an `Arc` or `Mutex`.
///
/// Note that you will probably want to await [`Pool::disconnect`] before dropping the runtime, as
/// otherwise you may end up with a number of connections that are not cleanly terminated.
///
/// ## Multi-runtime environments
///
/// `Pool` must not be shared across independent tokio runtimes. Each `tokio::net::TcpStream`
/// inside a pooled connection is bound to the I/O driver of the runtime that established it;
/// connections become invalid when that runtime shuts down. This is most likely to surface
/// during graceful shutdown, when some runtimes exit before others while pooled connections
/// are still in circulation. The pool's background tasks ([`Recycler`], TTL interval) are
/// also spawned on the first runtime to call [`Pool::get_conn`] and do not survive that
/// runtime's shutdown.
///
/// If your framework runs each worker on its own `current_thread` runtime (e.g. actix-server),
/// create a separate `Pool` per worker rather than sharing one across all workers.
#[derive(Debug, Clone)]
pub struct Pool {
    opts: Opts,
    inner: Arc<Inner>,
    drop: mpsc::UnboundedSender<Option<Conn>>,
}

impl Pool {
    /// Creates a new pool of connections.
    ///
    /// # Panic
    ///
    /// It'll panic if `Opts::try_from(opts)` returns error.
    pub fn new<O>(opts: O) -> Pool
    where
        Opts: TryFrom<O>,
        <Opts as TryFrom<O>>::Error: std::error::Error,
    {
        let opts = Opts::try_from(opts).unwrap();
        let pool_opts = opts.pool_opts().clone();
        let metrics = Arc::new(Metrics::default());
        let (tx, rx) = mpsc::unbounded_channel();
        Pool {
            opts,
            inner: Arc::new(Inner {
                close: false.into(),
                closed: false.into(),
                metrics: metrics.clone(),
                exchange: Mutex::new(Exchange {
                    available: InPoolConnections::new(
                        pool_opts.constraints().max(),
                        metrics.clone(),
                    ),
                    waiting: Waitlist::new(metrics),
                    exist: 0,
                    recycler_waker: None,
                    recycler: Some((rx, pool_opts)),
                }),
            }),
            drop: tx,
        }
    }

    /// Returns metrics for the connection pool.
    pub fn metrics(&self) -> Arc<Metrics> {
        self.inner.metrics.clone()
    }

    /// Creates a new pool of connections.
    pub fn from_url<T: AsRef<str>>(url: T) -> Result<Pool> {
        let opts = Opts::from_str(url.as_ref())?;
        Ok(Pool::new(opts))
    }

    /// Async function that resolves to `Conn`.
    pub fn get_conn(&self) -> impl Future<Output = Result<Conn>> {
        get_conn(self.clone())
    }

    /// Starts a new transaction.
    pub async fn start_transaction(&self, options: TxOpts) -> Result<Transaction<'static>> {
        let conn = self.get_conn().await?;
        Transaction::new(conn, options).await
    }

    /// Disconnects this pool from the server and resolves to `()`.
    ///
    /// **Note:** This future waits until all checked-out connections are dropped or disconnected,
    /// and until any `get_conn()` already establishing a connection finishes setup or is dropped.
    /// Parked and new `get_conn()` calls fail with [`DriverError::PoolDisconnected`].
    /// Callers that need bounded shutdown should cancel or time-limit outstanding `get_conn()` calls.
    pub async fn disconnect(self) -> Result<()> {
        disconnect_pool(self).await
    }

    /// A way to return connection taken from a pool.
    fn return_conn(&mut self, conn: Conn) {
        // NOTE: we're not in async context here, so we can't block or return NotReady
        // any and all cleanup work _has_ to be done in the spawned recycler
        self.inner
            .metrics
            .connections_in_use
            .fetch_sub(1, Ordering::Relaxed);
        self.send_to_recycler(conn);
    }

    pub(super) fn send_to_recycler(&self, conn: Conn) {
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

    /// Releases a pool connection slot when a connection does not reach the pool,
    /// then wakes the next waiter.
    pub(super) fn release_connection_slot(&self) {
        self.inner.release_connection_slot();
    }

    /// Indicates that connection creation failed and releases the connection slot.
    ///
    /// Decrements `exist` so a broken or dropped connection does not count toward the limit.
    pub(super) fn cancel_connection(&self) {
        self.inner
            .metrics
            .create_failed
            .fetch_add(1, atomic::Ordering::Relaxed);
        self.release_connection_slot();
    }

    /// Polls the pool for an available connection or a new connection slot.
    fn poll_new_conn(
        &mut self,
        cx: &mut Context<'_>,
        queue_id: QueueId,
    ) -> Poll<Result<GetConnInner>> {
        let mut exchange = self.inner.exchange.lock().unwrap();
        let was_woken = exchange.waiting.clear_woken(queue_id);

        // NOTE: this load must happen while we hold the lock,
        // otherwise the recycler may choose to exit, see that .exist == 0, and then exit,
        // and then we decide to create a new connection, which would then never be torn down.
        if self.inner.close.load(atomic::Ordering::Acquire) {
            exchange.waiting.remove(queue_id);
            return Poll::Ready(Err(Error::Driver(DriverError::PoolDisconnected)));
        }

        exchange.spawn_futures_if_needed(&self.inner);

        // Check if we are higher priority than anything current. The comparison is strict so
        // a still-queued waiter does not take a connection ahead of an earlier popped waiter.
        let highest = if let Some(cur) = exchange.waiting.peek_id() {
            queue_id > cur
        } else {
            true
        };

        // If we are not, just queue
        if !highest {
            exchange.waiting.push(cx.waker().clone(), queue_id);
            // If this waiter held a wakeup but ranks behind an older queued waiter,
            // wake the eligible head when available capacity is uncovered.
            let wake_head = if was_woken {
                exchange.pop_waker_if_needed(self.opts.pool_opts().constraints().max())
            } else {
                None
            };
            drop(exchange);
            if let Some(waker) = wake_head {
                waker.wake();
            }
            return Poll::Pending;
        }

        #[allow(unused_variables)] // `since` is only used when `hdrhistogram` is enabled
        while let Some(IdlingConn { mut conn, since }) = exchange.available.pop_back() {
            if !conn.expired() {
                #[cfg(feature = "hdrhistogram")]
                self.inner
                    .metrics
                    .connection_idle_duration
                    .lock()
                    .unwrap()
                    .record(since.elapsed().as_micros() as u64)
                    .ok();
                #[cfg(feature = "hdrhistogram")]
                let metrics = self.metrics();
                conn.inner.active_since = Instant::now();
                // Create the guard outside the async block so dropping before the first poll
                // still closes the socket and releases the connection slot.
                let mut guard = CheckingConnGuard::new(conn, self.inner.clone());
                return Poll::Ready(Ok(GetConnInner::Checking(
                    async move {
                        let check_result = match guard.conn_mut().stream_mut() {
                            Ok(stream) => stream.check().await.map_err(Error::from),
                            Err(err) => Err(err),
                        };
                        match check_result {
                            Ok(()) => {
                                #[cfg(feature = "hdrhistogram")]
                                metrics
                                    .check_duration
                                    .lock()
                                    .unwrap()
                                    .record(
                                        guard.conn_mut().inner.active_since.elapsed().as_micros()
                                            as u64,
                                    )
                                    .ok();
                                Ok(guard.take_conn())
                            }
                            Err(err) => {
                                guard.disarm_slot_release();
                                Err(err)
                            }
                        }
                    }
                    .boxed(),
                )));
            } else {
                self.send_to_recycler(conn);
            }
        }

        if exchange.exist < self.opts.pool_opts().constraints().max() {
            exchange.exist += 1;

            self.inner
                .metrics
                .connection_count
                .store(exchange.exist, atomic::Ordering::Relaxed);

            let opts = self.opts.clone();
            #[cfg(feature = "hdrhistogram")]
            let metrics = self.metrics();

            return Poll::Ready(Ok(GetConnInner::Connecting(
                async move {
                    let conn = Conn::new(opts).await;
                    #[cfg(feature = "hdrhistogram")]
                    if let Ok(conn) = &conn {
                        metrics
                            .connect_duration
                            .lock()
                            .unwrap()
                            .record(conn.inner.active_since.elapsed().as_micros() as u64)
                            .ok();
                    }
                    conn
                }
                .boxed(),
            )));
        }

        // Polled, but no conn available? Back into the queue.
        exchange.waiting.push(cx.waker().clone(), queue_id);
        Poll::Pending
    }

    fn unqueue(&self, queue_id: QueueId) {
        let waker = {
            let mut exchange = self.inner.exchange.lock().unwrap();
            exchange.waiting.remove(queue_id);
            let was_woken = exchange.waiting.clear_woken(queue_id);
            let closing = self.inner.close.load(atomic::Ordering::Acquire);
            let max = self.opts.pool_opts().constraints().max();
            // Wake the next waiter if the pool is closing, or if this waiter held a wakeup
            // and the remaining eligible popped waiters do not cover the available capacity.
            if closing {
                exchange.waiting.pop_waker()
            } else if was_woken {
                exchange.pop_waker_if_needed(max)
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.inner.infile_handler = None;

        if std::thread::panicking() {
            // Try to decrease the number of existing connections.
            if let Some(pool) = self.inner.pool.take() {
                pool.inner
                    .metrics
                    .connections_in_use
                    .fetch_sub(1, Ordering::Relaxed);
                pool.cancel_connection();
            }

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
    use futures_util::{
        future::{join_all, select, select_all, try_join_all, Either},
        poll, try_join, FutureExt,
    };
    use tokio::time::{sleep, timeout};
    use waker_fn::waker_fn;

    use std::{
        future::Future,
        pin::pin,
        sync::{Arc, OnceLock},
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };

    use crate::{
        conn::pool::Pool, opts::PoolOpts, prelude::*, test_misc::get_opts, PoolConstraints, Row,
        TxOpts, Value,
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

    fn pool_with_one_connection() -> Pool {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(1, 1).unwrap());
        let opts = get_opts().pool_opts(pool_opts.clone());
        Pool::new(opts)
    }

    #[tokio::test]
    async fn should_opt_out_of_connection_reset() -> super::Result<()> {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(1, 1).unwrap());
        let opts = get_opts().pool_opts(pool_opts.clone());

        let pool = Pool::new(opts.clone());

        let mut conn = pool.get_conn().await.unwrap();
        assert_eq!(
            conn.query_first::<Value, _>("SELECT @foo").await?.unwrap(),
            Value::NULL
        );
        conn.query_drop("SET @foo = 'foo'").await?;
        assert_eq!(
            conn.query_first::<String, _>("SELECT @foo").await?.unwrap(),
            "foo",
        );
        drop(conn);

        conn = pool.get_conn().await.unwrap();
        assert_eq!(
            conn.query_first::<Value, _>("SELECT @foo").await?.unwrap(),
            Value::NULL
        );
        conn.query_drop("SET @foo = 'foo'").await?;
        conn.reset_connection(false);
        drop(conn);

        conn = pool.get_conn().await.unwrap();
        assert_eq!(
            conn.query_first::<String, _>("SELECT @foo").await?.unwrap(),
            "foo",
        );
        drop(conn);
        pool.disconnect().await.unwrap();

        let pool = Pool::new(opts.pool_opts(pool_opts.with_reset_connection(false)));
        conn = pool.get_conn().await.unwrap();
        conn.query_drop("SET @foo = 'foo'").await?;
        drop(conn);
        conn = pool.get_conn().await.unwrap();
        assert_eq!(
            conn.query_first::<String, _>("SELECT @foo").await?.unwrap(),
            "foo",
        );
        drop(conn);
        pool.disconnect().await
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
        let pool = Pool::new(crate::Opts::from(get_opts()));
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
                async {
                    let mut conn = pool.get_conn().await?;
                    conn.ping().await?;
                    crate::Result::Ok(conn)
                }
                .boxed()
            });

            // collect ids..
            let ids = try_join_all(connections)
                .await?
                .into_iter()
                .map(|conn| conn.id())
                .collect::<Vec<_>>();

            // give some time to reset connections
            sleep(Duration::from_millis(1000)).await;

            // get_conn should work if connection is available and alive
            pool.get_conn().await?;

            // give some time to reset connections
            sleep(Duration::from_millis(1000)).await;

            // now we'll kill connections..
            for id in ids {
                master.query_drop(format!("KILL {}", id)).await?;
            }

            // now check, that they're still in the pool..
            assert_eq!(ex_field!(pool, available).len(), NUM_CONNS);

            sleep(Duration::from_millis(500)).await;

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
    async fn should_reuse_connections() -> super::Result<()> {
        let pool = pool_with_one_connection();
        let mut conn = pool.get_conn().await?;

        let server_version = conn.server_version();
        let connection_id = conn.id();

        for _ in 0..16 {
            drop(conn);
            conn = pool.get_conn().await?;
            println!("CONN connection_id={}", conn.id());
            assert!(conn.id() == connection_id || server_version < (5, 7, 2));
        }

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
            while (rx.recv().await).is_some() {}
        }
        drop(pool);
    }

    #[tokio::test]
    async fn should_start_transaction() -> super::Result<()> {
        let pool = pool_with_one_connection();

        "CREATE TABLE IF NOT EXISTS mysql.tmp(id int)"
            .ignore(&pool)
            .await?;
        "DELETE FROM mysql.tmp".ignore(&pool).await?;

        let mut tx = pool.start_transaction(TxOpts::default()).await?;
        tx.exec_batch(
            "INSERT INTO mysql.tmp (id) VALUES (?)",
            vec![(1_u8,), (2_u8,)],
        )
        .await?;
        tx.exec_drop("SELECT * FROM mysql.tmp", ()).await?;
        drop(tx);
        let row_opt = pool
            .get_conn()
            .await?
            .query_first("SELECT COUNT(*) FROM mysql.tmp")
            .await?;
        assert_eq!(row_opt, Some((0u8,)));
        pool.get_conn()
            .await?
            .query_drop("DROP TABLE mysql.tmp")
            .await?;
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
        sleep(Duration::from_millis(100)).await;

        // check that connections are still in the pool because of inactive_connection_ttl
        assert_eq!(ex_field!(pool_clone, available).len(), POOL_MAX);

        // then, wait for ttl_check_interval
        sleep(TTL_CHECK_INTERVAL).await;

        // wait a bit more to let the connections be reclaimed by the ttl check
        sleep(Duration::from_millis(500)).await;

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
            sleep(Duration::from_millis(500)).await;

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
        let pool = Pool::new("mysql://255.255.255.255");

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

        sleep(Duration::from_secs(6)).await;

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
            runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let pool = Pool::new(get_opts());
                    let _conn = pool.get_conn().await.unwrap();
                    std::panic::panic_any(PANIC_MESSAGE);
                });
        });

        assert_eq!(
            *result.unwrap_err().downcast::<&str>().unwrap(),
            "ORIGINAL_PANIC",
        );
    }

    #[test]
    fn should_not_panic_on_unclean_shutdown() {
        // run more than once to trigger different drop orders
        for _ in 0..10 {
            let rt = tokio::runtime::Runtime::new().unwrap();
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
            let rt = tokio::runtime::Runtime::new().unwrap();
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

    #[tokio::test]
    async fn issue_126_should_cleanup_errors_in_multiresult_sets() -> super::Result<()> {
        let pool_constraints = PoolConstraints::new(0, 1).unwrap();
        let pool_opts = PoolOpts::default().with_constraints(pool_constraints);

        let pool = Pool::new(get_opts().pool_opts(pool_opts));

        for _ in 0u8..100 {
            pool.get_conn()
                .await?
                .query_iter("DO '42'; BLABLA;")
                .await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_ignore_non_fatal_errors_while_returning_to_a_pool() -> super::Result<()> {
        let pool = pool_with_one_connection();
        let id = pool.get_conn().await?.id();

        // non-fatal errors are ignored
        for _ in 0u8..10 {
            let mut conn = pool.get_conn().await?;
            conn.query_iter("DO '42'; BLABLA;").await?;
            assert_eq!(id, conn.id());
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_remove_waker_of_cancelled_task() {
        let pool = pool_with_one_connection();
        let only_conn = pool.get_conn().await.unwrap();

        let join_handle = tokio::spawn(timeout(Duration::from_secs(1), pool.get_conn()));

        sleep(Duration::from_secs(2)).await;

        match join_handle.await.unwrap() {
            Err(_elapsed) => (),
            _ => panic!("unexpected Ok()"),
        }
        drop(only_conn);

        assert_eq!(0, pool.inner.exchange.lock().unwrap().waiting.len());
        // metrics should catch up with waiting queue (see #335)
        assert_eq!(
            0,
            pool.metrics()
                .active_wait_requests
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn should_work_if_pooled_connection_operation_is_cancelled() -> super::Result<()> {
        let pool = Pool::new(get_opts());

        // warm up
        join_all((0..10).map(|_| pool.get_conn())).await;

        /// some operation
        async fn op(pool: &Pool) {
            let _: Option<Row> = pool
                .get_conn()
                .await
                .unwrap()
                .exec_first("SELECT ?, ?", (42, "foo"))
                .await
                .unwrap();
        }

        // Measure the delay
        let mut max_delay = 0_u128;
        for _ in 0..10_usize {
            let start = std::time::Instant::now();
            op(&pool).await;
            max_delay = std::cmp::max(max_delay, start.elapsed().as_micros());
        }

        for _ in 0_usize..128 {
            let fut = select_all((0_usize..5).map(|_| op(&pool).boxed()));

            // we need to cancel the op in the middle
            // this should not lead to the `packet out of order` error.
            let delay_micros = rand::random::<u128>() % max_delay;
            select(
                sleep(Duration::from_micros(delay_micros as u64)).boxed(),
                fut,
            )
            .await;

            // give some time for connections to return to the pool
            sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn check_absolute_connection_ttl() -> super::Result<()> {
        let constraints = PoolConstraints::new(1, 3).unwrap();
        let pool_opts = PoolOpts::default()
            .with_constraints(constraints)
            .with_inactive_connection_ttl(Duration::from_secs(99))
            .with_ttl_check_interval(Duration::from_secs(1))
            .with_abs_conn_ttl(Some(Duration::from_secs(2)));

        let pool = Pool::new(get_opts().pool_opts(pool_opts));

        let conn_ttl0 = pool.get_conn().await?;
        sleep(Duration::from_millis(1000)).await;
        let conn_ttl1 = pool.get_conn().await?;
        sleep(Duration::from_millis(1000)).await;
        let conn_ttl2 = pool.get_conn().await?;

        drop(conn_ttl0);
        drop(conn_ttl1);
        drop(conn_ttl2);
        assert_eq!(ex_field!(pool, exist), 3);

        sleep(Duration::from_millis(1500)).await;
        assert_eq!(ex_field!(pool, exist), 2);

        sleep(Duration::from_millis(1000)).await;
        assert_eq!(ex_field!(pool, exist), 1);

        // Go even below min pool size.
        sleep(Duration::from_millis(1000)).await;
        assert_eq!(ex_field!(pool, exist), 0);

        Ok(())
    }

    #[tokio::test]
    async fn save_last_waker() {
        // Test that if passed multiple wakers, we call the last one.

        let pool = pool_with_one_connection();

        // Get a connection, so we know the next future will be
        // queued.
        let conn = pool.get_conn().await.unwrap();
        let mut pending_fut = pin!(pool.get_conn());

        let build_waker = || {
            let called = Arc::new(OnceLock::new());
            let called2 = called.clone();
            let waker = waker_fn(move || called2.set(()).unwrap());
            (called, waker)
        };

        let mut assert_pending = |waker| {
            let mut context = Context::from_waker(&waker);
            let p = pending_fut.as_mut().poll(&mut context);
            assert!(matches!(p, Poll::Pending));
        };

        let (first_called, waker) = build_waker();
        assert_pending(waker);

        let (second_called, waker) = build_waker();
        assert_pending(waker);

        drop(conn);

        while second_called.get().is_none() {
            assert!(first_called.get().is_none());
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(first_called.get().is_none());
    }

    #[tokio::test]
    async fn check_priorities() -> super::Result<()> {
        let pool = pool_with_one_connection();

        let queue_len = || {
            let exchange = pool.inner.exchange.lock().unwrap();
            exchange.waiting.len()
        };

        // Get a connection, so we know the next futures will be
        // queued.
        let conn = pool.get_conn().await.unwrap();

        #[allow(clippy::async_yields_async)]
        let get_pending = || async {
            let fut = async {
                pool.get_conn().await.unwrap();
            }
            .shared();
            let p = poll!(fut.clone());
            assert!(matches!(p, Poll::Pending));
            fut
        };

        let fut1 = get_pending().await;
        let fut2 = get_pending().await;

        // Both futures are queued
        assert_eq!(queue_len(), 2);

        drop(conn); // This will pop fut1 from the queue, making it [2]
        while queue_len() != 1 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // We called wake on fut1, and even with the select fut1 will
        // resolve first
        let Either::Right((_, fut2)) = select(fut2, fut1).await else {
            panic!("wrong future");
        };

        // We dropped the connection of fut1, but very likely hasn't
        // made it through the recycler yet.
        assert_eq!(queue_len(), 1);

        let p = poll!(fut2.clone());
        assert!(matches!(p, Poll::Pending));
        assert_eq!(queue_len(), 1); // The queue still has fut2

        // The connection will pass by the recycler and unblock fut2
        // and pop it from the queue.
        fut2.await;
        assert_eq!(queue_len(), 0);

        // The recycler is probably not done, so a new future will be
        // pending.
        let fut3 = get_pending().await;
        assert_eq!(queue_len(), 1);

        // It is OK to await it.
        fut3.await;

        Ok(())
    }

    #[cfg(feature = "hdrhistogram")]
    #[tokio::test]
    async fn metrics() -> super::Result<()> {
        let pool = pool_with_one_connection();

        let metrics = pool.metrics();
        let conn = pool.get_conn().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(conn);
        pool.get_conn().await.unwrap();

        let max = metrics.connection_active_duration.lock().unwrap().max();
        // We slept for 100 miliseconds holding a conneciton.
        assert!(max > 100_000);

        Ok(())
    }

    #[tokio::test]
    async fn connection_in_use_metric() {
        let pool = pool_with_one_connection();
        let metrics = pool.metrics();

        let conn = pool.get_conn().await.unwrap();
        assert_eq!(
            metrics
                .connections_in_use
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        drop(conn);
        assert_eq!(
            metrics
                .connections_in_use
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        loop {
            if metrics
                .connections_in_pool
                .load(std::sync::atomic::Ordering::Relaxed)
                == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Ensure that we increase the connections in use metric when we get a connection from the pool
        let conn = pool.get_conn().await.unwrap();
        assert_eq!(
            metrics
                .connections_in_use
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics
                .connections_in_pool
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        drop(conn);
        assert_eq!(
            metrics
                .connections_in_use
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        loop {
            if metrics
                .connections_in_pool
                .load(std::sync::atomic::Ordering::Relaxed)
                == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    #[tokio::test]
    async fn pool_waitlist_woken_waiter_claims_returned_conn() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let opts = crate::Opts::from(get_opts().pool_opts(pool_opts));
        let mut pool = Pool::new(opts.clone());

        // Occupy the single connection slot so `poll_new_conn` queues the waiter.
        ex_field!(pool, exist) = 1;
        let q1 = super::QueueId::next();

        let woken = Arc::new(OnceLock::new());
        let woken_clone = woken.clone();
        let waker = waker_fn(move || {
            let _ = woken_clone.set(());
        });
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(pool.poll_new_conn(&mut cx, q1), Poll::Pending));
        assert_eq!(ex_field!(pool, waiting).len(), 1);
        assert_eq!(
            pool.metrics()
                .active_wait_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // Return an idle connection and wake the queued waiter.
        let popped_waker = {
            let mut conn = crate::Conn::empty(opts);
            conn.inner.disconnected = true;
            let mut exchange = pool.inner.exchange.lock().unwrap();
            exchange.available.push_back(conn.into());
            exchange.waiting.pop_waker()
        };
        popped_waker
            .expect("expected the queued waiter to be popped")
            .wake();
        assert!(woken.get().is_some());

        // On the next poll, `q1` claims the idle connection.
        let checking = pool.poll_new_conn(&mut cx, q1);
        assert!(matches!(
            checking,
            Poll::Ready(Ok(super::futures::GetConnInner::Checking(_)))
        ));
        assert_eq!(ex_field!(pool, available).len(), 0);
        assert_eq!(ex_field!(pool, waiting).len(), 0);
        assert_eq!(ex_field!(pool, exist), 1);

        // Dropping the checking future releases the connection slot.
        drop(checking);
        assert_eq!(ex_field!(pool, exist), 0);
        assert_eq!(
            pool.metrics()
                .active_wait_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn pool_waitlist_queued_waiter_does_not_steal_woken_waiters_connection() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let opts = crate::Opts::from(get_opts().pool_opts(pool_opts));
        let mut pool = Pool::new(opts.clone());

        // Occupy the single connection slot so `q1` and `q2` both enqueue.
        ex_field!(pool, exist) = 1;
        let q1 = super::QueueId::next();
        let q2 = super::QueueId::next();

        let noop_waker = Waker::noop();
        let mut cx = Context::from_waker(noop_waker);

        assert!(matches!(pool.poll_new_conn(&mut cx, q1), Poll::Pending));
        assert!(matches!(pool.poll_new_conn(&mut cx, q2), Poll::Pending));
        assert_eq!(ex_field!(pool, waiting).len(), 2);

        // Return an idle connection and pop `q1`. `q2` remains at the head of `waiting`.
        let popped_waker = {
            let mut conn = crate::Conn::empty(opts);
            conn.inner.disconnected = true;
            let mut exchange = pool.inner.exchange.lock().unwrap();
            exchange.available.push_back(conn.into());
            exchange.waiting.pop_waker()
        };
        assert!(popped_waker.is_some());
        assert_eq!(ex_field!(pool, waiting).len(), 1);

        // Poll the still-queued waiter `q2` before `q1`. `q2` must not take the connection.
        assert!(matches!(pool.poll_new_conn(&mut cx, q2), Poll::Pending));
        assert_eq!(ex_field!(pool, available).len(), 1);
        assert_eq!(ex_field!(pool, waiting).len(), 1);

        // Poll `q1`. `q1` has higher priority than `q2` and takes the connection.
        let checking = pool.poll_new_conn(&mut cx, q1);
        assert!(matches!(
            checking,
            Poll::Ready(Ok(super::futures::GetConnInner::Checking(_)))
        ));
        assert_eq!(ex_field!(pool, available).len(), 0);
        assert_eq!(ex_field!(pool, waiting).len(), 1);

        // Drop `checking` to release the slot and pop `q2`.
        drop(checking);
        assert_eq!(ex_field!(pool, exist), 0);
        assert_eq!(ex_field!(pool, waiting).len(), 0);
        pool.unqueue(q2);
    }

    #[tokio::test]
    async fn pool_waitlist_wakes_next_waiter_when_woken_waiter_cancels() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let pool = Pool::new(get_opts().pool_opts(pool_opts));

        // Occupy the single connection slot so both callers enqueue in `exchange.waiting`.
        ex_field!(pool, exist) = 1;

        let noop_waker = Waker::noop();
        let mut cx1 = Context::from_waker(noop_waker);

        let fut2_woken = Arc::new(OnceLock::new());
        let fut2_woken_clone = fut2_woken.clone();
        let waker2 = waker_fn(move || {
            let _ = fut2_woken_clone.set(());
        });
        let mut cx2 = Context::from_waker(&waker2);

        {
            let mut fut1 = Box::pin(pool.get_conn());
            assert!(matches!(fut1.as_mut().poll(&mut cx1), Poll::Pending));

            let mut fut2 = pin!(pool.get_conn());
            assert!(matches!(fut2.as_mut().poll(&mut cx2), Poll::Pending));
            assert_eq!(ex_field!(pool, waiting).len(), 2);

            // Release the slot. This pops `fut1` from `waiting` and leaves `fut2` queued.
            pool.release_connection_slot();
            assert_eq!(ex_field!(pool, exist), 0);
            assert_eq!(ex_field!(pool, waiting).len(), 1);
            assert!(fut2_woken.get().is_none());

            // Drop `fut1` before its next poll. `unqueue` must wake `fut2`.
            drop(fut1);
            assert!(
                fut2_woken.get().is_some(),
                "cancelling a woken waiter failed to wake the next waiter"
            );
            assert_eq!(ex_field!(pool, waiting).len(), 0);
        }

        assert_eq!(
            pool.metrics()
                .active_wait_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn pool_waitlist_cancelling_queued_waiter_does_not_wake_others() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let opts = crate::Opts::from(get_opts().pool_opts(pool_opts));
        let pool = Pool::new(opts.clone());

        // Occupy the single connection slot so `fut1`, `fut2`, and `fut3` all enqueue.
        ex_field!(pool, exist) = 1;

        let noop_waker = Waker::noop();
        let mut cx1 = Context::from_waker(noop_waker);
        let mut cx3 = Context::from_waker(noop_waker);

        let fut2_woken = Arc::new(OnceLock::new());
        let fut2_woken_clone = fut2_woken.clone();
        let waker2 = waker_fn(move || {
            let _ = fut2_woken_clone.set(());
        });
        let mut cx2 = Context::from_waker(&waker2);

        {
            let mut fut1 = pin!(pool.get_conn());
            assert!(matches!(fut1.as_mut().poll(&mut cx1), Poll::Pending));

            let mut fut2 = pin!(pool.get_conn());
            assert!(matches!(fut2.as_mut().poll(&mut cx2), Poll::Pending));

            let mut fut3 = Box::pin(pool.get_conn());
            assert!(matches!(fut3.as_mut().poll(&mut cx3), Poll::Pending));
            assert_eq!(ex_field!(pool, waiting).len(), 3);

            // Return an idle connection and pop `fut1`. `fut2` and `fut3` remain queued.
            let popped_waker = {
                let mut conn = crate::Conn::empty(opts);
                conn.inner.disconnected = true;
                let mut exchange = pool.inner.exchange.lock().unwrap();
                exchange.available.push_back(conn.into());
                exchange.waiting.pop_waker()
            };
            assert!(popped_waker.is_some());
            assert_eq!(ex_field!(pool, waiting).len(), 2);

            // Drop `fut3` while it is still queued. `fut2` must remain asleep because `fut1`
            // already covers the available connection.
            drop(fut3);
            assert!(
                fut2_woken.get().is_none(),
                "cancelling a still-queued waiter must not wake another waiter"
            );
            assert_eq!(ex_field!(pool, waiting).len(), 1);

            // Clear synthetic test state before `fut1` and `fut2` drop.
            let mut exchange = pool.inner.exchange.lock().unwrap();
            let _ = exchange.available.pop_back();
            exchange.exist = 0;
        }

        assert_eq!(
            pool.metrics()
                .active_wait_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn pool_waitlist_cancelling_old_wakeup_does_not_wake_younger_waiter() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let opts = crate::Opts::from(get_opts().pool_opts(pool_opts));
        let mut pool = Pool::new(opts.clone());

        // Occupy the single connection slot so waiter A enqueues.
        ex_field!(pool, exist) = 1;

        let noop_waker = Waker::noop();
        let mut cx_a = Context::from_waker(noop_waker);
        let mut cx_b = Context::from_waker(noop_waker);
        let mut cx_x = Context::from_waker(noop_waker);

        let c_woken = Arc::new(OnceLock::new());
        let c_woken_clone = c_woken.clone();
        let waker_c = waker_fn(move || {
            let _ = c_woken_clone.set(());
        });
        let mut cx_c = Context::from_waker(&waker_c);

        {
            let mut fut_a = Box::pin(pool.get_conn());
            assert!(matches!(fut_a.as_mut().poll(&mut cx_a), Poll::Pending));
            assert_eq!(ex_field!(pool, waiting).len(), 1);

            // Return a connection and pop A. The queue is now empty while A has not polled yet.
            {
                let mut conn = crate::Conn::empty(opts.clone());
                conn.inner.disconnected = true;
                let mut exchange = pool.inner.exchange.lock().unwrap();
                exchange.available.push_back(conn.into());
                assert!(exchange.waiting.pop_waker().is_some());
            }

            // A newcomer X checks out the connection before A runs.
            let q_x = super::QueueId::next();
            let x_checking = pool.poll_new_conn(&mut cx_x, q_x);
            assert!(matches!(
                x_checking,
                Poll::Ready(Ok(super::futures::GetConnInner::Checking(_)))
            ));
            assert_eq!(ex_field!(pool, available).len(), 0);

            // Waiters B and C enqueue in order while X holds the connection.
            let mut fut_b = Box::pin(pool.get_conn());
            assert!(matches!(fut_b.as_mut().poll(&mut cx_b), Poll::Pending));
            let mut fut_c = pin!(pool.get_conn());
            assert!(matches!(fut_c.as_mut().poll(&mut cx_c), Poll::Pending));
            assert_eq!(ex_field!(pool, waiting).len(), 2);

            // Return the connection and pop B. C remains queued.
            {
                let mut conn = crate::Conn::empty(opts);
                conn.inner.disconnected = true;
                let mut exchange = pool.inner.exchange.lock().unwrap();
                exchange.available.push_back(conn.into());
                assert!(exchange.waiting.pop_waker().is_some());
            }
            assert_eq!(ex_field!(pool, waiting).len(), 1);

            // Cancel A. B already covers the available connection, so C must remain queued.
            drop(fut_a);
            assert!(
                c_woken.get().is_none(),
                "cancelling an older unused wakeup must not wake C while B is woken"
            );
            assert_eq!(ex_field!(pool, waiting).len(), 1);

            // Cancel B as well. Now no woken waiter covers the connection, so C must wake.
            drop(fut_b);
            assert!(
                c_woken.get().is_some(),
                "cancelling B when no other woken waiter remains must wake C"
            );
            assert_eq!(ex_field!(pool, waiting).len(), 0);

            // Clear synthetic test state before `x_checking` and `fut_c` drop.
            let mut exchange = pool.inner.exchange.lock().unwrap();
            let _ = exchange.available.pop_back();
            drop(exchange);
            drop(x_checking);
        }

        assert_eq!(ex_field!(pool, exist), 0);
        assert_eq!(
            pool.metrics()
                .active_wait_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn pool_waitlist_cancelling_head_with_lower_priority_woken_wakes_next_head() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let opts = crate::Opts::from(get_opts().pool_opts(pool_opts));
        let mut pool = Pool::new(opts.clone());

        // Occupy the single connection slot so A, B, and C all enqueue in priority order.
        ex_field!(pool, exist) = 1;

        let noop_waker = Waker::noop();
        let mut cx_a = Context::from_waker(noop_waker);
        let mut cx_c = Context::from_waker(noop_waker);

        let b_woken = Arc::new(OnceLock::new());
        let b_woken_clone = b_woken.clone();
        let waker_b = waker_fn(move || {
            let _ = b_woken_clone.set(());
        });
        let mut cx_b = Context::from_waker(&waker_b);

        let q_a = super::QueueId::next();
        let q_b = super::QueueId::next();
        let q_c = super::QueueId::next();

        assert!(matches!(pool.poll_new_conn(&mut cx_a, q_a), Poll::Pending));
        assert!(matches!(pool.poll_new_conn(&mut cx_b, q_b), Poll::Pending));
        assert!(matches!(pool.poll_new_conn(&mut cx_c, q_c), Poll::Pending));
        assert_eq!(ex_field!(pool, waiting).len(), 3);

        // Pop A, B, and C into `woken` while another caller still holds the slot.
        {
            let mut exchange = pool.inner.exchange.lock().unwrap();
            assert!(exchange.waiting.pop_waker().is_some());
            assert!(exchange.waiting.pop_waker().is_some());
            assert!(exchange.waiting.pop_waker().is_some());
        }
        assert_eq!(ex_field!(pool, waiting).len(), 0);

        // Poll A and B while `exist == 1` and `available == 0`. Both requeue in order [A, B],
        // leaving only the younger waiter C with an outstanding wakeup in `woken`.
        assert!(matches!(pool.poll_new_conn(&mut cx_a, q_a), Poll::Pending));
        assert!(matches!(pool.poll_new_conn(&mut cx_b, q_b), Poll::Pending));
        assert_eq!(ex_field!(pool, waiting).len(), 2);

        // Return an idle connection and pop A. `waiting` is [B] and `woken` is {A, C}.
        {
            let mut conn = crate::Conn::empty(opts.clone());
            conn.inner.disconnected = true;
            let mut exchange = pool.inner.exchange.lock().unwrap();
            exchange.available.push_back(conn.into());
            assert!(exchange.waiting.pop_waker().is_some());
        }
        assert_eq!(ex_field!(pool, waiting).len(), 1);
        assert!(b_woken.get().is_none());

        // Cancel A. C ranks behind B, so C cannot claim the connection. `unqueue` must wake B.
        pool.unqueue(q_a);
        assert!(
            b_woken.get().is_some(),
            "cancelling A while only lower-priority C was woken failed to wake B"
        );
        assert_eq!(ex_field!(pool, waiting).len(), 0);

        // Poll B. B claims the idle connection while C remains parked when polled next.
        let b_checking = pool.poll_new_conn(&mut cx_b, q_b);
        assert!(matches!(
            b_checking,
            Poll::Ready(Ok(super::futures::GetConnInner::Checking(_)))
        ));
        assert_eq!(ex_field!(pool, available).len(), 0);
        assert_eq!(ex_field!(pool, exist), 1);

        assert!(matches!(pool.poll_new_conn(&mut cx_c, q_c), Poll::Pending));
        assert_eq!(ex_field!(pool, waiting).len(), 1);

        // Drop B's checking future to release the slot and pop C.
        drop(b_checking);
        assert_eq!(ex_field!(pool, exist), 0);
        assert_eq!(ex_field!(pool, waiting).len(), 0);
        pool.unqueue(q_c);

        // Also verify lower-priority requeue handoff in `poll_new_conn` directly:
        // if C is popped while B sits at the head and an idle connection is available,
        // polling C requeues C behind B and wakes B.
        ex_field!(pool, exist) = 1;
        let q_b2 = super::QueueId::next();
        let q_c2 = super::QueueId::next();
        let b2_woken = Arc::new(OnceLock::new());
        let b2_woken_clone = b2_woken.clone();
        let waker_b2 = waker_fn(move || {
            let _ = b2_woken_clone.set(());
        });
        let mut cx_b2 = Context::from_waker(&waker_b2);

        assert!(matches!(pool.poll_new_conn(&mut cx_c, q_c2), Poll::Pending));
        {
            let mut exchange = pool.inner.exchange.lock().unwrap();
            assert!(exchange.waiting.pop_waker().is_some());
        }
        assert!(matches!(
            pool.poll_new_conn(&mut cx_b2, q_b2),
            Poll::Pending
        ));
        {
            let mut conn = crate::Conn::empty(opts);
            conn.inner.disconnected = true;
            let mut exchange = pool.inner.exchange.lock().unwrap();
            exchange.available.push_back(conn.into());
        }
        assert!(matches!(pool.poll_new_conn(&mut cx_c, q_c2), Poll::Pending));
        assert!(
            b2_woken.get().is_some(),
            "requeueing a lower-priority woken waiter did not wake the eligible head"
        );
        assert_eq!(ex_field!(pool, waiting).len(), 1);

        let checking = pool.poll_new_conn(&mut cx_b2, q_b2);
        assert!(matches!(
            checking,
            Poll::Ready(Ok(super::futures::GetConnInner::Checking(_)))
        ));
        drop(checking);
        pool.unqueue(q_c2);
        assert_eq!(ex_field!(pool, exist), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pool_waitlist_cancelling_checking_closes_socket_before_releasing_slot() {
        use std::{io::Read, os::unix::net::UnixListener};

        let temp_dir = tempfile::tempdir().unwrap();
        let socket_path = temp_dir.path().join("checking.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();

        let client_stream = crate::io::Stream::connect_socket(&socket_path)
            .await
            .unwrap();
        let (mut server_stream, _) = listener.accept().unwrap();
        server_stream.set_nonblocking(true).unwrap();

        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let opts = crate::Opts::from(
            crate::OptsBuilder::default()
                .socket(Some(socket_path.to_string_lossy().into_owned()))
                .pool_opts(pool_opts),
        );
        let mut pool = Pool::new(opts.clone());

        // Place the real Unix-socket connection into `available` occupying the single slot.
        {
            let mut conn = crate::Conn::empty(opts);
            conn.inner.stream = Some(client_stream);
            let mut exchange = pool.inner.exchange.lock().unwrap();
            exchange.exist = 1;
            pool.metrics()
                .connection_count
                .store(1, std::sync::atomic::Ordering::Relaxed);
            exchange.available.push_back(conn.into());
        }

        let noop_waker = Waker::noop();
        let mut cx1 = Context::from_waker(noop_waker);
        let q1 = super::QueueId::next();

        // Take the idle connection into `GetConnInner::Checking`.
        let checking = pool.poll_new_conn(&mut cx1, q1);
        assert!(matches!(
            checking,
            Poll::Ready(Ok(super::futures::GetConnInner::Checking(_)))
        ));
        assert_eq!(ex_field!(pool, available).len(), 0);
        assert_eq!(ex_field!(pool, exist), 1);

        // Verify the peer socket is still open while `checking` is alive.
        let mut buf = [0_u8; 1];
        assert_eq!(
            server_stream.read(&mut buf).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );

        let woken = Arc::new(OnceLock::new());
        let woken_clone = woken.clone();
        let waker2 = waker_fn(move || {
            let _ = woken_clone.set(());
        });
        let mut cx2 = Context::from_waker(&waker2);

        let mut fut2 = Box::pin(pool.get_conn());
        assert!(matches!(fut2.as_mut().poll(&mut cx2), Poll::Pending));
        assert_eq!(ex_field!(pool, waiting).len(), 1);

        // Drop `checking`. It must close the socket synchronously before releasing the slot.
        drop(checking);

        assert!(
            woken.get().is_some(),
            "dropping a checking future did not wake the next waiter"
        );
        assert_eq!(ex_field!(pool, exist), 0);
        assert_eq!(ex_field!(pool, waiting).len(), 0);
        assert_eq!(
            pool.metrics()
                .connection_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            pool.metrics()
                .create_failed
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // Before yielding to any spawned cleanup task, start the replacement checkout and verify
        // via direct OS `read` that the original socket already observed EOF.
        assert!(matches!(fut2.as_mut().poll(&mut cx2), Poll::Pending));
        assert_eq!(ex_field!(pool, exist), 1);
        assert_eq!(
            server_stream
                .read(&mut buf)
                .expect("original socket was still open when replacement checkout started"),
            0,
            "expected immediate EOF on the cancelled Checking socket before yielding"
        );

        drop(fut2);
        assert_eq!(ex_field!(pool, exist), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pool_waitlist_cancelling_connecting_closes_socket_before_releasing_slot() {
        use std::{
            io::{Read, Write},
            os::unix::net::UnixListener,
            sync::atomic::{AtomicBool, Ordering},
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let socket_path = temp_dir.path().join("connecting.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();

        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let opts = crate::Opts::from(
            crate::OptsBuilder::default()
                .socket(Some(socket_path.to_string_lossy().into_owned()))
                .pool_opts(pool_opts),
        );
        let pool = Pool::new(opts);

        let fut1_woken = Arc::new(AtomicBool::new(false));
        let fut1_woken_clone = fut1_woken.clone();
        let waker1 = waker_fn(move || {
            fut1_woken_clone.store(true, Ordering::Release);
        });
        let mut cx1 = Context::from_waker(&waker1);

        // Start `fut1` so it connects to the Unix listener.
        let mut fut1 = Box::pin(pool.get_conn());
        assert!(matches!(fut1.as_mut().poll(&mut cx1), Poll::Pending));
        assert_eq!(ex_field!(pool, exist), 1);

        let (mut server_stream, _) = timeout(Duration::from_millis(100), async {
            loop {
                match listener.accept() {
                    Ok(pair) => break pair,
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        tokio::task::yield_now().await;
                    }
                    Err(err) => panic!("unexpected accept error: {err}"),
                }
            }
        })
        .await
        .expect("timed out waiting for Connecting future to connect");
        server_stream.set_nonblocking(true).unwrap();

        // Drive `fut1` past `Stream::connect_socket` into `handle_handshake().await`.
        // Write 1 byte of the 4-byte packet header so `waker1` fires only once the driver
        // has stored `conn.inner.stream` and registered its greeting read.
        fut1_woken.store(false, Ordering::Release);
        assert!(matches!(fut1.as_mut().poll(&mut cx1), Poll::Pending));
        server_stream.write_all(&[0x00]).unwrap();
        timeout(Duration::from_millis(100), async {
            while !fut1_woken.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for Connecting future to read the partial greeting");
        assert!(matches!(fut1.as_mut().poll(&mut cx1), Poll::Pending));

        let mut buf = [0_u8; 1];
        assert_eq!(
            server_stream.read(&mut buf).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );

        let woken = Arc::new(OnceLock::new());
        let woken_clone = woken.clone();
        let waker2 = waker_fn(move || {
            let _ = woken_clone.set(());
        });
        let mut cx2 = Context::from_waker(&waker2);

        let mut fut2 = Box::pin(pool.get_conn());
        assert!(matches!(fut2.as_mut().poll(&mut cx2), Poll::Pending));
        assert_eq!(ex_field!(pool, waiting).len(), 1);

        // Drop `fut1` while waiting for the greeting. It must close its socket synchronously
        // before releasing the slot to `fut2`.
        drop(fut1);

        assert!(
            woken.get().is_some(),
            "dropping a connecting future did not wake the next waiter"
        );
        assert_eq!(ex_field!(pool, exist), 0);
        assert_eq!(ex_field!(pool, waiting).len(), 0);
        assert_eq!(
            pool.metrics()
                .create_failed
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        // Before yielding to any spawned task, start the replacement checkout and verify via
        // direct OS `read` that the original peer socket already observed EOF.
        assert!(matches!(fut2.as_mut().poll(&mut cx2), Poll::Pending));
        assert_eq!(ex_field!(pool, exist), 1);
        assert_eq!(
            server_stream
                .read(&mut buf)
                .expect("original connecting socket was still open when replacement started"),
            0,
            "expected immediate EOF on the cancelled Connecting socket before yielding"
        );

        drop(fut2);
        assert_eq!(ex_field!(pool, exist), 0);
    }

    #[tokio::test]
    async fn pool_waitlist_wakes_waiter_when_ttl_check_frees_slot() {
        let pool_opts = PoolOpts::new()
            .with_constraints(PoolConstraints::new(0, 1).unwrap())
            .with_abs_conn_ttl(Some(Duration::from_millis(1)));
        let opts = crate::Opts::from(get_opts().pool_opts(pool_opts.clone()));
        let pool = Pool::new(opts.clone());

        // Record a deadline and wait briefly so the deadline is strictly in the past without
        // subtracting from `Instant::now()`.
        let expired_deadline = Instant::now();
        tokio::time::sleep(Duration::from_millis(2)).await;

        // Place an expired idle connection into `available` to occupy the single slot.
        {
            let mut conn = crate::Conn::empty(opts);
            conn.inner.disconnected = true;
            conn.inner.ttl_deadline = Some(expired_deadline);
            let mut exchange = pool.inner.exchange.lock().unwrap();
            exchange.exist = 1;
            exchange.available.push_back(conn.into());
        }

        // `check_ttl` removes the expired connection from `available` and spawns a task that
        // disconnects it and releases the slot.
        let ttl_check =
            super::ttl_check_inerval::TtlCheckInterval::new(pool_opts, pool.inner.clone());
        ttl_check.check_ttl();
        assert_eq!(ex_field!(pool, available).len(), 0);
        assert_eq!(ex_field!(pool, exist), 1);

        let woken = Arc::new(OnceLock::new());
        let woken_clone = woken.clone();
        let waker = waker_fn(move || {
            let _ = woken_clone.set(());
        });
        let mut cx = Context::from_waker(&waker);

        let mut fut = pin!(pool.get_conn());
        assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(ex_field!(pool, waiting).len(), 1);

        // Wait for the spawned TTL task to release the slot and wake `fut`.
        timeout(Duration::from_millis(100), async {
            while woken.get().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("TTL slot release did not wake the waiting get_conn caller");

        assert_eq!(ex_field!(pool, exist), 0);
        assert_eq!(ex_field!(pool, waiting).len(), 0);
    }

    #[tokio::test]
    async fn pool_waitlist_disconnect_completes_when_external_release_frees_last_slot() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let pool = Pool::new(get_opts().pool_opts(pool_opts));
        let inner = pool.inner.clone();

        // Simulate an in-progress TTL disconnect or cancelled acquisition owning the last slot.
        ex_field!(pool, exist) = 1;

        let mut disconnect = pin!(pool.disconnect());
        let noop_waker = Waker::noop();
        let mut cx = Context::from_waker(noop_waker);

        // Start shutdown and let the recycler process the close signal while `exist == 1`.
        assert!(matches!(disconnect.as_mut().poll(&mut cx), Poll::Pending));
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert!(!inner.closed.load(std::sync::atomic::Ordering::Acquire));

        // Release the last slot externally. This must wake the recycler so `disconnect` completes.
        inner.release_connection_slot();

        timeout(Duration::from_millis(100), disconnect)
            .await
            .expect("Pool::disconnect hung after external release of the last slot")
            .unwrap();
        assert!(inner.closed.load(std::sync::atomic::Ordering::Acquire));
    }

    #[tokio::test]
    async fn pool_waitlist_drop_pool_during_ttl_cleanup_does_not_panic_recycler() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let pool = Pool::new(get_opts().pool_opts(pool_opts));
        let inner = pool.inner.clone();

        // Spawn the recycler and hold `exist = 1` as if a TTL disconnect task owns the connection.
        {
            let mut exchange = inner.exchange.lock().unwrap();
            exchange.spawn_futures_if_needed(&inner);
            exchange.exist = 1;
        }

        // Drop the last `Pool` handle so the recycler receives channel EOF while `exist == 1`.
        drop(pool);
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert!(
            !inner.closed.load(std::sync::atomic::Ordering::Acquire),
            "recycler must wait for outstanding TTL slots before exiting"
        );

        // Finish the TTL release. The recycler must wake and exit cleanly without panicking.
        inner.release_connection_slot();
        timeout(Duration::from_millis(100), async {
            while !inner.closed.load(std::sync::atomic::Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recycler did not exit after TTL released the last slot");
    }

    #[tokio::test]
    async fn pool_waitlist_disconnect_wakes_parked_waiters() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let pool = Pool::new(get_opts().pool_opts(pool_opts));
        let inner = pool.inner.clone();

        // Occupy the single connection slot so `fut1` parks in `exchange.waiting`.
        ex_field!(pool, exist) = 1;

        let woken = Arc::new(OnceLock::new());
        let woken_clone = woken.clone();
        let waker = waker_fn(move || {
            let _ = woken_clone.set(());
        });
        let mut cx = Context::from_waker(&waker);

        let mut fut1 = pin!(pool.get_conn());
        assert!(matches!(fut1.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(ex_field!(pool, waiting).len(), 1);

        // Start shutdown while `exist == 1`. `disconnect` must wake `fut1` immediately.
        let mut disconnect = pin!(pool.disconnect());
        let noop_waker = Waker::noop();
        let mut disc_cx = Context::from_waker(noop_waker);
        assert!(matches!(
            disconnect.as_mut().poll(&mut disc_cx),
            Poll::Pending
        ));
        assert!(
            woken.get().is_some(),
            "starting Pool::disconnect did not wake already-parked waiters"
        );

        // Polling `fut1` after wakeup returns `PoolDisconnected` while `exist` is still 1.
        assert!(matches!(
            fut1.as_mut().poll(&mut cx),
            Poll::Ready(Err(crate::Error::Driver(
                crate::DriverError::PoolDisconnected
            )))
        ));
        assert_eq!(
            inner
                .metrics
                .active_wait_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // Release the occupied slot so shutdown finishes cleanly.
        inner.release_connection_slot();
        timeout(Duration::from_millis(100), disconnect)
            .await
            .expect("Pool::disconnect did not finish after releasing the slot")
            .unwrap();
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

            let pool_constraints = PoolConstraints::new(0, 1).unwrap();
            let pool_opts = PoolOpts::default()
                .with_constraints(pool_constraints)
                .with_inactive_connection_ttl(Duration::from_secs(1));

            let pool = Pool::new(get_opts().pool_opts(pool_opts));

            bencher.iter(|| {
                let fut = pool.get_conn().map(drop);
                runtime.block_on(fut);
            });

            runtime.block_on(pool.disconnect()).unwrap();
        }
    }
}
