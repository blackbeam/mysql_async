// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_util::FutureExt;
use priority_queue::PriorityQueue;
use tokio::sync::mpsc;

use std::{
    cmp::{Ordering, Reverse},
    collections::VecDeque,
    convert::TryFrom,
    hash::{Hash, Hasher},
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
    queryable::transaction::{Transaction, TxOpts, TxStatus},
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
    waiting: Waitlist,
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

#[derive(Default, Debug)]
struct Waitlist {
    queue: PriorityQueue<QueuedWaker, QueueId>,
}

impl Waitlist {
    fn push(&mut self, w: Waker, queue_id: QueueId) {
        self.queue.push(
            QueuedWaker {
                queue_id,
                waker: Some(w),
            },
            queue_id,
        );
    }

    fn pop(&mut self) -> Option<Waker> {
        match self.queue.pop() {
            Some((qw, _)) => Some(qw.waker.unwrap()),
            None => None,
        }
    }

    fn remove(&mut self, id: QueueId) {
        let tmp = QueuedWaker {
            queue_id: id,
            waker: None,
        };
        self.queue.remove(&tmp);
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

const QUEUE_END_ID: QueueId = QueueId(Reverse(u64::MAX));

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct QueueId(Reverse<u64>);

impl QueueId {
    fn next() -> Self {
        static NEXT_QUEUE_ID: atomic::AtomicU64 = atomic::AtomicU64::new(0);
        let id = NEXT_QUEUE_ID.fetch_add(1, atomic::Ordering::SeqCst);
        QueueId(Reverse(id))
    }
}

#[derive(Debug)]
struct QueuedWaker {
    queue_id: QueueId,
    waker: Option<Waker>,
}

impl Eq for QueuedWaker {}

impl PartialEq for QueuedWaker {
    fn eq(&self, other: &Self) -> bool {
        self.queue_id == other.queue_id
    }
}

impl Ord for QueuedWaker {
    fn cmp(&self, other: &Self) -> Ordering {
        self.queue_id.cmp(&other.queue_id)
    }
}

impl PartialOrd for QueuedWaker {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for QueuedWaker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.queue_id.hash(state)
    }
}

/// Connection pool data.
#[derive(Debug)]
pub struct Inner {
    close: atomic::AtomicBool,
    closed: atomic::AtomicBool,
    exchange: Mutex<Exchange>,
}

/// Asynchronous pool of MySql connections.
///
/// Actually `Pool` is a shared reference, i.e. every clone will lead to the same instance
/// created with [`Pool::new`]. Also `Pool` satisfies `Send` and `Sync`, so you don't have to wrap
/// it into an `Arc` or `Mutex`.
///
/// Note that you will probably want to await [`Pool::disconnect`] before dropping the runtime, as
/// otherwise you may end up with a number of connections that are not cleanly terminated.
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
        let (tx, rx) = mpsc::unbounded_channel();
        Pool {
            opts,
            inner: Arc::new(Inner {
                close: false.into(),
                closed: false.into(),
                exchange: Mutex::new(Exchange {
                    available: VecDeque::with_capacity(pool_opts.constraints().max()),
                    waiting: Waitlist::default(),
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

    /// Async function that resolves to `Conn`.
    pub fn get_conn(&self) -> GetConn {
        GetConn::new(self)
    }

    /// Starts a new transaction.
    pub async fn start_transaction(&self, options: TxOpts) -> Result<Transaction<'static>> {
        let conn = self.get_conn().await?;
        Transaction::new(conn, options).await
    }

    /// Async function that disconnects this pool from the server and resolves to `()`.
    ///
    /// **Note:** This Future won't resolve until all active connections, taken from it,
    /// are dropped or disonnected. Also all pending and new `GetConn`'s will resolve to error.
    pub fn disconnect(self) -> DisconnectPool {
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
            && !conn.has_pending_result()
            && !self.inner.close.load(atomic::Ordering::Acquire)
        {
            let mut exchange = self.inner.exchange.lock().unwrap();
            if exchange.available.len() < self.opts.pool_opts().active_bound() {
                exchange.available.push_back(conn.into());
                if let Some(w) = exchange.waiting.pop() {
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
        if let Some(w) = exchange.waiting.pop() {
            w.wake();
        }
    }

    /// Poll the pool for an available connection.
    fn poll_new_conn(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        queued: bool,
        queue_id: QueueId,
    ) -> Poll<Result<GetConnInner>> {
        self.poll_new_conn_inner(cx, queued, queue_id)
    }

    fn poll_new_conn_inner(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        queued: bool,
        queue_id: QueueId,
    ) -> Poll<Result<GetConnInner>> {
        let mut exchange = self.inner.exchange.lock().unwrap();

        // NOTE: this load must happen while we hold the lock,
        // otherwise the recycler may choose to exit, see that .exist == 0, and then exit,
        // and then we decide to create a new connection, which would then never be torn down.
        if self.inner.close.load(atomic::Ordering::Acquire) {
            return Err(Error::Driver(DriverError::PoolDisconnected)).into();
        }

        exchange.spawn_futures_if_needed(&self.inner);

        // Check if others are waiting and we're not queued.
        if !exchange.waiting.is_empty() && !queued {
            exchange.waiting.push(cx.waker().clone(), queue_id);
            return Poll::Pending;
        }

        while let Some(IdlingConn { mut conn, .. }) = exchange.available.pop_back() {
            if !conn.expired() {
                return Poll::Ready(Ok(GetConnInner::Checking(
                    async move {
                        conn.stream_mut()?.check().await?;
                        Ok(conn)
                    }
                    .boxed(),
                )));
            } else {
                self.send_to_recycler(conn);
            }
        }

        // we didn't _immediately_ get one -- try to make one
        // we first try to just do a load so we don't do an unnecessary add then sub
        if exchange.exist < self.opts.pool_opts().constraints().max() {
            // we are allowed to make a new connection, so we will!
            exchange.exist += 1;

            return Poll::Ready(Ok(GetConnInner::Connecting(
                Conn::new(self.opts.clone()).boxed(),
            )));
        }

        // Polled, but no conn available? Back into the queue.
        exchange.waiting.push(cx.waker().clone(), queue_id);
        Poll::Pending
    }

    fn unqueue(&self, queue_id: QueueId) {
        let mut exchange = self.inner.exchange.lock().unwrap();
        exchange.waiting.remove(queue_id);
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.inner.infile_handler = None;

        if std::thread::panicking() {
            // Try to decrease the number of existing connections.
            if let Some(pool) = self.inner.pool.take() {
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
        future::{join_all, select, select_all, try_join_all},
        try_join, FutureExt,
    };
    use mysql_common::row::Row;
    use tokio::time::{sleep, timeout};

    use std::{
        cmp::Reverse,
        task::{RawWaker, RawWakerVTable, Waker},
        time::Duration,
    };

    use crate::{
        conn::pool::{Pool, QueueId, Waitlist, QUEUE_END_ID},
        opts::PoolOpts,
        prelude::*,
        test_misc::get_opts,
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

            // get_conn should work if connection is available and alive
            pool.get_conn().await?;

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
        let constraints = PoolConstraints::new(1, 1).unwrap();
        let opts = get_opts().pool_opts(PoolOpts::default().with_constraints(constraints));

        let pool = Pool::new(opts);
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
            while let Some(_) = rx.recv().await {}
        }
        drop(pool);
    }

    #[tokio::test]
    async fn should_start_transaction() -> super::Result<()> {
        let constraints = PoolConstraints::new(1, 1).unwrap();
        let opts = get_opts().pool_opts(PoolOpts::default().with_constraints(constraints));

        let pool = Pool::new(opts);

        "CREATE TEMPORARY TABLE tmp(id int)".ignore(&pool).await?;

        let mut tx = pool.start_transaction(TxOpts::default()).await?;
        tx.exec_batch("INSERT INTO tmp (id) VALUES (?)", vec![(1_u8,), (2_u8,)])
            .await?;
        tx.exec_drop("SELECT * FROM tmp", ()).await?;
        drop(tx);
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
            sleep(Duration::from_millis(50)).await;

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
        let pool_constraints = PoolConstraints::new(1, 1).unwrap();
        let pool_opts = PoolOpts::default().with_constraints(pool_constraints);

        let pool = Pool::new(get_opts().pool_opts(pool_opts));
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
        let pool_constraints = PoolConstraints::new(1, 1).unwrap();
        let pool_opts = PoolOpts::default().with_constraints(pool_constraints);

        let pool = Pool::new(get_opts().pool_opts(pool_opts));
        let only_conn = pool.get_conn().await.unwrap();

        let join_handle = tokio::spawn(timeout(Duration::from_secs(1), pool.get_conn()));

        sleep(Duration::from_secs(2)).await;

        match join_handle.await.unwrap() {
            Err(_elapsed) => (),
            _ => panic!("unexpected Ok()"),
        }
        drop(only_conn);

        assert_eq!(0, pool.inner.exchange.lock().unwrap().waiting.queue.len());
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

    #[test]
    fn waitlist_integrity() {
        const DATA: *const () = &();
        const NOOP_CLONE_FN: unsafe fn(*const ()) -> RawWaker = |_| RawWaker::new(DATA, &RW_VTABLE);
        const NOOP_FN: unsafe fn(*const ()) = |_| {};
        static RW_VTABLE: RawWakerVTable =
            RawWakerVTable::new(NOOP_CLONE_FN, NOOP_FN, NOOP_FN, NOOP_FN);
        let w = unsafe { Waker::from_raw(RawWaker::new(DATA, &RW_VTABLE)) };

        let mut waitlist = Waitlist::default();
        assert_eq!(0, waitlist.queue.len());

        waitlist.push(w.clone(), QueueId(Reverse(4)));
        waitlist.push(w.clone(), QueueId(Reverse(2)));
        waitlist.push(w.clone(), QueueId(Reverse(8)));
        waitlist.push(w.clone(), QUEUE_END_ID);
        waitlist.push(w.clone(), QueueId(Reverse(10)));

        waitlist.remove(QueueId(Reverse(8)));

        assert_eq!(4, waitlist.queue.len());

        let (_, id) = waitlist.queue.pop().unwrap();
        assert_eq!(2, id.0 .0);
        let (_, id) = waitlist.queue.pop().unwrap();
        assert_eq!(4, id.0 .0);
        let (_, id) = waitlist.queue.pop().unwrap();
        assert_eq!(10, id.0 .0);
        let (_, id) = waitlist.queue.pop().unwrap();
        assert_eq!(QUEUE_END_ID, id);

        assert_eq!(0, waitlist.queue.len());
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
