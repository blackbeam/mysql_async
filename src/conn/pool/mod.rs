// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use futures_core::{ready, stream::Stream};
use futures_util::stream::futures_unordered::FuturesUnordered;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use tokio::sync::mpsc;

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
    BoxFuture,
};

// this is a really unfortunate name for a module
pub mod futures;

struct Recycler {
    inner: Arc<Inner>,
    discard: FuturesUnordered<BoxFuture<()>>,
    discarded: usize,
    cleaning: FuturesUnordered<BoxFuture<Conn>>,

    // Option<Conn> so that we have a way to send a "I didn't make a Conn after all" signal
    dropped: mpsc::UnboundedReceiver<Option<Conn>>,
    min: usize,
    eof: bool,
}

impl Future for Recycler {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut readied = 0;
        let mut close = self.inner.close.load(atomic::Ordering::Acquire);

        macro_rules! conn_decision {
            ($self:ident, $readied:ident, $conn:ident) => {
                if $conn.inner.stream.is_none() || $conn.inner.disconnected {
                    // drop unestablished connection
                    $self.discard.push(Box::pin(::futures_util::future::ok(())));
                } else if $conn.inner.in_transaction || $conn.inner.has_result.is_some() {
                    $self.cleaning.push(Box::pin($conn.cleanup()));
                } else if $conn.expired() || $self.inner.idle.len() >= $self.min || close {
                    $self.discard.push(Box::pin($conn.close()));
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

        while !self.eof {
            // see if there are more connections for us to recycle
            match Pin::new(&mut self.dropped).poll_next(cx) {
                Poll::Ready(Some(Some(conn))) => {
                    conn_decision!(self, readied, conn);
                }
                Poll::Ready(Some(None)) => {
                    // someone signaled us that it's exit time
                    close = self.inner.close.load(atomic::Ordering::Acquire);
                    assert!(close);
                    continue;
                }
                Poll::Ready(None) => {
                    // no more connections are coming -- time to exit!
                    self.inner.close.store(true, atomic::Ordering::Release);
                    self.eof = true;
                    close = true;
                }
                Poll::Pending => {
                    // nope -- but let's still make progress on the ones we have
                    break;
                }
            }
        }

        // if we've been asked to close, reclaim any idle connections
        if close {
            while let Ok(conn) = self.inner.idle.pop() {
                conn_decision!(self, readied, conn);
            }
        }

        // are any dirty connections ready for us to reclaim?
        loop {
            match Pin::new(&mut self.cleaning).poll_next(cx) {
                Poll::Pending | Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(conn))) => conn_decision!(self, readied, conn),
                Poll::Ready(Some(Err(e))) => {
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
            match Pin::new(&mut self.discard).poll_next(cx) {
                Poll::Pending | Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(()))) => {
                    // yes! count it.
                    self.discarded += 1
                }
                Poll::Ready(Some(Err(e))) => {
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

        // NOTE: we are asserting here that no more connections will ever be returned to
        // us. see the explanation in Pool::poll_new_conn for why this is okay, even during
        // races on .exist
        let effectively_eof = close && self.inner.exist.load(atomic::Ordering::Acquire) == 0;

        if (self.eof || effectively_eof) && self.cleaning.is_empty() && self.discard.is_empty() {
            // we know that all Pool handles have been dropped (self.dropped.poll returned None).

            // if this assertion fails, where are the remaining connections?
            assert_eq!(self.inner.idle.len(), 0);
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
            self.inner.closed.store(true, atomic::Ordering::Release);
        }

        self.inner.wake(readied);

        if self.inner.closed.load(atomic::Ordering::Acquire) {
            // since there are no more Pools, we also know that no-one is waiting anymore,
            // so we don't have to worry about calling wake more times
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct Inner {
    close: atomic::AtomicBool,
    closed: atomic::AtomicBool,
    idle: crossbeam::queue::ArrayQueue<Conn>,
    wake: crossbeam::queue::SegQueue<Waker>,
    exist: atomic::AtomicUsize,
    extra_wakeups: atomic::AtomicUsize,

    // only used to spawn the recycler the first time we're in async context
    maker: Mutex<Option<mpsc::UnboundedReceiver<Option<Conn>>>>,
}

impl Inner {
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
    pool_constraints: PoolConstraints,
    drop: mpsc::UnboundedSender<Option<Conn>>,
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
            && self.inner.idle.len() < self.pool_constraints.min()
        {
            self.inner
                .idle
                .push(conn)
                .expect("more connections than max");
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
                Ok(conn) => {
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
                        eof: false,
                    });
                }
            }

            if exist < self.pool_constraints.max() {
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
            if exist < self.pool_constraints.max() {
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

    use crate::{
        conn::pool::Pool, queryable::Queryable, test_misc::DATABASE_URL, TransactionOptions,
    };

    #[tokio::test]
    async fn should_connect() {
        let pool = Pool::new(&**DATABASE_URL);
        pool.get_conn().await.unwrap().ping().await.unwrap();
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn can_handle_the_pressure() {
        let pool = Pool::new(&**DATABASE_URL);
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
    async fn should_start_transaction() {
        let pool = Pool::new(format!("{}?pool_min=1&pool_max=1", &**DATABASE_URL));
        pool.get_conn()
            .await
            .unwrap()
            .drop_query("CREATE TABLE IF NOT EXISTS tmp(id int)")
            .await
            .unwrap();
        let _ = pool
            .start_transaction(TransactionOptions::default())
            .await
            .unwrap()
            .batch_exec("INSERT INTO tmp (id) VALUES (?)", vec![(1,), (2,)])
            .await
            .unwrap()
            .prep_exec("SELECT * FROM tmp", ())
            .await
            .unwrap();
        let row_opt = pool
            .get_conn()
            .await
            .unwrap()
            .first("SELECT COUNT(*) FROM tmp")
            .await
            .unwrap()
            .1;
        assert_eq!(row_opt, Some((0u8,)));
        pool.get_conn()
            .await
            .unwrap()
            .drop_query("DROP TABLE tmp")
            .await
            .unwrap();
        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn should_hold_bounds2() {
        use std::cmp::min;

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

        let mut conns = futures_util::try_future::try_join_all(conns).await.unwrap();

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
            tokio::timer::delay(std::time::Instant::now() + std::time::Duration::from_millis(100))
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
            let have = pool_clone.inner.exist.load(atomic::Ordering::SeqCst);
            assert_eq!(have, expected);
        }

        pool.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn should_hold_bounds1() {
        let pool = Pool::new(format!("{}?pool_min=1&pool_max=2", &**DATABASE_URL));
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
        let conn1 = pool_clone.get_conn().await.unwrap();
        assert_eq!(conn1.inner.pool.as_ref().unwrap().inner.idle.len(), 0);

        drop(conn1);

        // the connection should be returned to idle
        // (but may not have been returned _yet_)
        assert!(pool.inner.idle.len() <= 1);
        pool.disconnect().await.unwrap();
    }

    // Test that connections which err do not count towards the connection count in the pool.
    #[tokio::test]
    async fn should_hold_bounds_on_error() {
        // Should not be possible to connect to broadcast address.
        let pool = Pool::new(String::from("mysql://255.255.255.255"));

        assert!(
            futures_util::try_future::try_join(pool.get_conn(), pool.get_conn())
                .await
                .is_err()
        );
        assert_eq!(pool.inner.exist.load(atomic::Ordering::SeqCst), 0);
    }

    /*
    #[test]
    fn should_hold_bounds_on_get_conn_drop() {
        let pool = Pool::new(format!("{}?pool_min=1&pool_max=2", &**DATABASE_URL));
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
    async fn droptest() {
        let pool = Pool::new(&**DATABASE_URL);
        let conns = futures_util::try_future::try_join_all((0..10).map(|_| pool.get_conn()))
            .await
            .unwrap();
        drop(conns);
        drop(pool);
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
        let pool = Pool::new(&**DATABASE_URL);
        run(collect(
            (0..10).map(|_| pool.get_conn()).collect::<Vec<_>>(),
        ))
        .unwrap();
        // pool will drop here
    }
    */

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
