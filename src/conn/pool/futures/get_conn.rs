// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{fmt, future::poll_fn, sync::atomic::Ordering, task::Context};

use crate::{
    conn::{
        pool::{waitlist::QueueId, Pool},
        Conn,
    },
    error::*,
};

/// States of the GetConn future.
pub(crate) enum GetConnInner {
    New,
    Done,
    // TODO: one day this should be an existential
    Connecting(crate::BoxFuture<'static, Conn>),
    /// This future will check, that idling connection is alive.
    Checking(crate::BoxFuture<'static, Conn>),
}

impl fmt::Debug for GetConnInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GetConnInner::New => f.debug_tuple("GetConnInner::New").finish(),
            GetConnInner::Done => f.debug_tuple("GetConnInner::Done").finish(),
            GetConnInner::Connecting(_) => f
                .debug_tuple("GetConnInner::Connecting")
                .field(&"<future>")
                .finish(),
            GetConnInner::Checking(_) => f
                .debug_tuple("GetConnInner::Checking")
                .field(&"<future>")
                .finish(),
        }
    }
}

#[derive(Debug)]
struct GetConnState {
    queue_id: QueueId,
    pool: Option<Pool>,
    inner: GetConnInner,
}

impl GetConnState {
    fn pool_mut(&mut self) -> &mut Pool {
        self.pool
            .as_mut()
            .expect("GetConn::poll polled after returning Async::Ready")
    }

    fn pool_take(&mut self) -> Pool {
        self.pool
            .take()
            .expect("GetConn::poll polled after returning Async::Ready")
    }

    /// Prepares the connection to be delivered to the application.
    fn ready_connection(&mut self, mut c: Conn) -> Result<Conn> {
        let pool = self.pool_take();
        if pool.inner.close.load(Ordering::Acquire) {
            // Reject connections that finish connecting or checking after shutdown starts.
            pool.send_to_recycler(c);
            return Err(Error::Driver(DriverError::PoolDisconnected));
        }
        pool.inner
            .metrics
            .connections_in_use
            .fetch_add(1, Ordering::Relaxed);

        c.inner.reset_upon_returning_to_a_pool = pool.opts.pool_opts().reset_connection();
        c.inner.pool = Some(pool);

        Ok(c)
    }
}

/// This future will take connection from a pool and resolve to [`Conn`].
#[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all))]
pub(crate) async fn get_conn(pool: Pool) -> Result<Conn> {
    let queue_id = QueueId::next();
    let mut state = GetConnState {
        queue_id,
        pool: Some(pool),
        inner: GetConnInner::New,
    };

    loop {
        match state.inner {
            GetConnInner::New => {
                let pool = state.pool_mut();
                let poll_new = |cx: &mut Context<'_>| pool.poll_new_conn(cx, queue_id);
                let next = poll_fn(poll_new).await?;
                match next {
                    GetConnInner::Connecting(conn_fut) => {
                        state.inner = GetConnInner::Connecting(conn_fut);
                    }
                    GetConnInner::Checking(conn_fut) => {
                        state.inner = GetConnInner::Checking(conn_fut);
                    }
                    GetConnInner::Done => unreachable!(
                        "Pool::poll_new_conn never gives out already-consumed GetConns"
                    ),
                    GetConnInner::New => {
                        unreachable!("Pool::poll_new_conn never gives out GetConnInner::New")
                    }
                }
            }
            GetConnInner::Done => {
                unreachable!("GetConn::poll polled after returning Async::Ready");
            }
            GetConnInner::Connecting(ref mut f) => {
                let result = f.await;
                state.inner = GetConnInner::Done;

                return match result {
                    Ok(c) => state.ready_connection(c),
                    Err(e) => {
                        let pool = state.pool_take();
                        pool.cancel_connection();
                        Err(e)
                    }
                };
            }
            GetConnInner::Checking(ref mut f) => {
                let result = f.await;
                match result {
                    Ok(c) => {
                        state.inner = GetConnInner::Done;

                        return state.ready_connection(c);
                    }
                    Err(_) => {
                        // Idling connection is broken. We'll drop it and try again.
                        state.inner = GetConnInner::New;

                        let pool = state.pool_mut();
                        pool.cancel_connection();
                        continue;
                    }
                }
            }
        }
    }
}

impl Drop for GetConnState {
    fn drop(&mut self) {
        // Keep pool state consistent when a caller drops this future before completion.
        if let Some(pool) = self.pool.take() {
            match self.inner {
                // Remove this waiter from the waitlist and pass its wakeup on if needed.
                GetConnInner::New => pool.unqueue(self.queue_id),
                GetConnInner::Connecting(..) => {
                    // Drop the connecting future first so its socket closes synchronously
                    // before `cancel_connection` releases the pool slot.
                    self.inner = GetConnInner::Done;
                    pool.cancel_connection();
                }
                // `CheckingConnGuard` inside the inner future closes the socket and releases
                // the slot when `self.inner` drops.
                GetConnInner::Checking(..) | GetConnInner::Done => {}
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        pin::pin,
        sync::atomic::Ordering,
        task::{Context, Poll, Waker},
        time::Duration,
    };

    use futures_util::Future;
    use tokio::time::timeout;

    use super::{GetConnInner, GetConnState};
    use crate::{
        conn::pool::waitlist::QueueId, test_misc::get_opts, Conn, DriverError, Error, Opts, Pool,
        PoolConstraints, PoolOpts,
    };

    #[tokio::test]
    async fn pool_waitlist_disconnect_rejects_connecting_ready_connection() {
        let pool_opts = PoolOpts::new().with_constraints(PoolConstraints::new(0, 1).unwrap());
        let opts = Opts::from(get_opts().pool_opts(pool_opts));
        let pool = Pool::new(opts.clone());
        let inner = pool.inner.clone();

        // Simulate an in-flight `Connecting` acquisition holding the single slot.
        {
            let mut exchange = inner.exchange.lock().unwrap();
            exchange.exist = 1;
            pool.metrics().connection_count.store(1, Ordering::Relaxed);
        }

        let mut state = GetConnState {
            queue_id: QueueId::next(),
            pool: Some(pool.clone()),
            inner: GetConnInner::Done,
        };

        // Start shutdown while the connection is still establishing.
        let mut disconnect = pin!(pool.disconnect());
        let noop_waker = Waker::noop();
        let mut cx = Context::from_waker(noop_waker);
        assert!(matches!(disconnect.as_mut().poll(&mut cx), Poll::Pending));
        assert!(inner.close.load(Ordering::Acquire));

        // Deliver the established connection after shutdown started. It must be rejected
        // and sent to the recycler so shutdown completes.
        let mut conn = Conn::empty(opts);
        conn.inner.disconnected = true;
        assert!(matches!(
            state.ready_connection(conn),
            Err(Error::Driver(DriverError::PoolDisconnected))
        ));
        assert_eq!(inner.metrics.connections_in_use.load(Ordering::Relaxed), 0);

        timeout(Duration::from_millis(100), disconnect)
            .await
            .expect("Pool::disconnect did not finish after rejecting an in-flight connection")
            .unwrap();
        assert!(inner.closed.load(Ordering::Acquire));
    }
}
