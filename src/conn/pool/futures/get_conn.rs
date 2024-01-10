// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{fmt, future::poll_fn, task::Context};

use crate::{
    conn::{
        pool::{Pool, QueueId},
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

impl Drop for GetConnState {
    fn drop(&mut self) {
        // We drop a connection before it can be resolved, a.k.a. cancelling it.
        // Make sure we maintain the necessary invariants towards the pool.
        if let Some(pool) = self.pool.take() {
            // Remove the waker from the pool's waitlist in case this task was
            // woken by another waker, like from tokio::time::timeout.
            pool.unqueue(self.queue_id);
            if let GetConnInner::Connecting(..) = self.inner {
                pool.cancel_connection();
            }
        }
    }
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
}

/// This future will take connection from a pool and resolve to [`Conn`].
#[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all))]
pub(crate) async fn get_conn(pool: Pool) -> Result<Conn> {
    let reset_upon_returning_to_a_pool = pool.opts.pool_opts().reset_connection();
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
                let pool = state.pool_take();
                state.inner = GetConnInner::Done;

                return match result {
                    Ok(mut c) => {
                        c.inner.pool = Some(pool);
                        c.inner.reset_upon_returning_to_a_pool = reset_upon_returning_to_a_pool;
                        Ok(c)
                    }
                    Err(e) => {
                        pool.cancel_connection();
                        Err(e)
                    }
                };
            }
            GetConnInner::Checking(ref mut f) => {
                let result = f.await;
                match result {
                    Ok(mut c) => {
                        state.inner = GetConnInner::Done;

                        let pool = state.pool_take();
                        c.inner.pool = Some(pool);
                        c.inner.reset_upon_returning_to_a_pool = reset_upon_returning_to_a_pool;
                        return Ok(c);
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
