use std::marker::PhantomData;

use futures_core::future::BoxFuture;
use futures_util::FutureExt;

use crate::{queryable::Protocol, Conn};

use super::Routine;

/// A routine that handles subsequent result of a mutlti-result set.
#[derive(Debug, Clone, Copy)]
pub struct NextSetRoutine<P>(PhantomData<P>);

impl<P> NextSetRoutine<P> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<P> Routine<()> for NextSetRoutine<P>
where
    P: Protocol,
{
    fn call<'a>(&'a mut self, conn: &'a mut Conn) -> BoxFuture<'a, crate::Result<()>> {
        conn.sync_seq_id();
        async move {
            conn.read_result_set::<P>(false).await?;
            Ok(())
        }
        .boxed()
    }
}
