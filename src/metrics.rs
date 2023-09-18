use serde::Serialize;
#[cfg(feature = "metrics")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Default, Serialize)]
pub struct PoolMetrics {
    pub gets: Counter,
    pub reuses: Counter,
    pub direct_returnals: Counter,
    pub discards: Counter,

    pub(crate) recycler: RecyclerMetrics,
    pub(crate) conn: Arc<ConnMetrics>,
}

// Manually implemented Clone to clone ConnMetrics, not the Arc.
impl Clone for PoolMetrics {
    fn clone(&self) -> Self {
        PoolMetrics {
            gets: self.gets.clone(),
            reuses: self.reuses.clone(),
            direct_returnals: self.direct_returnals.clone(),
            discards: self.discards.clone(),
            recycler: self.recycler.clone(),
            conn: Arc::new(self.conn.as_ref().clone()),
        }
    }
}

impl PoolMetrics {
    #[inline]
    pub fn recycler(&self) -> &RecyclerMetrics {
        &self.recycler
    }

    #[inline]
    pub fn connections(&self) -> &ConnMetrics {
        &self.conn
    }
}

#[derive(Clone, Debug, Default, Serialize)]
#[non_exhaustive]
pub struct RecyclerMetrics {
    pub recycles: Counter,
    pub discards: Counter,
    pub cleans: Counter,
    pub recycled_returnals: Counter,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ConnMetrics {
    pub connects: Counter,
    pub reuses: Counter,
    pub disconnects: Counter,

    pub(crate) routines: RoutineMetrics,
    pub(crate) stmt_cache: StmtCacheMetrics,
}

impl ConnMetrics {
    #[inline]
    pub fn routines(&self) -> &RoutineMetrics {
        &self.routines
    }

    #[inline]
    pub fn stmt_cache(&self) -> &StmtCacheMetrics {
        &self.stmt_cache
    }
}

#[derive(Clone, Debug, Default, Serialize)]
#[non_exhaustive]
pub struct RoutineMetrics {
    pub change_user: Counter,
    pub queries: Counter,
    pub prepares: Counter,
    pub execs: Counter,
    pub pings: Counter,
    pub resets: Counter,
    pub next_sets: Counter,
}

#[derive(Clone, Debug, Default, Serialize)]
#[non_exhaustive]
pub struct StmtCacheMetrics {
    pub additions: Counter,
    pub drops: Counter,
    pub removals: Counter,
    pub evictions: Counter,
    pub resets: Counter,
    pub hits: Counter,
    pub misses: Counter,
}

#[derive(Clone, Debug, Default, Serialize)]
#[non_exhaustive]
pub struct BufferPoolMetrics {
    pub creations: Counter,
    pub reuses: Counter,
    pub returns: Counter,
    pub discards: Counter,
}

#[cfg(feature = "metrics")]
impl BufferPoolMetrics {
    #[inline]
    pub fn snapshot_global() -> Self {
        crate::BUFFER_POOL.snapshot_metrics()
    }
}

#[cfg(feature = "metrics")]
#[derive(Debug, Default, Serialize)]
pub struct Counter(AtomicUsize);

#[cfg(not(feature = "metrics"))]
#[derive(Clone, Debug, Default, Serialize)]
pub struct Counter;

impl Counter {
    #[cfg(feature = "metrics")]
    #[inline]
    pub fn value(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn incr(&self) {
        self.incr_by(1)
    }

    #[allow(unused_variables)]
    #[inline]
    pub(crate) fn incr_by(&self, count: usize) {
        #[cfg(feature = "metrics")]
        self.0.fetch_add(count, Ordering::Relaxed);
    }
}

#[cfg(feature = "metrics")]
impl Clone for Counter {
    fn clone(&self) -> Self {
        Counter(AtomicUsize::new(self.0.load(Ordering::Relaxed)))
    }
}
