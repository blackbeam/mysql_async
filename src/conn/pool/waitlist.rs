use keyed_priority_queue::KeyedPriorityQueue;

use std::{
    borrow::Borrow,
    cmp::Reverse,
    collections::BTreeSet,
    hash::{Hash, Hasher},
    ops::Bound,
    sync::atomic,
    sync::Arc,
    task::Waker,
};

use crate::Metrics;

#[derive(Debug)]
pub(crate) struct Waitlist {
    queue: KeyedPriorityQueue<QueuedWaker, QueueId>,
    woken: BTreeSet<QueueId>,
    metrics: Arc<Metrics>,
}

impl Waitlist {
    pub(crate) fn new(metrics: Arc<Metrics>) -> Waitlist {
        Waitlist {
            queue: Default::default(),
            woken: Default::default(),
            metrics,
        }
    }

    pub(crate) fn push(&mut self, waker: Waker, queue_id: QueueId) {
        // The documentation of Future::poll says:
        //   Note that on multiple calls to poll, only the Waker from
        //   the Context passed to the most recent call should be
        //   scheduled to receive a wakeup.
        //
        // But the the documentation of KeyedPriorityQueue::push says:
        //   Adds new element to queue if missing key or replace its
        //   priority if key exists. In second case doesn’t replace key.
        //
        // This means we have to remove first to have the most recent
        // waker in the queue.
        self.woken.remove(&queue_id);
        self.remove(queue_id);
        self.queue.push(QueuedWaker { queue_id, waker }, queue_id);

        self.metrics
            .active_wait_requests
            .fetch_add(1, atomic::Ordering::Relaxed);
    }

    /// Removes and returns the waker of the highest-priority waiter.
    ///
    /// Tracks the popped ID until that waiter polls again or cancels.
    /// Call `Waker::wake` after you release the `exchange` mutex.
    pub(crate) fn pop_waker(&mut self) -> Option<Waker> {
        let (qw, id) = self.queue.pop()?;
        self.metrics
            .active_wait_requests
            .fetch_sub(1, atomic::Ordering::Relaxed);
        self.woken.insert(id);
        Some(qw.waker)
    }

    /// Moves all queued wakers into `out` and clears tracked woken IDs.
    ///
    /// Call `Waker::wake` on each entry after you release the `exchange` mutex.
    pub(crate) fn drain_wakers(&mut self, out: &mut Vec<Waker>) {
        while let Some((qw, _)) = self.queue.pop() {
            self.metrics
                .active_wait_requests
                .fetch_sub(1, atomic::Ordering::Relaxed);
            out.push(qw.waker);
        }
        self.woken.clear();
    }

    /// Clears a previously popped waiter ID and returns `true` if it was tracked.
    pub(crate) fn clear_woken(&mut self, id: QueueId) -> bool {
        self.woken.remove(&id)
    }

    /// Returns `true` if fewer than `limit` popped waiters outrank the current queue head.
    ///
    /// Returns immediately when `limit == 0` or the queue is empty, and stops once `limit`
    /// eligible waiters are found.
    pub(crate) fn has_fewer_eligible_woken(&mut self, limit: usize) -> bool {
        if limit == 0 || self.queue.is_empty() {
            return false;
        }
        if self.woken.len() < limit {
            return true;
        }
        match self.peek_id() {
            Some(head) => {
                self.woken
                    .range((Bound::Excluded(head), Bound::Unbounded))
                    .take(limit)
                    .count()
                    < limit
            }
            None => false,
        }
    }

    /// Removes a waiter by `id` and returns `true` if the waiter was in the queue.
    pub(crate) fn remove(&mut self, id: QueueId) -> bool {
        if self.queue.remove(&id).is_some() {
            self.metrics
                .active_wait_requests
                .fetch_sub(1, atomic::Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Returns the number of queued waiters plus popped waiters that have not polled again.
    pub(crate) fn demand(&self) -> usize {
        self.queue.len() + self.woken.len()
    }

    pub(crate) fn peek_id(&mut self) -> Option<QueueId> {
        self.queue.peek().map(|(qw, _)| qw.queue_id)
    }

    // only used in tests for now
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
pub(crate) const QUEUE_END_ID: QueueId = QueueId(Reverse(u64::MAX));
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct QueueId(Reverse<u64>);

impl QueueId {
    pub(crate) fn next() -> Self {
        static NEXT_QUEUE_ID: atomic::AtomicU64 = atomic::AtomicU64::new(0);
        let id = NEXT_QUEUE_ID.fetch_add(1, atomic::Ordering::SeqCst);
        QueueId(Reverse(id))
    }
}

#[derive(Debug)]
struct QueuedWaker {
    queue_id: QueueId,
    waker: Waker,
}

impl Eq for QueuedWaker {}

impl Borrow<QueueId> for QueuedWaker {
    fn borrow(&self) -> &QueueId {
        &self.queue_id
    }
}

impl PartialEq for QueuedWaker {
    fn eq(&self, other: &Self) -> bool {
        self.queue_id == other.queue_id
    }
}

impl Hash for QueuedWaker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.queue_id.hash(state)
    }
}

#[cfg(test)]
mod test {
    use std::cmp::Reverse;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Waker;

    use super::*;

    #[test]
    fn waitlist_integrity() {
        const DATA: *const () = &();
        const NOOP_CLONE_FN: unsafe fn(*const ()) -> RawWaker = |_| RawWaker::new(DATA, &RW_VTABLE);
        const NOOP_FN: unsafe fn(*const ()) = |_| {};
        static RW_VTABLE: RawWakerVTable =
            RawWakerVTable::new(NOOP_CLONE_FN, NOOP_FN, NOOP_FN, NOOP_FN);
        let w = unsafe { Waker::from_raw(RawWaker::new(DATA, &RW_VTABLE)) };

        let mut waitlist = Waitlist::new(Default::default());
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
}
