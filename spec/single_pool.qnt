// Single-pool semaphore model.
//
// Models the basic semaphore contract: a pool of permits that can be
// acquired and returned, with a bounded queue for waiters and a priority
// reduction mechanism for shrinking the pool while permits are in flight.
//
// This serves as a baseline spec that the multi-pool model must refine.

module single_pool {
    // -----------------------------------------------------------------------
    // Constants — fixed for a given model-checking run
    // -----------------------------------------------------------------------

    /// Total permits the semaphore starts with.
    const INITIAL_PERMITS: int

    /// Maximum number of waiters allowed in the queue.
    const MAX_QUEUE_SIZE: int

    // -----------------------------------------------------------------------
    // Types
    // -----------------------------------------------------------------------

    type State = {
        available: int,
        in_flight: int,
        priority_reduction: int,
        total_limit: int,
        queue: List[int],
        next_waiter_id: int,
    }

    // -----------------------------------------------------------------------
    // State variables
    // -----------------------------------------------------------------------

    var s: State

    // -----------------------------------------------------------------------
    // Utility functions
    // -----------------------------------------------------------------------

    pure def min(a: int, b: int): int = if (a < b) a else b

    // -----------------------------------------------------------------------
    // Initialisation
    // -----------------------------------------------------------------------

    action init = all {
        s' = {
            available: INITIAL_PERMITS,
            in_flight: 0,
            priority_reduction: 0,
            total_limit: INITIAL_PERMITS,
            queue: [],
            next_waiter_id: 0,
        },
    }

    // -----------------------------------------------------------------------
    // Invariants
    // -----------------------------------------------------------------------

    /// The fundamental conservation law.
    ///
    /// available + in_flight = the actual number of permits in existence.
    /// total_limit = the target number of permits.
    /// priority_reduction = how many in-flight permits are earmarked for
    ///   destruction on return (limit was decreased while they were out).
    ///
    /// So: actual permits = target + not-yet-consumed reductions
    val conservation =
        s.available + s.in_flight == s.total_limit + s.priority_reduction

    /// Nothing goes negative.
    val non_negative = and {
        s.available >= 0,
        s.in_flight >= 0,
        s.priority_reduction >= 0,
        s.total_limit >= 0,
    }

    /// Queue respects its bound.
    val queue_bounded =
        length(s.queue) <= MAX_QUEUE_SIZE

    /// All invariants combined.
    val inv = and {
        conservation,
        non_negative,
        queue_bounded,
    }

    // -----------------------------------------------------------------------
    // Actions
    // -----------------------------------------------------------------------

    /// Acquire a permit immediately, if one is available. No queueing.
    ///
    /// Guard: available > 0
    /// Effect: move one permit from available → in_flight
    action try_acquire = all {
        s.available > 0,
        s' = { ...s, available: s.available - 1, in_flight: s.in_flight + 1 },
    }

    /// Return a permit. Three possible outcomes, checked in order:
    ///
    /// 1. priority_reduction > 0 → consume the permit (it vanishes)
    /// 2. queue is non-empty     → wake the front waiter (permit stays in_flight)
    /// 3. otherwise              → return to available pool
    ///
    /// Guard: in_flight > 0 (you must hold a permit to return one)
    action return_permit = all {
        s.in_flight > 0,
        if (s.priority_reduction > 0) {
            // Case 1: consume the permit to satisfy a pending reduction.
            s' = { ...s,
                in_flight: s.in_flight - 1,
                priority_reduction: s.priority_reduction - 1,
            }
        } else if (length(s.queue) > 0) {
            // Case 2: wake the front waiter. The permit transfers directly
            // from the returner to the waiter — in_flight stays the same
            // (one holder out, one holder in).
            s' = { ...s, queue: tail(s.queue) }
        } else {
            // Case 3: no reduction pending, no waiters — return to pool.
            s' = { ...s,
                in_flight: s.in_flight - 1,
                available: s.available + 1,
            }
        },
    }

    /// A caller tries to acquire but no permit is available, so they join
    /// the queue to wait.
    ///
    /// Guard: available == 0 AND queue not full
    /// Effect: add a new waiter ID to the back of the queue
    action enqueue = all {
        s.available == 0,
        length(s.queue) < MAX_QUEUE_SIZE,
        s' = { ...s,
            queue: s.queue.append(s.next_waiter_id),
            next_waiter_id: s.next_waiter_id + 1,
        },
    }

    /// A queued waiter times out or is cancelled. They leave the queue
    /// without acquiring a permit.
    ///
    /// Any waiter in the queue can be cancelled (not just the front),
    /// since timeouts can expire in any order.
    ///
    /// Guard: queue is non-empty
    action cancel = all {
        length(s.queue) > 0,
        nondet idx = oneOf(indices(s.queue))
        s' = { ...s,
            queue: slice(s.queue, 0, idx).concat(
                slice(s.queue, idx + 1, length(s.queue))
            ),
        },
    }

    /// Change the total permit limit. Unifies increasing and decreasing
    /// the limit into a single set_limit(new_total) operation.
    ///
    /// When increasing (delta > 0):
    ///   1. Cancel pending reductions (up to delta)
    ///   2. Wake queued waiters (up to remaining delta)
    ///   3. Add remainder to available
    ///
    /// When decreasing (delta < 0):
    ///   1. Consume from available (up to |delta|)
    ///   2. Remainder becomes priority_reduction
    action set_limit = {
        // Non-deterministically pick a new limit. Bounded to keep
        // the state space finite for model checking.
        nondet new_total = oneOf(0.to(2 * INITIAL_PERMITS))
        val delta = new_total - s.total_limit
        if (delta > 0) {
            val cancel_reductions = min(delta, s.priority_reduction)
            val after_cancel = delta - cancel_reductions
            val wake_waiters = min(after_cancel, length(s.queue))
            val add_to_available = after_cancel - wake_waiters
            s' = { ...s,
                total_limit: new_total,
                priority_reduction: s.priority_reduction - cancel_reductions,
                in_flight: s.in_flight + wake_waiters,
                queue: slice(s.queue, wake_waiters, length(s.queue)),
                available: s.available + add_to_available,
            }
        } else if (delta < 0) {
            val abs_delta = -delta
            val consume_available = min(abs_delta, s.available)
            val new_reductions = abs_delta - consume_available
            s' = { ...s,
                total_limit: new_total,
                available: s.available - consume_available,
                priority_reduction: s.priority_reduction + new_reductions,
            }
        } else {
            s' = s
        }
    }

    /// The next-state relation: non-deterministically pick one enabled action.
    action step = any {
        try_acquire,
        return_permit,
        enqueue,
        cancel,
        set_limit,
    }
}

// ---------------------------------------------------------------------------
// Test instances
// ---------------------------------------------------------------------------

/// Smaller instance for exhaustive model checking (Apalache).
/// 2 permits, queue size 1, set_limit range [0,4] — tractable state space.
module single_pool_verify {
    import single_pool(
        INITIAL_PERMITS = 2,
        MAX_QUEUE_SIZE = 1
    ).*
}

/// Larger instance for simulation (quint run).
module single_pool_test {
    import single_pool(
        INITIAL_PERMITS = 3,
        MAX_QUEUE_SIZE = 2
    ).*
}
