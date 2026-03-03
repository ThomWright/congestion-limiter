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
    // State variables
    // -----------------------------------------------------------------------

    /// Permits sitting in the pool, ready to be handed out.
    var available: int

    /// Permits currently held by callers (acquired but not yet returned).
    var in_flight: int

    /// Permits that have been "cancelled" — when a returning permit sees
    /// priority_reduction > 0, it is silently consumed rather than going
    /// back to available or waking a waiter.
    var priority_reduction: int

    /// The current total limit. Starts at INITIAL_PERMITS but can change
    /// via set_limit.
    var total_limit: int

    /// The queue of waiters. Each entry is just a unique waiter ID (int).
    /// FIFO: head of list is served first.
    var queue: List[int]

    /// Counter for generating unique waiter IDs.
    var next_waiter_id: int

    // -----------------------------------------------------------------------
    // Utility functions
    // -----------------------------------------------------------------------

    pure def min(a: int, b: int): int = if (a < b) a else b

    // -----------------------------------------------------------------------
    // Initialisation
    // -----------------------------------------------------------------------

    action init = all {
        available' = INITIAL_PERMITS,
        in_flight' = 0,
        priority_reduction' = 0,
        total_limit' = INITIAL_PERMITS,
        queue' = [],
        next_waiter_id' = 0,
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
        available + in_flight == total_limit + priority_reduction

    /// Nothing goes negative.
    val non_negative = and {
        available >= 0,
        in_flight >= 0,
        priority_reduction >= 0,
        total_limit >= 0,
    }

    /// Queue respects its bound.
    val queue_bounded =
        length(queue) <= MAX_QUEUE_SIZE

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
        available > 0,
        available' = available - 1,
        in_flight' = in_flight + 1,
        // Unchanged:
        priority_reduction' = priority_reduction,
        total_limit' = total_limit,
        queue' = queue,
        next_waiter_id' = next_waiter_id,
    }

    /// Return a permit. Three possible outcomes, checked in order:
    ///
    /// 1. priority_reduction > 0 → consume the permit (it vanishes)
    /// 2. queue is non-empty     → wake the front waiter (permit stays in_flight)
    /// 3. otherwise              → return to available pool
    ///
    /// Guard: in_flight > 0 (you must hold a permit to return one)
    action return_permit = all {
        in_flight > 0,
        if (priority_reduction > 0) {
            // Case 1: consume the permit to satisfy a pending reduction.
            // The permit is destroyed — in_flight goes down, priority_reduction
            // goes down, available stays the same.
            all {
                in_flight' = in_flight - 1,
                priority_reduction' = priority_reduction - 1,
                available' = available,
                queue' = queue,
                next_waiter_id' = next_waiter_id,
            }
        } else if (length(queue) > 0) {
            // Case 2: wake the front waiter. The permit transfers directly
            // from the returner to the waiter — in_flight stays the same
            // (one holder out, one holder in), available unchanged.
            all {
                in_flight' = in_flight,  // net zero: -1 returner, +1 waiter
                queue' = tail(queue),
                available' = available,
                priority_reduction' = priority_reduction,
                next_waiter_id' = next_waiter_id,
            }
        } else {
            // Case 3: no reduction pending, no waiters — return to pool.
            all {
                in_flight' = in_flight - 1,
                available' = available + 1,
                priority_reduction' = priority_reduction,
                queue' = queue,
                next_waiter_id' = next_waiter_id,
            }
        },
        // total_limit never changes in return_permit
        total_limit' = total_limit,
    }

    /// A caller tries to acquire but no permit is available, so they join
    /// the queue to wait.
    ///
    /// Guard: available == 0 AND queue not full
    /// Effect: add a new waiter ID to the back of the queue
    action enqueue = all {
        available == 0,
        length(queue) < MAX_QUEUE_SIZE,
        queue' = queue.append(next_waiter_id),
        next_waiter_id' = next_waiter_id + 1,
        // Unchanged:
        available' = available,
        in_flight' = in_flight,
        priority_reduction' = priority_reduction,
        total_limit' = total_limit,
    }

    /// A queued waiter times out or is cancelled. They leave the queue
    /// without acquiring a permit.
    ///
    /// We non-deterministically pick any waiter in the queue to cancel
    /// (not just the front), since timeouts can expire in any order.
    ///
    /// Guard: queue is non-empty
    action cancel = all {
        length(queue) > 0,
        // Pick a non-deterministic index to remove
        nondet idx = oneOf(indices(queue))
        all {
            // Remove the element at idx: take everything before + everything after
            queue' = slice(queue, 0, idx).concat(slice(queue, idx + 1, length(queue))),
            // Unchanged:
            available' = available,
            in_flight' = in_flight,
            priority_reduction' = priority_reduction,
            total_limit' = total_limit,
            next_waiter_id' = next_waiter_id,
        }
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
    ///
    /// Guard: new_total >= 0 (we bound it to keep the state space finite)
    action set_limit = {
        // Non-deterministically pick a new limit. We bound it to
        // [0, 2*INITIAL_PERMITS] to keep the state space finite.
        nondet new_total = oneOf(0.to(2 * INITIAL_PERMITS))
        val delta = new_total - total_limit
        if (delta > 0) {
            // Increasing: cancel reductions, then wake waiters, then add to available
            val cancel_reductions = min(delta, priority_reduction)
            val after_cancel = delta - cancel_reductions
            val wake_waiters = min(after_cancel, length(queue))
            val add_to_available = after_cancel - wake_waiters
            all {
                total_limit' = new_total,
                priority_reduction' = priority_reduction - cancel_reductions,
                // Woken waiters go straight to in_flight
                in_flight' = in_flight + wake_waiters,
                queue' = slice(queue, wake_waiters, length(queue)),
                available' = available + add_to_available,
                next_waiter_id' = next_waiter_id,
            }
        } else if (delta < 0) {
            // Decreasing: consume available first, remainder → priority_reduction
            val abs_delta = -delta
            val consume_available = min(abs_delta, available)
            val new_reductions = abs_delta - consume_available
            all {
                total_limit' = new_total,
                available' = available - consume_available,
                priority_reduction' = priority_reduction + new_reductions,
                // Unchanged:
                in_flight' = in_flight,
                queue' = queue,
                next_waiter_id' = next_waiter_id,
            }
        } else {
            // delta == 0: no-op, but we still need to assign all vars
            all {
                total_limit' = total_limit,
                available' = available,
                in_flight' = in_flight,
                priority_reduction' = priority_reduction,
                queue' = queue,
                next_waiter_id' = next_waiter_id,
            }
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
// Test instance — provides concrete values for the constants
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
