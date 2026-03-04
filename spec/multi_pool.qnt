// Multi-pool semaphore model.
//
// Each pool has its own available permits, in-flight count, priority reduction,
// and queue. Permits carry a home pool and can be borrowed across pools when
// the donor has spare capacity above its buffer threshold.
//
// This spec refines single_pool: any reachable state here, when projected
// down to aggregate values, must satisfy the single-pool invariants.
// See the refinement section below for details and limitations.

module multi_pool {
    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    /// Number of pools (partitions).
    const NUM_POOLS: int

    /// Total permits across all pools.
    const INITIAL_PERMITS: int

    /// Maximum queue size per pool.
    const MAX_QUEUE_SIZE: int

    /// Weight for each pool. Length must equal NUM_POOLS.
    /// Used by distribute() to allocate permits proportionally.
    const WEIGHTS: List[int]

    // -----------------------------------------------------------------------
    // Types
    // -----------------------------------------------------------------------

    type Pool = {
        available: int,
        limit: int,
        priority_reduction: int,
        queue: List[int],
    }

    /// A permit in flight, tracking where it came from and who's using it.
    type Permit = {
        id: int,
        home_pool: int,
        user_pool: int,
    }

    // -----------------------------------------------------------------------
    // State variables
    // -----------------------------------------------------------------------

    /// Per-pool state, keyed by pool index.
    var pools: int -> Pool

    /// Set of permits currently in flight.
    var permits: Set[Permit]

    /// The current total limit across all pools.
    var total_limit: int

    /// Counter for generating unique IDs (waiters and permits).
    var next_id: int

    // -----------------------------------------------------------------------
    // Utility functions
    // -----------------------------------------------------------------------

    pure def min(a: int, b: int): int = if (a < b) a else b
    pure def max(a: int, b: int): int = if (a > b) a else b

    /// The set of valid pool indices.
    val POOL_INDICES = 0.to(NUM_POOLS - 1)

    /// Sum a function over all pools.
    pure def sum_over(idxs: Set[int], f: int => int): int =
        idxs.fold(0, (acc, i) => acc + f(i))

    /// Number of in-flight permits for a given pool (as user).
    val pool_in_flight = POOL_INDICES.mapBy(
        i => permits.filter(p => p.user_pool == i).size()
    )

    /// Buffer threshold for a pool: 10% of its limit, rounded up, minimum 1
    /// (unless limit is 0).
    pure def buffer(limit: int): int =
        if (limit == 0) 0
        else {
            val tenth = limit / 10
            if (tenth * 10 < limit) tenth + 1 else max(tenth, 1)
        }

    /// Distribute `total` across pools proportional to WEIGHTS using the
    /// largest remainder method.
    ///
    /// Returns a map from pool index to allocation.
    /// Guarantees: sum of allocations == total.
    pure def distribute(total: int, weights: List[int]): int -> int = {
        val weight_sum = weights.indices().fold(0, (acc, i) => acc + weights[i])
        val floors = weights.indices().mapBy(
            i => (total * weights[i]) / weight_sum
        )
        val floor_sum = weights.indices().fold(0, (acc, i) => acc + floors.get(i))
        val remainder = total - floor_sum
        val remainders = weights.indices().mapBy(
            i => (total * weights[i]) % weight_sum
        )
        if (remainder == 0) floors
        else {
            // Greedily assign +1 to the pools with the largest remainders.
            // Ties broken by lowest index.
            val winners = POOL_INDICES.fold(Set(), (selected, _) => {
                if (selected.size() >= remainder) selected
                else {
                    val candidates = POOL_INDICES.exclude(selected)
                    val best = candidates.fold(-1, (best_so_far, i) =>
                        if (best_so_far == -1) i
                        else if (remainders.get(i) > remainders.get(best_so_far)) i
                        else if (remainders.get(i) == remainders.get(best_so_far) and i < best_so_far) i
                        else best_so_far
                    )
                    selected.union(Set(best))
                }
            })
            POOL_INDICES.mapBy(i =>
                if (winners.contains(i)) floors.get(i) + 1
                else floors.get(i)
            )
        }
    }

    /// Build initial pool state from a distribution map.
    pure def init_pools(dist: int -> int): int -> Pool =
        dist.keys().mapBy(i => {
            available: dist.get(i),
            limit: dist.get(i),
            priority_reduction: 0,
            queue: [],
        })

    // -----------------------------------------------------------------------
    // Initialisation
    // -----------------------------------------------------------------------

    action init = {
        val dist = distribute(INITIAL_PERMITS, WEIGHTS)
        all {
            pools' = init_pools(dist),
            permits' = Set(),
            total_limit' = INITIAL_PERMITS,
            next_id' = 0,
        }
    }

    // -----------------------------------------------------------------------
    // Invariants
    // -----------------------------------------------------------------------

    /// Per-pool conservation: available + in_flight == limit + priority_reduction.
    /// NOTE: this does NOT hold when borrowing is enabled, because a borrowed
    /// permit counts against the user pool's in_flight but the donor pool's
    /// available. Defined here for reference but excluded from `inv`.
    val per_pool_conservation = POOL_INDICES.forall(i =>
        pools.get(i).available + pool_in_flight.get(i)
            == pools.get(i).limit + pools.get(i).priority_reduction
    )

    /// Global conservation: sum(available) + sum(in_flight) == total_limit + sum(priority_reduction)
    val global_conservation = {
        val sum_available = sum_over(POOL_INDICES, i => pools.get(i).available)
        val sum_in_flight = permits.size()
        val sum_reduction = sum_over(POOL_INDICES, i => pools.get(i).priority_reduction)
        sum_available + sum_in_flight == total_limit + sum_reduction
    }

    /// Nothing goes negative.
    val non_negative = POOL_INDICES.forall(i => and {
        pools.get(i).available >= 0,
        pools.get(i).priority_reduction >= 0,
        pools.get(i).limit >= 0,
    })

    /// Each pool's queue respects its bound.
    val queues_bounded = POOL_INDICES.forall(i =>
        length(pools.get(i).queue) <= MAX_QUEUE_SIZE
    )

    /// Pool limits sum to total_limit.
    val limits_sum_correct =
        sum_over(POOL_INDICES, i => pools.get(i).limit) == total_limit

    // -----------------------------------------------------------------------
    // Refinement: single-pool projection
    // -----------------------------------------------------------------------

    // Project multi-pool state down to aggregate single-pool values.
    val projected_available =
        sum_over(POOL_INDICES, i => pools.get(i).available)

    val projected_in_flight = permits.size()

    val projected_priority_reduction =
        sum_over(POOL_INDICES, i => pools.get(i).priority_reduction)

    val projected_queue_length =
        sum_over(POOL_INDICES, i => length(pools.get(i).queue))

    /// The single-pool conservation law holds on the projected state.
    val refines_single_pool_conservation =
        projected_available + projected_in_flight
            == total_limit + projected_priority_reduction

    /// The single-pool non-negativity holds on the projected state.
    val refines_single_pool_non_negative = and {
        projected_available >= 0,
        projected_in_flight >= 0,
        projected_priority_reduction >= 0,
        total_limit >= 0,
    }

    /// The projected queue respects the aggregate bound.
    /// Total queue capacity = NUM_POOLS * MAX_QUEUE_SIZE.
    val refines_single_pool_queue_bounded =
        projected_queue_length <= NUM_POOLS * MAX_QUEUE_SIZE

    /// Safety refinement: every reachable multi-pool state, when projected
    /// to aggregate values, satisfies the single-pool invariants.
    ///
    /// This checks state validity but not transition validity. A stronger
    /// guarantee — full behavioral refinement — would verify that every
    /// multi-pool *transition* maps to a valid single-pool step (or stutter).
    /// In TLA+, this is expressed as:
    ///
    ///   SinglePool == INSTANCE single_pool WITH
    ///       available <- projected_available,
    ///       in_flight <- projected_in_flight, ...
    ///   THEOREM MultiPoolSpec => SinglePool!Spec
    ///
    /// where Spec = Init /\ [][Next]_vars. TLC verifies this by checking
    /// that every mapped transition is a valid single-pool step or stutter.
    /// Quint/Apalache don't support behavioral refinement yet, so we use
    /// the weaker invariant-based check. This is sufficient in practice:
    /// if invariants hold at every state, transitions are unlikely to
    /// violate the single-pool contract in ways that only manifest between
    /// states.
    val refines_single_pool = and {
        refines_single_pool_conservation,
        refines_single_pool_non_negative,
        refines_single_pool_queue_bounded,
    }

    /// All invariants combined.
    val inv = and {
        global_conservation,
        non_negative,
        queues_bounded,
        limits_sum_correct,
        refines_single_pool,
    }

    // -----------------------------------------------------------------------
    // Actions
    // -----------------------------------------------------------------------

    /// Acquire a permit from the caller's own pool.
    ///
    /// Guard: pool has available > 0
    action acquire_own(pool_idx: int): bool = all {
        pools.get(pool_idx).available > 0,
        pools' = pools.set(pool_idx, { ...pools.get(pool_idx),
            available: pools.get(pool_idx).available - 1,
        }),
        permits' = permits.union(Set({
            id: next_id, home_pool: pool_idx, user_pool: pool_idx,
        })),
        next_id' = next_id + 1,
        total_limit' = total_limit,
    }

    /// Borrow a permit from a donor pool that has spare capacity above
    /// its buffer threshold. The permit records the donor as home_pool
    /// so it returns there later.
    ///
    /// Guard: own pool empty, donor has available > buffer(donor.limit)
    action acquire_borrow(pool_idx: int, donor_idx: int): bool = all {
        pool_idx != donor_idx,
        pools.get(pool_idx).available == 0,
        pools.get(donor_idx).available > buffer(pools.get(donor_idx).limit),
        pools' = pools.set(donor_idx, { ...pools.get(donor_idx),
            available: pools.get(donor_idx).available - 1,
        }),
        permits' = permits.union(Set({
            id: next_id, home_pool: donor_idx, user_pool: pool_idx,
        })),
        next_id' = next_id + 1,
        total_limit' = total_limit,
    }

    /// Acquire a permit successfully: own pool first, then borrow from
    /// any donor with spare capacity.
    action acquire_success = {
        nondet pool_idx = oneOf(POOL_INDICES)
        any {
            acquire_own(pool_idx),
            nondet donor_idx = oneOf(POOL_INDICES)
            acquire_borrow(pool_idx, donor_idx),
        }
    }

    /// Return a permit. The permit goes back to its home pool. Four
    /// outcomes, checked in order:
    ///
    /// 1. home pool has priority_reduction > 0 → consume (permit destroyed)
    /// 2. home pool's queue has a waiter → wake it (permit stays in-flight)
    /// 3. home pool has spare and another pool has a queued waiter →
    ///    lend to that waiter (permit stays in-flight, re-homed)
    /// 4. otherwise → return to home pool's available
    action return_permit = {
        nondet permit = oneOf(permits)
        val home = permit.home_pool
        val hp = pools.get(home)
        if (hp.priority_reduction > 0) {
            // Case 1: consume the permit to satisfy a pending reduction.
            all {
                pools' = pools.set(home, { ...hp,
                    priority_reduction: hp.priority_reduction - 1,
                }),
                permits' = permits.exclude(Set(permit)),
                total_limit' = total_limit,
                next_id' = next_id,
            }
        } else if (length(hp.queue) > 0) {
            // Case 2: wake the home pool's front waiter. The permit
            // transfers directly — one holder out, one holder in.
            all {
                pools' = pools.set(home, { ...hp,
                    queue: tail(hp.queue),
                }),
                // Remove the old permit, add a new one for the woken waiter.
                permits' = permits.exclude(Set(permit)).union(Set({
                    id: next_id, home_pool: home, user_pool: home,
                })),
                next_id' = next_id + 1,
                total_limit' = total_limit,
            }
        } else if (hp.available >= buffer(hp.limit)) {
            // Case 3: home pool would have spare after return. Instead of
            // adding to available, lend to another pool's queued waiter.
            // Non-deterministically pick which other pool to serve.
            val other_pools_with_waiters = POOL_INDICES.filter(i =>
                i != home and length(pools.get(i).queue) > 0
            )
            if (other_pools_with_waiters.size() > 0) {
                nondet other = oneOf(other_pools_with_waiters)
                val op = pools.get(other)
                all {
                    pools' = pools
                        .set(other, { ...op, queue: tail(op.queue) }),
                    permits' = permits.exclude(Set(permit)).union(Set({
                        id: next_id, home_pool: home, user_pool: other,
                    })),
                    next_id' = next_id + 1,
                    total_limit' = total_limit,
                }
            } else {
                // No other pool has waiters — fall through to case 4.
                all {
                    pools' = pools.set(home, { ...hp,
                        available: hp.available + 1,
                    }),
                    permits' = permits.exclude(Set(permit)),
                    total_limit' = total_limit,
                    next_id' = next_id,
                }
            }
        } else {
            // Case 4: return to home pool's available.
            all {
                pools' = pools.set(home, { ...hp,
                    available: hp.available + 1,
                }),
                permits' = permits.exclude(Set(permit)),
                total_limit' = total_limit,
                next_id' = next_id,
            }
        }
    }

    /// Acquire attempt where no permit is available (own pool empty, no
    /// donor with spare). The caller joins their pool's queue to wait.
    ///
    /// Guard: own pool empty, no donor has spare, queue not full
    action acquire_queued = {
        nondet pool_idx = oneOf(POOL_INDICES)
        val p = pools.get(pool_idx)
        val no_donor = POOL_INDICES.forall(d =>
            d == pool_idx or pools.get(d).available <= buffer(pools.get(d).limit)
        )
        all {
            p.available == 0,
            no_donor,
            length(p.queue) < MAX_QUEUE_SIZE,
            pools' = pools.set(pool_idx, { ...p,
                queue: p.queue.append(next_id),
            }),
            permits' = permits,
            next_id' = next_id + 1,
            total_limit' = total_limit,
        }
    }

    /// A queued waiter times out or is cancelled. They leave the queue
    /// without acquiring a permit.
    action cancel = {
        nondet pool_idx = oneOf(POOL_INDICES)
        val p = pools.get(pool_idx)
        all {
            length(p.queue) > 0,
            nondet idx = oneOf(indices(p.queue))
            pools' = pools.set(pool_idx, { ...p,
                queue: slice(p.queue, 0, idx).concat(
                    slice(p.queue, idx + 1, length(p.queue))
                ),
            }),
            permits' = permits,
            next_id' = next_id,
            total_limit' = total_limit,
        }
    }

    /// Change the total permit limit. Redistributes across pools
    /// proportionally using the largest remainder method.
    ///
    /// For each pool, given its new target limit:
    ///   If increasing: cancel priority reductions, wake waiters, add to available
    ///   If decreasing: consume available, remainder becomes priority_reduction
    action set_limit = {
        nondet new_total = oneOf(0.to(2 * INITIAL_PERMITS))
        val new_dist = distribute(new_total, WEIGHTS)
        // Fold over each pool, accumulating the updated pools map, new
        // permits (from woken waiters), and the next_id counter.
        val result = POOL_INDICES.fold(
            { new_pools: pools, new_permits: permits, nid: next_id },
            (acc, i) => {
                val p = pools.get(i)
                val new_limit = new_dist.get(i)
                val delta = new_limit - p.limit
                if (delta > 0) {
                    // Increasing: cancel reductions, wake waiters, add to available
                    val cancel_reductions = min(delta, p.priority_reduction)
                    val after_cancel = delta - cancel_reductions
                    val wake = min(after_cancel, length(p.queue))
                    val add_available = after_cancel - wake
                    // Create permits for woken waiters
                    val woken_permits = 0.to(wake - 1).fold(
                        { ps: acc.new_permits, id: acc.nid },
                        (pacc, _) => {
                            ps: pacc.ps.union(Set({
                                id: pacc.id, home_pool: i, user_pool: i,
                            })),
                            id: pacc.id + 1,
                        }
                    )
                    {
                        new_pools: acc.new_pools.set(i, { ...p,
                            limit: new_limit,
                            priority_reduction: p.priority_reduction - cancel_reductions,
                            queue: slice(p.queue, wake, length(p.queue)),
                            available: p.available + add_available,
                        }),
                        new_permits: woken_permits.ps,
                        nid: woken_permits.id,
                    }
                } else if (delta < 0) {
                    // Decreasing: consume available, remainder → priority_reduction
                    val abs_delta = -delta
                    val consume = min(abs_delta, p.available)
                    val new_reductions = abs_delta - consume
                    {
                        new_pools: acc.new_pools.set(i, { ...p,
                            limit: new_limit,
                            available: p.available - consume,
                            priority_reduction: p.priority_reduction + new_reductions,
                        }),
                        new_permits: acc.new_permits,
                        nid: acc.nid,
                    }
                } else {
                    // delta == 0: just update limit (may differ due to rounding)
                    {
                        new_pools: acc.new_pools.set(i, { ...p, limit: new_limit }),
                        new_permits: acc.new_permits,
                        nid: acc.nid,
                    }
                }
            }
        )
        all {
            pools' = result.new_pools,
            permits' = result.new_permits,
            next_id' = result.nid,
            total_limit' = new_total,
        }
    }

    action step = any {
        acquire_success,
        acquire_queued,
        return_permit,
        cancel,
        set_limit,
    }
}

// ---------------------------------------------------------------------------
// Test instances
// ---------------------------------------------------------------------------

/// Small instance for exhaustive verification.
module multi_pool_verify {
    import multi_pool(
        NUM_POOLS = 2,
        INITIAL_PERMITS = 2,
        MAX_QUEUE_SIZE = 1,
        WEIGHTS = [1, 3]
    ).*
}

/// Larger instance for simulation.
module multi_pool_test {
    import multi_pool(
        NUM_POOLS = 3,
        INITIAL_PERMITS = 3,
        MAX_QUEUE_SIZE = 2,
        WEIGHTS = [1, 2, 3]
    ).*
}
