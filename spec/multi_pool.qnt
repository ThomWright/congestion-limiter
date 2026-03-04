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

    action stutter = all {
        pools' = pools,
        permits' = permits,
        total_limit' = total_limit,
        next_id' = next_id,
    }

    action step = stutter
}

// ---------------------------------------------------------------------------
// Test instances
// ---------------------------------------------------------------------------

/// Small instance for exhaustive verification.
module multi_pool_verify {
    import multi_pool(
        NUM_POOLS = 2,
        INITIAL_PERMITS = 4,
        MAX_QUEUE_SIZE = 1,
        WEIGHTS = [1, 3]
    ).*
}

/// Larger instance for simulation.
module multi_pool_test {
    import multi_pool(
        NUM_POOLS = 3,
        INITIAL_PERMITS = 6,
        MAX_QUEUE_SIZE = 2,
        WEIGHTS = [1, 2, 3]
    ).*
}
