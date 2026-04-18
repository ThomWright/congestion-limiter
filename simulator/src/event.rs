use std::cmp::Ordering;

use congestion_limiter::limiter::{Outcome, Token};
use tokio::time::Instant;

/// Events that can occur during a simulation.
pub enum Event {
    /// A new request arrives from a client.
    Arrive { client_id: usize },

    /// A request has finished processing; release tokens and record outcome.
    Complete {
        client_id: usize,
        /// When the request started, for latency measurement.
        start_time: Instant,
        client_token: Option<Token>,
        /// `None` when the server has no limiter.
        server_token: Option<Token>,
        server_outcome: Outcome,
        /// Whether to decrement the database's in-flight counter on completion.
        has_db: bool,
    },

    /// Change the database's worker count at a pre-scheduled simulation time.
    CapacityChange { workers: usize },

    /// The simulation has reached its configured end time.
    End,
}

/// An event scheduled to occur at a specific instant.
pub struct ScheduledEvent {
    pub time: Instant,
    pub event: Event,
}

impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for ScheduledEvent {}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}
