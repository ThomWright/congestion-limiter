use congestion_limiter::limiter::LimiterState;
use tokio::time::Instant;

/// The outcome of an individual request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOutcome {
    /// The request was processed successfully (or failed for non-overload reasons).
    Success,
    /// Rejected by the client-side limiter before reaching the server.
    ClientRejected,
    /// Rejected by the server-side limiter.
    ServerRejected,
    /// Accepted but the server reported overload.
    Overload,
}

impl RequestOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            RequestOutcome::Success => "success",
            RequestOutcome::ClientRejected => "client_rejected",
            RequestOutcome::ServerRejected => "server_rejected",
            RequestOutcome::Overload => "overload",
        }
    }
}

/// A record of a completed (or immediately rejected) request.
pub struct RequestRecord {
    pub time_s: f64,
    pub client_id: usize,
    /// Zero for immediately rejected requests.
    pub latency_s: f64,
    pub outcome: RequestOutcome,
}

/// A snapshot of a limiter's state at a point in time.
pub struct LimiterSnapshot {
    pub time_s: f64,
    /// Identifies the node, e.g. `"client_0"` or `"server"`.
    pub node: String,
    pub limit: usize,
    pub in_flight: usize,
    pub available: usize,
}

/// Collects per-request records and per-event limiter snapshots.
pub struct Metrics {
    pub requests: Vec<RequestRecord>,
    pub snapshots: Vec<LimiterSnapshot>,
    start: Instant,
}

impl Metrics {
    pub fn new(start: Instant) -> Self {
        Self {
            requests: Vec::new(),
            snapshots: Vec::new(),
            start,
        }
    }

    pub fn record_request(
        &mut self,
        now: Instant,
        client_id: usize,
        start_time: Instant,
        outcome: RequestOutcome,
    ) {
        self.requests.push(RequestRecord {
            time_s: self.elapsed_s(now),
            client_id,
            latency_s: now.duration_since(start_time).as_secs_f64(),
            outcome,
        });
    }

    pub fn record_rejection(&mut self, now: Instant, client_id: usize, outcome: RequestOutcome) {
        self.requests.push(RequestRecord {
            time_s: self.elapsed_s(now),
            client_id,
            latency_s: 0.0,
            outcome,
        });
    }

    pub fn snapshot_limiter(&mut self, now: Instant, node: &str, state: LimiterState) {
        self.snapshots.push(LimiterSnapshot {
            time_s: self.elapsed_s(now),
            node: node.to_owned(),
            limit: state.limit(),
            in_flight: state.in_flight(),
            available: state.available(),
        });
    }

    fn elapsed_s(&self, now: Instant) -> f64 {
        now.duration_since(self.start).as_secs_f64()
    }
}
