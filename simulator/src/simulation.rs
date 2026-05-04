use std::{cmp::Reverse, collections::BinaryHeap, time::Duration};

use rand::{SeedableRng, rngs::SmallRng};
use tokio::time::Instant;

use congestion_limiter::limiter::Outcome;

use crate::{
    client::Client,
    database::Database,
    event::{Event, ScheduledEvent},
    metrics::{Metrics, RequestOutcome},
    server::Server,
};

/// The configuration for a simulation run.
pub struct Simulation {
    pub duration: Duration,
    pub clients: Vec<Client>,
    pub server: Server,
    pub database: Option<Database>,
    pub seed: u64,
}

impl Simulation {
    /// Run the simulation and return the collected metrics.
    pub async fn run(&self) -> Metrics {
        tokio::time::pause();
        let start = Instant::now();

        let mut rng = SmallRng::seed_from_u64(self.seed);
        let mut queue: BinaryHeap<Reverse<ScheduledEvent>> = BinaryHeap::new();
        let mut metrics = Metrics::new(start);

        self.seed_queue(start, &mut rng, &mut queue);

        while let Some(Reverse(scheduled)) = queue.pop() {
            let dt = scheduled.time.duration_since(Instant::now());
            tokio::time::advance(dt).await;
            let now = Instant::now();

            match scheduled.event {
                Event::Arrive { client_id } => {
                    self.handle_arrive(client_id, now, start, &mut queue, &mut metrics, &mut rng);
                }
                Event::Complete {
                    client_id,
                    start_time,
                    client_token,
                    server_token,
                    server_outcome,
                    has_db,
                } => {
                    if has_db {
                        self.database.as_ref().unwrap().finish_request();
                    }
                    if let Some(t) = server_token {
                        t.set_outcome(server_outcome).await;
                    }
                    if let Some(t) = client_token {
                        t.set_outcome(server_outcome).await;
                    }
                    let outcome = match server_outcome {
                        Outcome::Overload => RequestOutcome::Overload,
                        Outcome::Success => RequestOutcome::Success,
                    };
                    let elapsed = now.duration_since(start);
                    metrics.record_request(now, client_id, start_time, outcome);
                    self.snapshot_all(now, elapsed, &mut metrics);
                }
                Event::NetworkReturn { client_token, .. } => {
                    if let Some(t) = client_token {
                        t.set_outcome(Outcome::Overload).await;
                    }
                }
                Event::CapacityChange { workers } => {
                    if let Some(db) = &self.database {
                        db.set_workers(workers);
                    }
                }
                Event::End => break,
            }
        }

        metrics
    }

    /// Schedules the initial arrival events, simulation end, and any capacity changes.
    fn seed_queue(
        &self,
        start: Instant,
        rng: &mut SmallRng,
        queue: &mut BinaryHeap<Reverse<ScheduledEvent>>,
    ) {
        for client in &self.clients {
            if client.active_from < self.duration {
                let dt = client.next_arrival_offset(Duration::ZERO, rng);
                queue.push(Reverse(ScheduledEvent {
                    time: start + client.active_from + dt,
                    event: Event::Arrive {
                        client_id: client.id,
                    },
                }));
            }
        }
        queue.push(Reverse(ScheduledEvent {
            time: start + self.duration,
            event: Event::End,
        }));
        if let Some(db) = &self.database {
            for &(offset, workers) in db.capacity_timeline() {
                queue.push(Reverse(ScheduledEvent {
                    time: start + offset,
                    event: Event::CapacityChange { workers },
                }));
            }
        }
    }

    /// Handles an arrival event: checks limiters, schedules a `Complete` on acceptance, and
    /// schedules the next arrival.
    fn handle_arrive(
        &self,
        client_id: usize,
        now: Instant,
        start: Instant,
        queue: &mut BinaryHeap<Reverse<ScheduledEvent>>,
        metrics: &mut Metrics,
        rng: &mut SmallRng,
    ) {
        let elapsed = now.duration_since(start);
        let client = &self.clients[client_id];

        let client_token = if client.limiter.is_some() {
            if let Some(t) = client.try_acquire() {
                Some(t)
            } else {
                metrics.record_rejection(now, client_id, RequestOutcome::ClientRejected);
                self.snapshot_all(now, elapsed, metrics);
                self.schedule_next(client_id, queue, now, start, elapsed, rng);
                return;
            }
        } else {
            None
        };

        // Try server, then database (if present).
        // client_token is either consumed on rejection or moved into the tuple on acceptance.
        let accepted = if let Some(db) = &self.database {
            match self.server.try_acquire() {
                None => {
                    metrics.record_rejection(now, client_id, RequestOutcome::ServerRejected);
                    queue.push(Reverse(ScheduledEvent {
                        time: now + self.server.network_latency,
                        event: Event::NetworkReturn { client_token },
                    }));
                    None
                }
                Some(server_token) => {
                    let db_latency = db.start_request(rng);
                    let timeout = self.server.db_timeout.unwrap_or(Duration::MAX);
                    let outcome = if db_latency > timeout {
                        Outcome::Overload
                    } else {
                        self.server.sample_outcome(elapsed, rng)
                    };
                    Some((server_token, client_token, db_latency.min(timeout), outcome, true))
                }
            }
        } else {
            match self.server.try_accept(rng) {
                None => {
                    metrics.record_rejection(now, client_id, RequestOutcome::ServerRejected);
                    queue.push(Reverse(ScheduledEvent {
                        time: now + self.server.network_latency,
                        event: Event::NetworkReturn { client_token },
                    }));
                    None
                }
                Some((server_token, latency)) => {
                    let outcome = self.server.sample_outcome(elapsed, rng);
                    Some((server_token, client_token, latency, outcome, false))
                }
            }
        };

        if let Some((server_token, client_token, latency, outcome, has_db)) = accepted {
            queue.push(Reverse(ScheduledEvent {
                time: now + latency + self.server.network_latency,
                event: Event::Complete {
                    client_id,
                    start_time: now,
                    client_token,
                    server_token,
                    server_outcome: outcome,
                    has_db,
                },
            }));
        }

        self.snapshot_all(now, elapsed, metrics);
        self.schedule_next(client_id, queue, now, start, elapsed, rng);
    }

    fn schedule_next(
        &self,
        client_id: usize,
        queue: &mut BinaryHeap<Reverse<ScheduledEvent>>,
        now: Instant,
        start: Instant,
        elapsed: Duration,
        rng: &mut SmallRng,
    ) {
        let client = &self.clients[client_id];
        let active_until = client.active_until.unwrap_or(self.duration);
        if now < start + self.duration && elapsed < active_until {
            let dt = client.next_arrival_offset(elapsed, rng);
            queue.push(Reverse(ScheduledEvent {
                time: now + dt,
                event: Event::Arrive { client_id },
            }));
        }
    }

    fn snapshot_all(&self, now: Instant, elapsed: Duration, metrics: &mut Metrics) {
        for client in &self.clients {
            if elapsed >= client.active_from
                && let Some(state) = client.limiter_state()
            {
                metrics.snapshot_limiter(now, &format!("client_{}", client.id), state);
            }
        }
        if let Some(state) = self.server.limiter_state() {
            metrics.snapshot_limiter(now, "server", state);
        }
        if let Some(db) = &self.database {
            metrics.snapshot_gauge(now, "database", db.in_flight(), db.workers());
        }
    }
}
