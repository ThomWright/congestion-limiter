use std::{cmp::Reverse, collections::BinaryHeap, time::Duration};

use rand::{rngs::SmallRng, SeedableRng};
use tokio::time::Instant;

use congestion_limiter::limiter::Outcome;

use crate::{
    client::Client,
    event::{Event, ScheduledEvent},
    metrics::{Metrics, RequestOutcome},
    simulation::Simulation,
};

/// Run the simulation and return the collected metrics.
pub async fn run(sim: &Simulation) -> Metrics {
    tokio::time::pause();
    let start = Instant::now();

    let mut rng = SmallRng::seed_from_u64(sim.seed);
    let mut queue: BinaryHeap<Reverse<ScheduledEvent>> = BinaryHeap::new();
    let mut metrics = Metrics::new(start);

    // Schedule the first arrival for each client and the simulation end.
    for client in &sim.clients {
        let dt = client.next_arrival_offset(Duration::ZERO, &mut rng);
        queue.push(Reverse(ScheduledEvent {
            time: start + dt,
            event: Event::Arrive {
                client_id: client.id,
            },
        }));
    }
    queue.push(Reverse(ScheduledEvent {
        time: start + sim.duration,
        event: Event::End,
    }));

    // Schedule database capacity changes.
    if let Some(db) = &sim.database {
        for &(offset, workers) in db.capacity_timeline() {
            queue.push(Reverse(ScheduledEvent {
                time: start + offset,
                event: Event::CapacityChange { workers },
            }));
        }
    }

    while let Some(Reverse(scheduled)) = queue.pop() {
        let dt = scheduled.time.duration_since(Instant::now());
        tokio::time::advance(dt).await;
        let now = Instant::now();

        match scheduled.event {
            Event::Arrive { client_id } => {
                let elapsed = now.duration_since(start);
                let client = &sim.clients[client_id];

                // Try client limiter.
                let client_token = if client.limiter.is_some() {
                    match client.try_acquire() {
                        Some(t) => Some(t),
                        None => {
                            metrics.record_rejection(
                                now,
                                client_id,
                                RequestOutcome::ClientRejected,
                            );
                            snapshot_all(now, sim, &mut metrics);
                            schedule_next(
                                &mut queue,
                                client,
                                now,
                                start,
                                elapsed,
                                sim.duration,
                                &mut rng,
                            );
                            continue;
                        }
                    }
                } else {
                    None
                };

                // Try server, then database (if present).
                // client_token is either consumed on rejection or moved into the tuple on acceptance.
                let accepted = if let Some(db) = &sim.database {
                    match sim.server.try_acquire() {
                        None => {
                            if let Some(t) = client_token {
                                t.set_outcome(Outcome::Overload).await;
                            }
                            metrics.record_rejection(
                                now,
                                client_id,
                                RequestOutcome::ServerRejected,
                            );
                            None
                        }
                        Some(server_token) => {
                            let db_latency = db.start_request(&mut rng);
                            let timeout = sim.server.db_timeout.unwrap_or(Duration::MAX);
                            let timed_out = db_latency > timeout;
                            let outcome = if timed_out {
                                Outcome::Overload
                            } else {
                                sim.server.sample_outcome(elapsed, &mut rng)
                            };
                            Some((server_token, client_token, db_latency.min(timeout), outcome, true))
                        }
                    }
                } else {
                    match sim.server.try_accept(&mut rng) {
                        None => {
                            if let Some(t) = client_token {
                                t.set_outcome(Outcome::Overload).await;
                            }
                            metrics.record_rejection(
                                now,
                                client_id,
                                RequestOutcome::ServerRejected,
                            );
                            None
                        }
                        Some((server_token, latency)) => {
                            let outcome = sim.server.sample_outcome(elapsed, &mut rng);
                            Some((server_token, client_token, latency, outcome, false))
                        }
                    }
                };

                if let Some((server_token, client_token, latency, outcome, has_db)) = accepted {
                    queue.push(Reverse(ScheduledEvent {
                        time: now + latency,
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

                snapshot_all(now, sim, &mut metrics);
                schedule_next(
                    &mut queue,
                    client,
                    now,
                    start,
                    elapsed,
                    sim.duration,
                    &mut rng,
                );
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
                    sim.database.as_ref().unwrap().finish_request();
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
                metrics.record_request(now, client_id, start_time, outcome);
                snapshot_all(now, sim, &mut metrics);
            }

            Event::CapacityChange { workers } => {
                if let Some(db) = &sim.database {
                    db.set_workers(workers);
                }
            }

            Event::End => break,
        }
    }

    metrics
}

fn schedule_next(
    queue: &mut BinaryHeap<Reverse<ScheduledEvent>>,
    client: &Client,
    now: Instant,
    start: Instant,
    elapsed: Duration,
    duration: Duration,
    rng: &mut SmallRng,
) {
    if now < start + duration {
        let dt = client.next_arrival_offset(elapsed, rng);
        queue.push(Reverse(ScheduledEvent {
            time: now + dt,
            event: Event::Arrive {
                client_id: client.id,
            },
        }));
    }
}

fn snapshot_all(now: Instant, sim: &Simulation, metrics: &mut Metrics) {
    for client in &sim.clients {
        if let Some(state) = client.limiter_state() {
            metrics.snapshot_limiter(now, &format!("client_{}", client.id), state);
        }
    }
    if let Some(state) = sim.server.limiter_state() {
        metrics.snapshot_limiter(now, "server", state);
    }
    if let Some(db) = &sim.database {
        metrics.snapshot_gauge(now, "database", db.in_flight(), db.workers());
    }
}
