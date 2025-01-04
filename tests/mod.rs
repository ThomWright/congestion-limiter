#![expect(dead_code)]

use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc, time::Duration};

use async_trait::async_trait;
use conv::ValueFrom;
use itertools::Itertools;
use rand::{prelude::Distribution, rngs::SmallRng, Rng, SeedableRng};
use statrs::{
    distribution::{Erlang, Exp},
    statistics::Distribution as StatDist,
};
use tokio::time::Instant;

use congestion_limiter::{
    limiter::{Limiter, LimiterState, Outcome, Token},
    limits::{Aimd, LimitAlgorithm, Sample},
};

mod iter_ext;

use iter_ext::MeanExt;

struct Simulation {
    duration: Duration,
    client: Client,
    server: Server,
}

type Id = usize;

/// The limiting algorithms we'll be testing.
#[derive(Debug)]
enum LimitWrapper {
    Aimd(Aimd),
}
#[async_trait]
impl LimitAlgorithm for LimitWrapper {
    fn limit(&self) -> usize {
        match self {
            LimitWrapper::Aimd(l) => l.limit(),
        }
    }
    async fn update(&self, reading: Sample) -> usize {
        match self {
            LimitWrapper::Aimd(l) => l.update(reading).await,
        }
    }
}

/// Models a Poisson process.
struct Client {
    limiter: Option<Arc<Limiter<LimitWrapper>>>,

    /// Poisson process, exponential interarrival times.
    interarrival: Exp,
}

struct Server {
    limiter: Option<Arc<Limiter<LimitWrapper>>>,

    latency: Erlang,

    /// Range: [0, 1)
    failure_rate: f64,
}

/// Latency is calculated according to the number of tasks needing to be performed and the
/// average rate of completion of these tasks (per second).
struct LatencyProfile {
    tasks: u64,
    task_rate: f64,
}

#[derive(Debug)]
struct LimiterToken {
    token: Token,

    /// Limiter state just after the request started.
    limit_state: LimiterState,
}

struct ServerResponse {
    latency: Duration,
    server_state: Option<LimiterToken>,
}

struct RequestOutcome {
    result: Outcome,

    /// Limiter state just after the request finished.
    limit_state: LimiterState,
}

/// Processed by a [`Simulation`].
#[derive(Debug)]
struct Event {
    time: Instant,
    typ: Action,
}
#[derive(Debug)]
enum Action {
    StartRequest {
        client_id: Id,
    },
    EndRequest {
        start_time: Instant,
        client_id: Id,
        server_id: Id,
        client: Option<LimiterToken>,
        server: Option<LimiterToken>,
    },
}

/// Summarises the outcome of a simulation run.
struct Summary {
    started_at: Instant,
    event_log: Vec<event_log::Item>,
    requests: Vec<RequestSummary>,
}

#[derive(Debug)]
struct RequestSummary {
    start_time: Instant,
    end_time: Instant,
    latency: Duration,
    result: Outcome,
}

mod event_log {
    use super::*;

    #[derive(Debug)]
    pub enum Item {
        Client(Id, LimiterEvent),
        Server(Id, LimiterEvent),
    }

    /// ```text
    /// <limiter?> N -> No log
    ///          ` Y -> <full?> N -> Accepted -> Finished
    ///                       ` Y -> Rejected
    /// ```
    #[derive(Debug)]
    pub enum LimiterEvent {
        Accepted(LimiterState),
        Rejected(LimiterState),
        Finished(Outcome, LimiterState),
    }

    impl Item {
        pub fn limit_state(&self) -> Option<LimiterState> {
            use Item::*;
            use LimiterEvent::*;
            let event = match self {
                Client(_, event) => event,
                Server(_, event) => event,
            };
            match event {
                Accepted(ls) => Some(*ls),
                Rejected(ls) => Some(*ls),
                Finished(_, ls) => Some(*ls),
            }
        }
    }
}

impl Client {
    /// Create a client which sends `rps` requests per second on average.
    fn with_rps(limiter: Option<Arc<Limiter<LimitWrapper>>>, rps: f64) -> Self {
        Self {
            limiter,
            interarrival: Exp::new(rps).unwrap(),
        }
    }

    fn next_arrival_in(&self, rng: &mut SmallRng) -> Duration {
        let dt = self.interarrival.sample(rng);
        Duration::from_secs_f64(dt)
    }

    /// Send a request.
    async fn send_req(&self) -> Result<Option<LimiterToken>, LimiterState> {
        let limiter = self.limiter.as_ref();

        match limiter {
            Some(limiter) => limiter
                .try_acquire()
                .await
                .map(|token| {
                    Some(LimiterToken {
                        token,
                        limit_state: limiter.state(),
                    })
                })
                .ok_or(limiter.state()),
            None => Ok(None),
        }
    }

    /// Receive a response.
    async fn res(&self, token: Token, result: Outcome) -> RequestOutcome {
        let limiter = self
            .limiter
            .as_ref()
            .expect("Shouldn't call Client::res() unless it has a limiter");

        token.set_outcome(result).await;

        RequestOutcome {
            result,
            limit_state: limiter.state(),
        }
    }

    fn rps(&self) -> f64 {
        self.interarrival.rate()
    }
}

impl Server {
    /// Create a server with a concurrency limiter, a latency distribution and a failure rate.
    fn new(
        limiter: Option<Arc<Limiter<LimitWrapper>>>,
        latency_profile: LatencyProfile,
        failure_rate: f64,
    ) -> Self {
        assert!((0.0..=1.0).contains(&failure_rate));
        Self {
            limiter,
            latency: Erlang::from(latency_profile),
            failure_rate,
        }
    }

    /// Start processing a request.
    async fn recv_req(&self, rng: &mut SmallRng) -> Result<ServerResponse, LimiterState> {
        let latency = Duration::from_secs_f64(self.latency.sample(rng));
        let limiter = self.limiter.as_ref();

        match limiter {
            Some(limiter) => limiter
                .try_acquire()
                .await
                .map(|token| LimiterToken {
                    token,
                    limit_state: limiter.state(),
                })
                .ok_or(limiter.state())
                .map(|limited| ServerResponse {
                    latency,
                    server_state: Some(limited),
                }),
            None => Ok(ServerResponse {
                latency,
                server_state: None,
            }),
        }
    }

    /// Return a response.
    async fn res(&self, token: Token, rng: &mut SmallRng) -> RequestOutcome {
        let limiter = self
            .limiter
            .as_ref()
            .expect("Shouldn't call Client::res() unless it has a limiter");

        let result = if rng.gen_range(0.0..=1.0) > self.failure_rate {
            Outcome::Success
        } else {
            Outcome::Overload
        };

        token.set_outcome(result).await;

        RequestOutcome {
            result,
            limit_state: limiter.state(),
        }
    }

    fn mean_latency(&self) -> f64 {
        self.latency.mean().unwrap()
    }
}

impl From<LatencyProfile> for Erlang {
    fn from(lp: LatencyProfile) -> Self {
        Erlang::new(lp.tasks, lp.task_rate).unwrap()
    }
}

impl PartialEq for Event {
    fn eq(&self, other: &Self) -> bool {
        self.time.eq(&other.time)
    }
}
impl Eq for Event {}
impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Event {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.cmp(&other.time)
    }
}

impl Simulation {
    async fn run(&mut self) -> Summary {
        tokio::time::pause();
        let start = Instant::now();

        let seed = rand::random();
        let mut rng = SmallRng::seed_from_u64(seed);
        println!("Seed: {seed}\n");

        // Priority queue of events (min heap).
        let mut queue = BinaryHeap::new();
        queue.push(Reverse(Event {
            time: start,
            typ: Action::StartRequest { client_id: 0 },
        }));

        let mut requests = BinaryHeap::new();
        let mut event_log = vec![];

        let mut current_time = start;
        while let Some(Reverse(event)) = queue.pop() {
            current_time = {
                let dt = event.time.duration_since(current_time);
                tokio::time::advance(dt).await;
                Instant::now()
            };

            match event.typ {
                Action::StartRequest { client_id } => {
                    let rejected = match self.client.send_req().await {
                        Ok(client_state) => {
                            if let Some(ref s) = client_state {
                                event_log.push(event_log::Item::Client(
                                    client_id,
                                    event_log::LimiterEvent::Accepted(s.limit_state),
                                ));
                            }

                            match self.server.recv_req(&mut rng).await {
                                Ok(res) => {
                                    if let Some(ref s) = res.server_state {
                                        event_log.push(event_log::Item::Server(
                                            0,
                                            event_log::LimiterEvent::Accepted(s.limit_state),
                                        ));
                                    }

                                    queue.push(Reverse(Event {
                                        time: current_time + res.latency,
                                        typ: Action::EndRequest {
                                            client_id,
                                            server_id: 0,
                                            start_time: current_time,
                                            client: client_state,
                                            server: res.server_state,
                                        },
                                    }));

                                    false
                                }
                                Err(limit_state) => {
                                    if let Some(client_state) = client_state {
                                        let req_outcome = self
                                            .client
                                            .res(client_state.token, Outcome::Overload)
                                            .await;

                                        event_log.push(event_log::Item::Client(
                                            client_id,
                                            event_log::LimiterEvent::Finished(
                                                Outcome::Overload,
                                                req_outcome.limit_state,
                                            ),
                                        ));
                                    }
                                    event_log.push(event_log::Item::Server(
                                        0,
                                        event_log::LimiterEvent::Rejected(limit_state),
                                    ));

                                    true
                                }
                            }
                        }
                        Err(limiter_state) => {
                            event_log.push(event_log::Item::Client(
                                client_id,
                                event_log::LimiterEvent::Rejected(limiter_state),
                            ));

                            true
                        }
                    };

                    if rejected {
                        requests.push(RequestSummary {
                            start_time: current_time,
                            end_time: current_time,
                            latency: Duration::ZERO,
                            result: Outcome::Overload,
                        });
                    }

                    if current_time.duration_since(start) < self.duration {
                        let dt = self.client.next_arrival_in(&mut rng);
                        let event = Event {
                            time: current_time + dt,
                            typ: Action::StartRequest { client_id },
                        };
                        queue.push(Reverse(event));
                    }
                }

                Action::EndRequest {
                    start_time,
                    client_id,
                    server_id,
                    client,
                    server,
                } => {
                    let server_result = if let Some(limiter_state) = server {
                        let result = self.server.res(limiter_state.token, &mut rng).await;

                        event_log.push(event_log::Item::Server(
                            server_id,
                            event_log::LimiterEvent::Finished(result.result, result.limit_state),
                        ));

                        result.result
                    } else {
                        Outcome::Success
                    };

                    let client_result = if let Some(client_state) = client {
                        let result = self.client.res(client_state.token, server_result).await;

                        event_log.push(event_log::Item::Client(
                            client_id,
                            event_log::LimiterEvent::Finished(result.result, result.limit_state),
                        ));

                        result.result
                    } else {
                        Outcome::Success
                    };

                    requests.push(RequestSummary {
                        start_time,
                        end_time: current_time,
                        latency: current_time.duration_since(start_time),
                        result: client_result,
                    });
                }
            }
        }

        Summary {
            started_at: start,
            event_log,
            requests: requests.into_sorted_vec(),
        }
    }
}

impl PartialEq for RequestSummary {
    fn eq(&self, other: &Self) -> bool {
        self.start_time.eq(&other.start_time)
    }
}
impl Eq for RequestSummary {}
impl PartialOrd for RequestSummary {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RequestSummary {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start_time.cmp(&other.start_time)
    }
}

impl Summary {
    fn total_requests(&self) -> usize {
        self.requests.len()
    }
    fn total_rejected(&self) -> usize {
        self.event_log
            .iter()
            .filter(|el| {
                matches!(
                    el,
                    event_log::Item::Client(_, event_log::LimiterEvent::Rejected(..))
                        | event_log::Item::Server(_, event_log::LimiterEvent::Rejected(..))
                )
            })
            .count()
    }
    fn mean_latency(&self) -> Duration {
        self.requests.iter().map(|r| r.latency).sum::<Duration>()
            / u32::value_from(self.total_requests()).unwrap()
    }
    fn max_concurrency(&self) -> usize {
        self.event_log
            .iter()
            .map(|log| log.limit_state().map(|l| l.in_flight()).unwrap_or_default())
            .max()
            .unwrap_or(0)
    }
    fn mean_interarrival_time(&self) -> Duration {
        self.requests
            .iter()
            .map(|r| r.start_time)
            .tuple_windows()
            .map(|(a, b)| b - a)
            .mean()
    }

    fn print_summary(&self) {
        // println!("{:#?}", self.requests);

        println!("Summary");
        println!("=======");

        println!("Requests: {}", self.total_requests());
        println!("Rejected: {}", self.total_rejected());

        println!(
            "Mean interarrival time: {:#?}",
            self.mean_interarrival_time()
        );

        println!("Mean latency: {:#?}", self.mean_latency());
        println!("Max. concurrency: {:#?}", self.max_concurrency());
    }
}

#[tokio::test]
async fn test() {
    let simulation_duration = Duration::from_secs(1);

    let client = Client::with_rps(
        Some(Limiter::new(LimitWrapper::Aimd(
            Aimd::new_with_initial_limit(10)
                .with_max_limit(20)
                .decrease_factor(0.9)
                .increase_by(1),
        ))),
        100.0,
    );

    let server = Server::new(
        None,
        LatencyProfile {
            tasks: 2,
            task_rate: 10.0,
        },
        0.01,
    );

    println!("Duration");
    println!("========");
    println!("{:#?}", simulation_duration);
    println!();
    println!("Client");
    println!("======");
    println!("RPS: {}", client.rps());
    println!();
    println!("Server");
    println!("======");
    println!("Mean latency: {}", server.mean_latency());
    println!();
    // TODO: print limiter info

    let mut simulation = Simulation {
        duration: simulation_duration,

        client,
        server,
    };

    let summary = simulation.run().await;

    summary.print_summary();
}
