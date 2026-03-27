use std::{sync::Arc, time::Duration};

use congestion_limiter::{
    aggregation::Percentile,
    limits::{Aimd, Fixed, Gradient, Vegas, Windowed},
    limiter::Limiter,
};
use statrs::distribution::Erlang;

use crate::{
    algo::LimitAlgo,
    client::Client,
    database::Database,
    load::LoadPattern,
    server::{FailureRate, Server},
    simulation::Simulation,
};

/// Build a limiter using the named algorithm with the given initial limit.
///
/// Windowed variants use `min_window=100ms`, `max_window=5s`, and `Percentile` aggregation.
/// Exits the process with an error message if the name is not recognised.
pub fn build_limiter(algo: &str, initial_limit: usize) -> Arc<Limiter<LimitAlgo>> {
    let limit_algo = match algo {
        "aimd" => LimitAlgo::Aimd(Aimd::new_with_initial_limit(initial_limit)),
        "windowed_aimd" => LimitAlgo::WindowedAimd(
            Windowed::new(Aimd::new_with_initial_limit(initial_limit), Percentile::default())
                .with_min_window(Duration::from_millis(1))
                .with_max_window(Duration::from_secs(1)),
        ),
        "vegas" => LimitAlgo::Vegas(Vegas::new_with_initial_limit(initial_limit)),
        "windowed_vegas" => LimitAlgo::WindowedVegas(
            Windowed::new(Vegas::new_with_initial_limit(initial_limit), Percentile::default())
                .with_min_window(Duration::from_millis(1))
                .with_max_window(Duration::from_secs(1)),
        ),
        "gradient" => LimitAlgo::Gradient(Gradient::new_with_initial_limit(initial_limit)),
        "windowed_gradient" => LimitAlgo::WindowedGradient(
            Windowed::new(Gradient::new_with_initial_limit(initial_limit), Percentile::default())
                .with_min_window(Duration::from_millis(1))
                .with_max_window(Duration::from_secs(1)),
        ),
        other => {
            eprintln!("Unknown algorithm: {other}");
            eprintln!("Available algorithms: aimd, windowed_aimd, vegas, windowed_vegas, gradient, windowed_gradient");
            std::process::exit(1);
        }
    };
    Limiter::builder().limit_algo(limit_algo).build()
}

/// One client with AIMD, server without a limiter. Constant load at roughly 2× server capacity.
pub fn basic(seed: u64) -> Simulation {
    let limiter = Limiter::builder()
        .limit_algo(LimitAlgo::Aimd(Aimd::new_with_initial_limit(10)))
        .build();

    Simulation {
        duration: Duration::from_secs(30),
        clients: vec![Client {
            id: 0,
            load_pattern: LoadPattern::constant(100.0),
            limiter: Some(limiter),
        }],
        server: Server {
            // Mean latency ~200 ms (Erlang k=2, rate=10 → mean = k/rate = 0.2 s)
            latency: Erlang::new(2, 10.0).expect("valid Erlang params"),
            failure_rate: FailureRate::Constant(0.001),
            db_timeout: None,
            limiter: None,
        },
        database: None,
        seed,
    }
}

/// Two-tier scenario: a client calls a server which talks to a database modelled as M/M/c.
///
/// Tests layered limiting: the client and server each run independent algorithms. Load steps
/// up (50 → 100 → 150 rps) then back down, above the database's natural capacity (~100 rps
/// with 2 workers at 20ms mean service time).
pub fn client_server(seed: u64, client_algo: &str, server_algo: &str) -> Simulation {
    let db_latency = Erlang::new(2, 100.0).expect("valid Erlang params");

    let server_limiter = build_limiter(server_algo, 15);

    Simulation {
        duration: Duration::from_secs(450),
        clients: vec![Client {
            id: 0,
            load_pattern: LoadPattern::segments(vec![
                (Duration::from_secs(120), 200.0),  // ramp up   1→200
                (Duration::from_secs(120), 1.0),    // ramp down 200→1
                (Duration::from_secs(30), 30.0),    // ease to 30 (calm before spike)
                (Duration::ZERO, 300.0),             // spike to 300
                (Duration::from_secs(90), 300.0),   // hold spike for 90s
                (Duration::ZERO, 30.0),              // drop back to 30
                (Duration::from_secs(90), 30.0),    // recover at 30 for 90s
            ]),
            limiter: Some(build_limiter(client_algo, 5)),
        }],
        server: Server {
            latency: Erlang::new(2, 10.0).expect("valid Erlang params"),
            failure_rate: FailureRate::Constant(0.001),
            db_timeout: Some(Duration::from_secs(1)),
            limiter: Some(server_limiter),
        },
        database: Some(Database::new(2, db_latency)),
        seed,
    }
}

/// Tests convergence from an initial limit far above the server's true capacity (~100 rps).
///
/// Client starts with limit=100 (5× the true capacity). Constant 200 rps load (2× capacity).
/// Expects the algorithm to converge downward to the actual capacity.
pub fn convergence_start_high(seed: u64, algo: &str) -> Simulation {
    let server_limiter = Limiter::builder()
        .limit_algo(LimitAlgo::Fixed(Fixed::new(20)))
        .build();

    Simulation {
        duration: Duration::from_secs(120),
        clients: vec![Client {
            id: 0,
            load_pattern: LoadPattern::constant(200.0),
            limiter: Some(build_limiter(algo, 100)),
        }],
        server: Server {
            latency: Erlang::new(2, 10.0).expect("valid Erlang params"),
            failure_rate: FailureRate::Constant(0.001),
            db_timeout: None,
            limiter: Some(server_limiter),
        },
        database: None,
        seed,
    }
}

/// Tests convergence from an initial limit far below the server's true capacity (~100 rps).
///
/// Client starts with limit=2 (well below true capacity). Constant 200 rps load (2× capacity).
/// Expects the algorithm to probe upward and converge to the actual capacity.
pub fn convergence_start_low(seed: u64, algo: &str) -> Simulation {
    let server_limiter = Limiter::builder()
        .limit_algo(LimitAlgo::Fixed(Fixed::new(20)))
        .build();

    Simulation {
        duration: Duration::from_secs(120),
        clients: vec![Client {
            id: 0,
            load_pattern: LoadPattern::constant(200.0),
            limiter: Some(build_limiter(algo, 2)),
        }],
        server: Server {
            latency: Erlang::new(2, 10.0).expect("valid Erlang params"),
            failure_rate: FailureRate::Constant(0.001),
            db_timeout: None,
            limiter: Some(server_limiter),
        },
        database: None,
        seed,
    }
}

/// Tests how algorithms track two load cycles, each ramping past capacity and back.
///
/// Two full up/down cycles: 1→200 rps over 90s then 200→1 over 90s, repeated.
/// Server + DB capacity ≈ 100 rps. Tests convergence, recovery, and whether the
/// latency baseline resets correctly between cycles.
pub fn ramp(seed: u64, algo: &str) -> Simulation {
    let db_latency = Erlang::new(2, 100.0).expect("valid Erlang params");

    Simulation {
        duration: Duration::from_secs(360),
        clients: vec![Client {
            id: 0,
            load_pattern: LoadPattern::segments(vec![
                (Duration::from_secs(90), 200.0),  // ramp up   1→200
                (Duration::from_secs(90), 1.0),    // ramp down 200→1
                (Duration::from_secs(90), 200.0),  // ramp up   1→200
                (Duration::from_secs(90), 1.0),    // ramp down 200→1
            ]),
            limiter: Some(build_limiter(algo, 10)),
        }],
        server: Server {
            latency: Erlang::new(2, 10.0).expect("valid Erlang params"),
            failure_rate: FailureRate::Constant(0.001),
            db_timeout: Some(Duration::from_secs(1)),
            limiter: None,
        },
        database: Some(Database::new(2, db_latency)),
        seed,
    }
}

/// Tests reaction to two sudden load spikes with recovery between them.
///
/// Load steps: 30 rps for 30s → 300 rps for 90s → 30 rps for 90s → 300 rps for 90s
/// → 30 rps for 90s. Server + DB capacity ≈ 100 rps.
pub fn spike(seed: u64, algo: &str) -> Simulation {
    let db_latency = Erlang::new(2, 100.0).expect("valid Erlang params");

    Simulation {
        duration: Duration::from_secs(390),
        clients: vec![Client {
            id: 0,
            load_pattern: LoadPattern::step(vec![
                (Duration::from_secs(30), 30.0),
                (Duration::from_secs(90), 300.0),
                (Duration::from_secs(90), 30.0),
                (Duration::from_secs(90), 300.0),
                (Duration::from_secs(90), 30.0),
            ]),
            limiter: Some(build_limiter(algo, 10)),
        }],
        server: Server {
            latency: Erlang::new(2, 10.0).expect("valid Erlang params"),
            failure_rate: FailureRate::Constant(0.001),
            db_timeout: Some(Duration::from_secs(1)),
            limiter: None,
        },
        database: Some(Database::new(2, db_latency)),
        seed,
    }
}

/// Same load profile as `step_load_sim` but with exponential latency (high variance).
///
/// Uses Erlang(k=1, rate=5) — same mean (200ms) as the standard scenarios but much higher
/// variance. Shows how latency noise affects each algorithm's stability.
pub fn high_variance(seed: u64, algo: &str) -> Simulation {
    Simulation {
        duration: Duration::from_secs(60),
        clients: vec![Client {
            id: 0,
            load_pattern: LoadPattern::step(vec![
                (Duration::from_secs(20), 20.0),
                (Duration::from_secs(20), 100.0),
                (Duration::from_secs(20), 20.0),
            ]),
            limiter: Some(build_limiter(algo, 10)),
        }],
        server: Server {
            // Erlang(k=1, rate=10) = Exponential(rate=10): mean=100ms, high variance
            latency: Erlang::new(1, 10.0).expect("valid Erlang params"),
            failure_rate: FailureRate::Constant(0.001),
            db_timeout: None,
            limiter: None,
        },
        database: None,
        seed,
    }
}

/// Tests whether N clients converge to equal shares of a fixed server capacity (~150 rps).
///
/// 3 clients, each sending 100 rps (300 rps total). Server has Fixed(30) limit.
/// Each client starts with limit=20 (above fair share of 10).
pub fn fairness(seed: u64, algo: &str) -> Simulation {
    let server_limiter = Limiter::builder()
        .limit_algo(LimitAlgo::Fixed(Fixed::new(30)))
        .build();

    let clients = (0..3)
        .map(|id| Client {
            id,
            load_pattern: LoadPattern::constant(100.0),
            limiter: Some(build_limiter(algo, 20)),
        })
        .collect();

    Simulation {
        duration: Duration::from_secs(180),
        clients,
        server: Server {
            latency: Erlang::new(2, 10.0).expect("valid Erlang params"),
            failure_rate: FailureRate::Constant(0.001),
            db_timeout: None,
            limiter: Some(server_limiter),
        },
        database: None,
        seed,
    }
}
