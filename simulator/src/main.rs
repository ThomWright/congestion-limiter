mod algo;
mod client;
mod engine;
mod event;
mod load;
mod metrics;
mod output;
mod server;
mod simulation;

use std::{path::PathBuf, time::Duration};

use congestion_limiter::limiter::Limiter;
use statrs::distribution::Erlang;

use algo::LimitAlgo;
use client::Client;
use load::LoadPattern;
use server::{FailureRate, Server};
use simulation::Simulation;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let scenario = args.get(1).map(|s| s.as_str()).unwrap_or("basic");
    let seed: u64 = args
        .iter()
        .position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(rand::random);
    let output_dir = args
        .iter()
        .position(|a| a == "--output-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("output").join(scenario));

    println!("Scenario: {scenario}  seed: {seed}");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    let sim = match scenario {
        "basic" => basic(seed),
        "step_load_vegas" => step_load_vegas(seed),
        "step_load_windowed_vegas" => step_load_windowed_vegas(seed),
        other => {
            eprintln!("Unknown scenario: {other}");
            eprintln!("Available scenarios: basic, step_load_vegas, step_load_windowed_vegas");
            std::process::exit(1);
        }
    };

    let metrics = rt.block_on(engine::run(&sim));

    let total = metrics.requests.len();
    let rejected = metrics
        .requests
        .iter()
        .filter(|r| !matches!(r.outcome, metrics::RequestOutcome::Success))
        .count();
    println!(
        "Requests: {total}  Rejected: {rejected}  ({:.1}%)",
        100.0 * rejected as f64 / total as f64
    );

    output::write(&metrics, &output_dir).expect("failed to write output");
    println!("Output written to {}", output_dir.display());
    println!();
    println!("To generate charts:");
    println!("  gnuplot -e \"scenario='{scenario}'\" simulator/plot.gnu");
}

/// One client with AIMD, server without a limiter. Constant load at roughly 2× server capacity.
fn basic(seed: u64) -> Simulation {
    use congestion_limiter::limits::Aimd;

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
            failure_rate: FailureRate::Constant(0.01),
            limiter: None,
        },
        seed,
    }
}

/// One client with raw Vegas, server without a limiter. Load steps up then down.
///
/// Demonstrates Vegas's sensitivity to latency variance: the minimum observed latency drifts
/// low during the quiet phase, corrupting the baseline and causing spurious limit changes.
fn step_load_vegas(seed: u64) -> Simulation {
    use congestion_limiter::limits::Vegas;

    let limiter = Limiter::builder()
        .limit_algo(LimitAlgo::Vegas(Vegas::new_with_initial_limit(10)))
        .build();

    step_load_sim(seed, limiter)
}

/// One client with windowed Vegas (P90 aggregation), server without a limiter. Load steps up then
/// down.
///
/// The percentile window stabilises the baseline latency estimate, allowing Vegas to correctly
/// distinguish congestion from natural latency variance.
fn step_load_windowed_vegas(seed: u64) -> Simulation {
    use congestion_limiter::{aggregation::Percentile, limits::{Vegas, Windowed}};

    let algo = Windowed::new(Vegas::new_with_initial_limit(10), Percentile::default());
    let limiter = Limiter::builder()
        .limit_algo(LimitAlgo::WindowedVegas(algo))
        .build();

    step_load_sim(seed, limiter)
}

fn step_load_sim(seed: u64, limiter: std::sync::Arc<congestion_limiter::limiter::Limiter<LimitAlgo>>) -> Simulation {
    Simulation {
        duration: Duration::from_secs(60),
        clients: vec![Client {
            id: 0,
            load_pattern: LoadPattern::step(vec![
                (Duration::from_secs(20), 20.0),
                (Duration::from_secs(20), 80.0),
                (Duration::from_secs(20), 20.0),
            ]),
            limiter: Some(limiter),
        }],
        server: Server {
            latency: Erlang::new(2, 10.0).expect("valid Erlang params"),
            failure_rate: FailureRate::Constant(0.01),
            limiter: None,
        },
        seed,
    }
}
