mod algo;
mod client;
mod database;
mod engine;
mod event;
mod load;
mod metrics;
mod output;
mod scenarios;
mod server;
mod simulation;

use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let scenario = args.get(1).map(|s| s.as_str()).unwrap_or("basic");
    let seed: u64 = args
        .iter()
        .position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(rand::random);
    let algo = args
        .iter()
        .position(|a| a == "--algo")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str());
    let output_dir = args
        .iter()
        .position(|a| a == "--output-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let base = PathBuf::from("output").join(scenario);
            if let Some(a) = algo {
                base.join(a)
            } else {
                base
            }
        });

    println!("Scenario: {scenario}  seed: {seed}");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    let sim = match (scenario, algo) {
        ("basic", _) => scenarios::basic(seed),
        ("multi_client", _) => scenarios::multi_client(seed),
        ("convergence_start_high", Some(a)) => scenarios::convergence_start_high(seed, a),
        ("convergence_start_low", Some(a)) => scenarios::convergence_start_low(seed, a),
        ("ramp", Some(a)) => scenarios::ramp(seed, a),
        ("spike", Some(a)) => scenarios::spike(seed, a),
        ("high_variance", Some(a)) => scenarios::high_variance(seed, a),
        ("fairness", Some(a)) => scenarios::fairness(seed, a),
        (s @ ("convergence_start_high" | "convergence_start_low" | "ramp" | "spike" | "high_variance" | "fairness"), None) => {
            eprintln!("Scenario '{s}' requires --algo <name>");
            eprintln!("Available algorithms: aimd, windowed_aimd, vegas, windowed_vegas, gradient, windowed_gradient");
            std::process::exit(1);
        }
        (other, _) => {
            eprintln!("Unknown scenario: {other}");
            eprintln!("Available scenarios: basic, multi_client");
            eprintln!("Scenarios requiring --algo: convergence_start_high, convergence_start_low, ramp, spike, high_variance, fairness");
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
    let chart_scenario = if let Some(a) = algo {
        format!("{scenario}/{a}")
    } else {
        scenario.to_string()
    };
    println!("  gnuplot -e \"scenario='{chart_scenario}'\" simulator/plot.gnu");
}
