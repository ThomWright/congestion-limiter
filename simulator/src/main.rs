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
    let client_algo = args
        .iter()
        .position(|a| a == "--client-algo")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str());
    let server_algo = args
        .iter()
        .position(|a| a == "--server-algo")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str());
    let output_dir = args
        .iter()
        .position(|a| a == "--output-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let base = PathBuf::from("output").join(scenario);
            match (client_algo, server_algo) {
                (Some(c), Some(s)) => base.join(format!("{c}__{s}")),
                (Some(c), None) => base.join(c),
                _ => base,
            }
        });

    println!("Scenario: {scenario}  seed: {seed}");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    let sim = match (scenario, client_algo, server_algo) {
        ("basic", _, _) => scenarios::basic(seed),
        ("client_server", Some(c), Some(s)) => scenarios::client_server(seed, c, s),
        ("client_server", _, _) => {
            eprintln!("Scenario 'client_server' requires --client-algo and --server-algo");
            eprintln!("Available algorithms: aimd, windowed_aimd, vegas, windowed_vegas, gradient, windowed_gradient");
            std::process::exit(1);
        }
        ("convergence_start_high", Some(c), _) => scenarios::convergence_start_high(seed, c),
        ("convergence_start_low", Some(c), _) => scenarios::convergence_start_low(seed, c),
        ("ramp", Some(c), _) => scenarios::ramp(seed, c),
        ("spike", Some(c), _) => scenarios::spike(seed, c),
        ("high_variance", Some(c), _) => scenarios::high_variance(seed, c),
        ("fairness", Some(c), _) => scenarios::fairness(seed, c),
        (s @ ("convergence_start_high" | "convergence_start_low" | "ramp" | "spike" | "high_variance" | "fairness"), None, _) => {
            eprintln!("Scenario '{s}' requires --client-algo <name>");
            eprintln!("Available algorithms: aimd, windowed_aimd, vegas, windowed_vegas, gradient, windowed_gradient");
            std::process::exit(1);
        }
        (other, _, _) => {
            eprintln!("Unknown scenario: {other}");
            eprintln!("Available scenarios: basic, client_server");
            eprintln!("Scenarios requiring --client-algo: convergence_start_high, convergence_start_low, ramp, spike, high_variance, fairness");
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
    let chart_scenario = match (client_algo, server_algo) {
        (Some(c), Some(s)) => format!("{scenario}/{c}__{s}"),
        (Some(c), None) => format!("{scenario}/{c}"),
        _ => scenario.to_string(),
    };
    println!("  gnuplot -e \"scenario='{chart_scenario}'\" simulator/plot.gnu");
}
