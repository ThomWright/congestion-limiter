mod algo;
mod client;
mod database;
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
    let scenario = args.get(1).map_or("basic", String::as_str);
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
        .map(String::as_str);
    let server_algo = args
        .iter()
        .position(|a| a == "--server-algo")
        .and_then(|i| args.get(i + 1))
        .map(String::as_str);
    let output_dir = args
        .iter()
        .position(|a| a == "--output-dir")
        .and_then(|i| args.get(i + 1))
        .map_or_else(
            || {
                let base = PathBuf::from("output").join(scenario);
                match (client_algo, server_algo) {
                    (Some(c), Some(s)) => base.join(format!("{c}__{s}")),
                    (Some(c), None) => base.join(c),
                    _ => base,
                }
            },
            PathBuf::from,
        );

    println!("Scenario: {scenario}  seed: {seed}");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("failed to build tokio runtime");

    let sim = scenarios::build(scenario, client_algo, server_algo, seed);

    let metrics = rt.block_on(sim.run());

    let total = metrics.requests.len();
    let rejected = metrics
        .requests
        .iter()
        .filter(|r| !matches!(r.outcome, metrics::RequestOutcome::Success))
        .count();
    #[allow(
        clippy::cast_precision_loss,
        reason = "request counts are bounded well within f64 precision"
    )]
    let rejection_pct = 100.0 * rejected as f64 / total as f64;
    println!("Requests: {total}  Rejected: {rejected}  ({rejection_pct:.1}%)");

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
