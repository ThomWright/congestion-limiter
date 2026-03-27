#!/usr/bin/env bash
# Usage: ./simulator/run_comparison.sh [--seed N] [scenario ...]
#
# Runs all 6 algorithms for each comparison scenario, and 4 algo combinations
# for client_server. Generates per-algo plots and comparison charts.
# With no scenario arguments, runs all scenarios.
#
# Examples:
#   ./simulator/run_comparison.sh
#   ./simulator/run_comparison.sh ramp spike
#   ./simulator/run_comparison.sh --seed 42 convergence_start_high
#   ./simulator/run_comparison.sh client_server

set -euo pipefail

ALL_SCENARIOS="convergence_start_high convergence_start_low ramp spike high_variance fairness client_server"
ALGOS="aimd windowed_aimd vegas windowed_vegas gradient windowed_gradient"
CLIENT_SERVER_COMBOS="windowed_aimd windowed_gradient"

SEED=$RANDOM
SCENARIOS=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --seed) SEED="$2"; shift 2 ;;
        *)      SCENARIOS="$SCENARIOS $1"; shift ;;
    esac
done

SCENARIOS="${SCENARIOS:-$ALL_SCENARIOS}"

echo "Seed: $SEED"
echo "Scenarios:$SCENARIOS"
echo

for scenario in $SCENARIOS; do
    if [[ "$scenario" == "client_server" ]]; then
        for client in $CLIENT_SERVER_COMBOS; do
            for server in $CLIENT_SERVER_COMBOS; do
                cargo run --package simulator -- "$scenario" \
                    --client-algo "$client" --server-algo "$server" --seed "$SEED"
            done
        done
    else
        for algo in $ALGOS; do
            cargo run --package simulator -- "$scenario" --client-algo "$algo" --seed "$SEED"
        done
    fi
done

echo "Plotting..."
for scenario in $SCENARIOS; do
    if [[ "$scenario" == "client_server" ]]; then
        for client in $CLIENT_SERVER_COMBOS; do
            for server in $CLIENT_SERVER_COMBOS; do
                gnuplot -e "scenario='${scenario}/${client}__${server}'" simulator/plot.gnu
            done
        done
    else
        for algo in $ALGOS; do
            gnuplot -e "scenario='${scenario}/${algo}'" simulator/plot.gnu
        done
        gnuplot -e "scenario='${scenario}'" simulator/compare.gnu
        gnuplot -e "scenario='${scenario}'" simulator/compare_grid.gnu
    fi
done
