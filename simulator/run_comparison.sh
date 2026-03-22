#!/usr/bin/env bash
# Usage: ./simulator/run_comparison.sh [--seed N] [scenario ...]
#
# Runs all 6 algorithms for each scenario, then generates per-algo plots and
# comparison charts. With no scenario arguments, runs all scenarios.
#
# Examples:
#   ./simulator/run_comparison.sh
#   ./simulator/run_comparison.sh ramp spike
#   ./simulator/run_comparison.sh --seed 42 convergence_start_high

set -euo pipefail

ALL_SCENARIOS="convergence_start_high convergence_start_low ramp spike high_variance fairness"
ALGOS="aimd windowed_aimd vegas windowed_vegas gradient windowed_gradient"

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
    for algo in $ALGOS; do
        cargo run --package simulator -- "$scenario" --algo "$algo" --seed "$SEED"
    done
done

echo "Plotting..."
for scenario in $SCENARIOS; do
    for algo in $ALGOS; do
        gnuplot -e "scenario='${scenario}/${algo}'" simulator/plot.gnu
    done
    gnuplot -e "scenario='${scenario}'" simulator/compare.gnu
    gnuplot -e "scenario='${scenario}'" simulator/compare_grid.gnu
done
