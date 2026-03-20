#!/usr/bin/env bash
set -euo pipefail
SEED=${1:-$RANDOM}
echo "Seed: $SEED"
for scenario in convergence_start_high convergence_start_low; do # ramp spike high_variance fairness; do
    for algo in aimd windowed_aimd vegas windowed_vegas gradient windowed_gradient; do
        cargo run --package simulator -- "$scenario" --algo "$algo" --seed "$SEED"
    done
done
echo "Plotting..."
for scenario in convergence_start_high convergence_start_low; do # ramp spike high_variance fairness; do
    for algo in aimd windowed_aimd vegas windowed_vegas gradient windowed_gradient; do
        gnuplot -e "scenario='${scenario}/${algo}'" simulator/plot.gnu
    done
    gnuplot -e "scenario='${scenario}'" simulator/compare.gnu
    gnuplot -e "scenario='${scenario}'" simulator/compare_grid.gnu
done
