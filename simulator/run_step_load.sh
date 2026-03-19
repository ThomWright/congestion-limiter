#!/usr/bin/env bash
set -euo pipefail

SEED=${1:-$RANDOM}
echo "Seed: $SEED"
echo

for scenario in step_load_aimd step_load_windowed_aimd \
                step_load_vegas step_load_windowed_vegas \
                step_load_gradient step_load_windowed_gradient; do
    echo "Running $scenario..."
    cargo run --package simulator -- "$scenario" --seed "$SEED"
done

echo "Plotting..."
for scenario in step_load_aimd step_load_windowed_aimd \
                step_load_vegas step_load_windowed_vegas \
                step_load_gradient step_load_windowed_gradient; do
    gnuplot -e "scenario='$scenario'" simulator/plot.gnu
done

echo
echo "Done. Plots written to output/<scenario>/plot.png"
