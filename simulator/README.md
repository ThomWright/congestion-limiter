# Simulator

Exercises the `congestion-limiter` library under controlled synthetic load to answer questions like:

- **Topology**: should limiters be placed client-side, server-side, or both?
- **Algorithms**: how do AIMD, Vegas, and Gradient compare under the same load?
- **Fairness**: do independent clients converge to equal shares, or can one starve the others?
- **Adaptation**: how quickly do limits respond to step changes in load?

## Running a scenario

From the repo root:

```bash
cargo run -p simulator -- <scenario> [--seed N] [--output-dir path]
```

Available scenarios:

| Scenario                   | Description                                                  |
|----------------------------|--------------------------------------------------------------|
| `basic`                    | Single client (AIMD), constant load at ~2× server capacity   |
| `step_load_vegas`          | Single client (raw Vegas), load steps up then down           |
| `step_load_windowed_vegas` | Single client (windowed Vegas, P90), load steps up then down |

Output is written to `output/<scenario>/`.

## Generating charts

Requires [gnuplot](http://www.gnuplot.info/). From the repo root:

```bash
gnuplot -e "scenario='basic'" simulator/plot.gnu
```

Charts are written to `output/<scenario>/plot.png`.
