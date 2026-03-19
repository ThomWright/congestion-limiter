use async_trait::async_trait;
use congestion_limiter::{
    aggregation::Percentile,
    limits::{Aimd, Fixed, Gradient, LimitAlgorithm, Sample, Vegas, Windowed},
};

/// The set of limit algorithms available in the simulator.
#[derive(Debug)]
pub enum LimitAlgo {
    Aimd(Aimd),
    WindowedAimd(Windowed<Aimd, Percentile>),
    Vegas(Vegas),
    WindowedVegas(Windowed<Vegas, Percentile>),
    Gradient(Gradient),
    WindowedGradient(Windowed<Gradient, Percentile>),
    #[allow(dead_code, reason = "available for baseline scenarios, not yet used")]
    Fixed(Fixed),
}

#[async_trait]
impl LimitAlgorithm for LimitAlgo {
    fn limit(&self) -> usize {
        match self {
            LimitAlgo::Aimd(a) => a.limit(),
            LimitAlgo::WindowedAimd(w) => w.limit(),
            LimitAlgo::Vegas(v) => v.limit(),
            LimitAlgo::WindowedVegas(w) => w.limit(),
            LimitAlgo::Gradient(g) => g.limit(),
            LimitAlgo::WindowedGradient(w) => w.limit(),
            LimitAlgo::Fixed(f) => f.limit(),
        }
    }

    async fn update(&self, sample: Sample) -> usize {
        match self {
            LimitAlgo::Aimd(a) => a.update(sample).await,
            LimitAlgo::WindowedAimd(w) => w.update(sample).await,
            LimitAlgo::Vegas(v) => v.update(sample).await,
            LimitAlgo::WindowedVegas(w) => w.update(sample).await,
            LimitAlgo::Gradient(g) => g.update(sample).await,
            LimitAlgo::WindowedGradient(w) => w.update(sample).await,
            LimitAlgo::Fixed(f) => f.update(sample).await,
        }
    }
}
