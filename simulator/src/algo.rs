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
    Fixed(Fixed),
}

#[async_trait]
impl LimitAlgorithm for LimitAlgo {
    fn limit(&self) -> usize {
        match self {
            Self::Aimd(a) => a.limit(),
            Self::WindowedAimd(w) => w.limit(),
            Self::Vegas(v) => v.limit(),
            Self::WindowedVegas(w) => w.limit(),
            Self::Gradient(g) => g.limit(),
            Self::WindowedGradient(w) => w.limit(),
            Self::Fixed(f) => f.limit(),
        }
    }

    async fn update(&self, sample: Sample) -> usize {
        match self {
            Self::Aimd(a) => a.update(sample).await,
            Self::WindowedAimd(w) => w.update(sample).await,
            Self::Vegas(v) => v.update(sample).await,
            Self::WindowedVegas(w) => w.update(sample).await,
            Self::Gradient(g) => g.update(sample).await,
            Self::WindowedGradient(w) => w.update(sample).await,
            Self::Fixed(f) => f.update(sample).await,
        }
    }
}
