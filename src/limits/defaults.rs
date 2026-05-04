use std::time::Duration;

pub const MIN_SAMPLE_LATENCY: Duration = Duration::from_micros(1);

pub const DEFAULT_MIN_LIMIT: usize = 1;
pub const DEFAULT_MAX_LIMIT: usize = 1000;
