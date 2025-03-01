//! Dynamic congestion-based concurrency limits for controlling backpressure.

#![deny(missing_docs)]

#[cfg(doctest)]
use doc_comment::doctest;
#[cfg(doctest)]
doctest!("../README.md");

pub mod aggregation;
pub mod limiter;
pub mod limits;
mod moving_avg;
mod semaphore;
