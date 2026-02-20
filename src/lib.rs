//! Dynamic concurrency limits for controlling backpressure, inspired by TCP congestion control.

#![deny(missing_docs)]

#[cfg(doctest)]
use doc_comment::doctest;
#[cfg(doctest)]
doctest!("../README.md");

pub mod aggregation;
pub mod limiter;
pub mod limits;
mod moving_avg;
