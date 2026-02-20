#![doc = include_str!(concat!(env!("OUT_DIR"), "/README-rustdocified.md"))]
#![deny(missing_docs)]
#[cfg_attr(doctest, doc = include_str!("../README.md"))]
pub mod aggregation;
pub mod limiter;
pub mod limits;
mod moving_avg;
