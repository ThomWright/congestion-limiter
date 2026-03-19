use std::time::Duration;

use crate::{client::Client, server::Server};

/// The configuration for a simulation run.
pub struct Simulation {
    pub duration: Duration,
    pub clients: Vec<Client>,
    pub server: Server,
    pub seed: u64,
}
