use std::time::Duration;

use crate::{client::Client, database::Database, server::Server};

/// The configuration for a simulation run.
pub struct Simulation {
    pub duration: Duration,
    pub clients: Vec<Client>,
    pub server: Server,
    pub database: Option<Database>,
    pub seed: u64,
}
