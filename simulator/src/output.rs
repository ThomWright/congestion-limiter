use std::{
    fs::{self, File},
    io::{self, Write},
    path::Path,
};

use crate::metrics::{LimiterSnapshot, Metrics, RequestRecord};

/// Write `requests.dat` and `snapshots.dat` to `output_dir`.
///
/// Creates the directory if it does not exist.
pub fn write(metrics: &Metrics, output_dir: &Path) -> io::Result<()> {
    fs::create_dir_all(output_dir)?;
    write_requests(&metrics.requests, &output_dir.join("requests.dat"))?;
    write_snapshots(&metrics.snapshots, &output_dir.join("snapshots.dat"))?;
    Ok(())
}

fn write_requests(records: &[RequestRecord], path: &Path) -> io::Result<()> {
    let mut f = File::create(path)?;
    writeln!(f, "# time_s client_id latency_s outcome")?;
    for r in records {
        writeln!(
            f,
            "{:.6} {} {:.6} {}",
            r.time_s,
            r.client_id,
            r.latency_s,
            r.outcome.as_str()
        )?;
    }
    Ok(())
}

fn write_snapshots(snapshots: &[LimiterSnapshot], path: &Path) -> io::Result<()> {
    let mut f = File::create(path)?;
    writeln!(f, "# time_s node limit in_flight available")?;
    for s in snapshots {
        writeln!(
            f,
            "{:.6} {} {} {} {}",
            s.time_s, s.node, s.limit, s.in_flight, s.available
        )?;
    }
    Ok(())
}
