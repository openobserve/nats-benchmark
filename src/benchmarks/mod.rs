pub mod kv;
pub mod jetstream;

use crate::metrics::BenchmarkResults;
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Benchmark {
    /// Setup benchmark resources (create buckets, streams, etc.)
    async fn setup(&mut self) -> Result<()>;

    /// Run the benchmark
    async fn run(&mut self) -> Result<BenchmarkResults>;

    /// Cleanup benchmark resources
    async fn teardown(&mut self) -> Result<()>;
}
