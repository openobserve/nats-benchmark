mod benchmarks;
mod config;
mod metrics;
mod reporter;
mod worker;

use anyhow::Result;
use benchmarks::Benchmark;
use clap::Parser;
use config::{Cli, Commands};
use reporter::Reporter;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup logging
    let log_level = if cli.verbose { Level::DEBUG } else { Level::INFO };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Connect to NATS
    info!("Connecting to NATS at {}", cli.url);
    let client = async_nats::connect(&cli.url).await?;
    info!("Connected successfully");

    // Create reporter
    let reporter = Reporter::new(cli.output.clone(), true);

    // Run appropriate benchmark
    let (results, benchmark_name) = match cli.command {
        Commands::KvPut(config) => {
            config.validate()?;
            let mut benchmark = benchmarks::kv::KvPutBenchmark::new(client, config.clone());
            benchmark.setup().await?;

            let metrics = benchmark.pool.as_ref().unwrap().metrics.clone();
            let mut progress_tracker = metrics::ProgressTracker::new(metrics);

            let results = reporter::run_with_progress(&reporter, &mut progress_tracker, || async {
                benchmark.run().await
            })
            .await?;

            benchmark.teardown().await?;
            (results, "KV PUT".to_string())
        }

        Commands::KvGet(config) => {
            config.validate()?;
            let mut benchmark = benchmarks::kv::KvGetBenchmark::new(client, config.clone());
            benchmark.setup().await?;

            let metrics = benchmark.pool.as_ref().unwrap().metrics.clone();
            let mut progress_tracker = metrics::ProgressTracker::new(metrics);

            let results = reporter::run_with_progress(&reporter, &mut progress_tracker, || async {
                benchmark.run().await
            })
            .await?;

            benchmark.teardown().await?;
            (results, "KV GET".to_string())
        }

        Commands::KvMixed(config) => {
            config.validate()?;
            let mut benchmark = benchmarks::kv::KvMixedBenchmark::new(client, config.clone());
            benchmark.setup().await?;

            let metrics = benchmark.pool.as_ref().unwrap().metrics.clone();
            let mut progress_tracker = metrics::ProgressTracker::new(metrics);

            let results = reporter::run_with_progress(&reporter, &mut progress_tracker, || async {
                benchmark.run().await
            })
            .await?;

            benchmark.teardown().await?;
            (
                results,
                format!("KV MIXED (read ratio: {})", config.read_ratio),
            )
        }

        Commands::JsPublish(config) => {
            config.validate()?;
            let mut benchmark =
                benchmarks::jetstream::JetStreamPublishBenchmark::new(client, config.clone());
            benchmark.setup().await?;

            let metrics = benchmark.pool.as_ref().unwrap().metrics.clone();
            let mut progress_tracker = metrics::ProgressTracker::new(metrics);

            let results = reporter::run_with_progress(&reporter, &mut progress_tracker, || async {
                benchmark.run().await
            })
            .await?;

            benchmark.teardown().await?;
            (results, "JetStream PUBLISH".to_string())
        }

        Commands::JsConsume(config) => {
            config.validate()?;
            let mut benchmark =
                benchmarks::jetstream::JetStreamConsumeBenchmark::new(client, config.clone());
            benchmark.setup().await?;

            let metrics = benchmark.pool.as_ref().unwrap().metrics.clone();
            let mut progress_tracker = metrics::ProgressTracker::new(metrics);

            let results = reporter::run_with_progress(&reporter, &mut progress_tracker, || async {
                benchmark.run().await
            })
            .await?;

            benchmark.teardown().await?;
            (results, "JetStream CONSUME".to_string())
        }

        Commands::JsPubSub(config) => {
            config.validate()?;
            let mut benchmark =
                benchmarks::jetstream::JetStreamPubSubBenchmark::new(client, config.clone());
            benchmark.setup().await?;

            let metrics = benchmark.pool.as_ref().unwrap().metrics.clone();
            let mut progress_tracker = metrics::ProgressTracker::new(metrics);

            let results = reporter::run_with_progress(&reporter, &mut progress_tracker, || async {
                benchmark.run().await
            })
            .await?;

            benchmark.teardown().await?;
            (
                results,
                format!(
                    "JetStream PUB/SUB ({} pubs, {} subs)",
                    config.pub_threads, config.sub_threads
                ),
            )
        }
    };

    // Print results
    reporter.print_results(&results, &benchmark_name)?;

    Ok(())
}
