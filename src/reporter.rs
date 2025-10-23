use crate::config::OutputFormat;
use crate::metrics::{BenchmarkResults, ProgressUpdate};
use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub struct Reporter {
    format: OutputFormat,
    progress_bar: Option<ProgressBar>,
}

impl Reporter {
    pub fn new(format: OutputFormat, enable_progress: bool) -> Self {
        let progress_bar = if enable_progress && matches!(format, OutputFormat::Text) {
            let pb = ProgressBar::new(0);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] {msg}")
                    .expect("Failed to create progress style")
                    .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
            );
            Some(pb)
        } else {
            None
        };

        Self {
            format,
            progress_bar,
        }
    }

    pub fn update_progress(&self, update: &ProgressUpdate) {
        if let Some(pb) = &self.progress_bar {
            let msg = format!(
                "Ops: {} (success: {}, failed: {}) | Rate: {:.0}/s (avg: {:.0}/s) | Data: {} | Elapsed: {:.1}s",
                update.total_ops,
                update.successful_ops,
                update.failed_ops,
                update.current_rate,
                update.avg_rate,
                format_bytes(update.bytes_transferred),
                update.elapsed_secs,
            );
            pb.set_message(msg);
        }
    }

    pub fn finish_progress(&self) {
        if let Some(pb) = &self.progress_bar {
            pb.finish_and_clear();
        }
    }

    pub fn print_results(&self, results: &BenchmarkResults, benchmark_name: &str) -> Result<()> {
        match self.format {
            OutputFormat::Text => self.print_text_results(results, benchmark_name),
            OutputFormat::Json => self.print_json_results(results),
            OutputFormat::Csv => self.print_csv_results(results),
        }
    }

    fn print_text_results(&self, results: &BenchmarkResults, benchmark_name: &str) -> Result<()> {
        println!("\n{}", "=".repeat(80));
        println!("NATS Benchmark Results - {}", benchmark_name);
        println!("{}", "=".repeat(80));

        println!("\n📊 Overall Statistics:");
        println!("  Duration:              {:.2}s", results.duration_secs);
        println!("  Total Operations:      {}", results.total_operations);
        println!("  Successful:            {}", results.successful_operations);
        println!("  Failed:                {}", results.failed_operations);
        println!(
            "  Success Rate:          {:.2}%",
            if results.total_operations > 0 {
                (results.successful_operations as f64 / results.total_operations as f64) * 100.0
            } else {
                0.0
            }
        );

        println!("\n🚀 Throughput:");
        println!("  Operations/sec:        {:.2}", results.operations_per_sec);
        println!("  Throughput:            {:.2} MB/s", results.throughput_mb_per_sec);
        println!("  Data Transferred:      {}", format_bytes(results.bytes_transferred));

        println!("\n⏱️  Latency (microseconds):");
        println!("  Min:                   {}", results.latency_stats.min_micros);
        println!("  Max:                   {}", results.latency_stats.max_micros);
        println!("  Mean:                  {:.2}", results.latency_stats.mean_micros);
        println!("  StdDev:                {:.2}", results.latency_stats.stddev_micros);
        println!("  p50:                   {}", results.latency_stats.p50_micros);
        println!("  p90:                   {}", results.latency_stats.p90_micros);
        println!("  p95:                   {}", results.latency_stats.p95_micros);
        println!("  p99:                   {}", results.latency_stats.p99_micros);
        println!("  p99.9:                 {}", results.latency_stats.p999_micros);

        if results.worker_stats.len() > 1 {
            println!("\n👷 Worker Statistics:");
            println!(
                "  {:<8} {:<15} {:<15} {:<15}",
                "Worker", "Success", "Failed", "Data"
            );
            println!("  {}", "-".repeat(60));
            for worker in &results.worker_stats {
                println!(
                    "  {:<8} {:<15} {:<15} {:<15}",
                    worker.worker_id,
                    worker.success_count,
                    worker.error_count,
                    format_bytes(worker.bytes_transferred)
                );
            }
        }

        println!("\n{}", "=".repeat(80));

        Ok(())
    }

    fn print_json_results(&self, results: &BenchmarkResults) -> Result<()> {
        let json = serde_json::to_string_pretty(results)?;
        println!("{}", json);
        Ok(())
    }

    fn print_csv_results(&self, results: &BenchmarkResults) -> Result<()> {
        // Print header
        println!("metric,value");

        // Overall stats
        println!("duration_secs,{}", results.duration_secs);
        println!("total_operations,{}", results.total_operations);
        println!("successful_operations,{}", results.successful_operations);
        println!("failed_operations,{}", results.failed_operations);
        println!("operations_per_sec,{}", results.operations_per_sec);
        println!("bytes_transferred,{}", results.bytes_transferred);
        println!("throughput_mb_per_sec,{}", results.throughput_mb_per_sec);

        // Latency stats
        println!("latency_min_micros,{}", results.latency_stats.min_micros);
        println!("latency_max_micros,{}", results.latency_stats.max_micros);
        println!("latency_mean_micros,{}", results.latency_stats.mean_micros);
        println!("latency_stddev_micros,{}", results.latency_stats.stddev_micros);
        println!("latency_p50_micros,{}", results.latency_stats.p50_micros);
        println!("latency_p90_micros,{}", results.latency_stats.p90_micros);
        println!("latency_p95_micros,{}", results.latency_stats.p95_micros);
        println!("latency_p99_micros,{}", results.latency_stats.p99_micros);
        println!("latency_p999_micros,{}", results.latency_stats.p999_micros);

        // Worker stats
        if !results.worker_stats.is_empty() {
            println!("\nworker_id,success_count,error_count,bytes_transferred");
            for worker in &results.worker_stats {
                println!(
                    "{},{},{},{}",
                    worker.worker_id,
                    worker.success_count,
                    worker.error_count,
                    worker.bytes_transferred
                );
            }
        }

        Ok(())
    }
}

pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

pub async fn run_with_progress<F, Fut>(
    reporter: &Reporter,
    progress_tracker: &mut crate::metrics::ProgressTracker,
    benchmark_task: F,
) -> Result<BenchmarkResults>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<BenchmarkResults>>,
{
    // Spawn progress update task
    let update_interval = Duration::from_millis(500);
    let mut interval = tokio::time::interval(update_interval);

    let benchmark_future = benchmark_task();
    tokio::pin!(benchmark_future);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let update = progress_tracker.update();
                reporter.update_progress(&update);
            }
            result = &mut benchmark_future => {
                reporter.finish_progress();
                return result;
            }
        }
    }
}
