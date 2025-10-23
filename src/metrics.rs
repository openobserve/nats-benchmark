use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Thread-safe metrics collector
#[derive(Clone)]
pub struct Metrics {
    start_time: Instant,
    success_count: Arc<AtomicU64>,
    error_count: Arc<AtomicU64>,
    bytes_transferred: Arc<AtomicU64>,
    latencies: Arc<Mutex<Histogram<u64>>>,
    worker_metrics: Arc<Mutex<Vec<WorkerMetrics>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetrics {
    pub worker_id: usize,
    pub success_count: u64,
    pub error_count: u64,
    pub bytes_transferred: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkResults {
    pub duration_secs: f64,
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub operations_per_sec: f64,
    pub bytes_transferred: u64,
    pub throughput_mb_per_sec: f64,
    pub latency_stats: LatencyStats,
    pub worker_stats: Vec<WorkerMetrics>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyStats {
    pub min_micros: u64,
    pub max_micros: u64,
    pub mean_micros: f64,
    pub stddev_micros: f64,
    pub p50_micros: u64,
    pub p90_micros: u64,
    pub p95_micros: u64,
    pub p99_micros: u64,
    pub p999_micros: u64,
}

impl Metrics {
    pub fn new(num_workers: usize) -> Self {
        let mut worker_metrics = Vec::with_capacity(num_workers);
        for i in 0..num_workers {
            worker_metrics.push(WorkerMetrics {
                worker_id: i,
                success_count: 0,
                error_count: 0,
                bytes_transferred: 0,
            });
        }

        Self {
            start_time: Instant::now(),
            success_count: Arc::new(AtomicU64::new(0)),
            error_count: Arc::new(AtomicU64::new(0)),
            bytes_transferred: Arc::new(AtomicU64::new(0)),
            // Track latencies from 1 microsecond to 1 minute with 3 significant digits
            latencies: Arc::new(Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
                    .expect("Failed to create histogram"),
            )),
            worker_metrics: Arc::new(Mutex::new(worker_metrics)),
        }
    }

    pub fn record_success(&self, worker_id: usize, latency_micros: u64, bytes: u64) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);

        // Record latency
        if let Ok(mut hist) = self.latencies.lock() {
            let _ = hist.record(latency_micros);
        }

        // Update worker metrics
        if let Ok(mut workers) = self.worker_metrics.lock() {
            if let Some(worker) = workers.get_mut(worker_id) {
                worker.success_count += 1;
                worker.bytes_transferred += bytes;
            }
        }
    }

    pub fn record_error(&self, worker_id: usize) {
        self.error_count.fetch_add(1, Ordering::Relaxed);

        if let Ok(mut workers) = self.worker_metrics.lock() {
            if let Some(worker) = workers.get_mut(worker_id) {
                worker.error_count += 1;
            }
        }
    }

    pub fn get_current_stats(&self) -> (u64, u64, u64, f64) {
        let success = self.success_count.load(Ordering::Relaxed);
        let errors = self.error_count.load(Ordering::Relaxed);
        let bytes = self.bytes_transferred.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        (success, errors, bytes, elapsed)
    }

    pub fn finalize(&self) -> BenchmarkResults {
        let duration = self.start_time.elapsed();
        let duration_secs = duration.as_secs_f64();

        let success = self.success_count.load(Ordering::Relaxed);
        let errors = self.error_count.load(Ordering::Relaxed);
        let total = success + errors;
        let bytes = self.bytes_transferred.load(Ordering::Relaxed);

        let ops_per_sec = if duration_secs > 0.0 {
            total as f64 / duration_secs
        } else {
            0.0
        };

        let throughput_mb_per_sec = if duration_secs > 0.0 {
            (bytes as f64) / (1024.0 * 1024.0 * duration_secs)
        } else {
            0.0
        };

        let latency_stats = self.calculate_latency_stats();
        let worker_stats = self.worker_metrics.lock().unwrap().clone();

        BenchmarkResults {
            duration_secs,
            total_operations: total,
            successful_operations: success,
            failed_operations: errors,
            operations_per_sec: ops_per_sec,
            bytes_transferred: bytes,
            throughput_mb_per_sec,
            latency_stats,
            worker_stats,
        }
    }

    fn calculate_latency_stats(&self) -> LatencyStats {
        let hist = self.latencies.lock().unwrap();

        if hist.len() == 0 {
            return LatencyStats {
                min_micros: 0,
                max_micros: 0,
                mean_micros: 0.0,
                stddev_micros: 0.0,
                p50_micros: 0,
                p90_micros: 0,
                p95_micros: 0,
                p99_micros: 0,
                p999_micros: 0,
            };
        }

        LatencyStats {
            min_micros: hist.min(),
            max_micros: hist.max(),
            mean_micros: hist.mean(),
            stddev_micros: hist.stdev(),
            p50_micros: hist.value_at_quantile(0.50),
            p90_micros: hist.value_at_quantile(0.90),
            p95_micros: hist.value_at_quantile(0.95),
            p99_micros: hist.value_at_quantile(0.99),
            p999_micros: hist.value_at_quantile(0.999),
        }
    }
}

/// Progress tracker for live updates
pub struct ProgressTracker {
    metrics: Metrics,
    last_count: u64,
    last_check: Instant,
}

impl ProgressTracker {
    pub fn new(metrics: Metrics) -> Self {
        Self {
            metrics,
            last_count: 0,
            last_check: Instant::now(),
        }
    }

    pub fn update(&mut self) -> ProgressUpdate {
        let (success, errors, bytes, elapsed) = self.metrics.get_current_stats();
        let total = success + errors;

        let now = Instant::now();
        let interval = now.duration_since(self.last_check).as_secs_f64();

        let current_rate = if interval > 0.0 {
            (total - self.last_count) as f64 / interval
        } else {
            0.0
        };

        self.last_count = total;
        self.last_check = now;

        ProgressUpdate {
            total_ops: total,
            successful_ops: success,
            failed_ops: errors,
            elapsed_secs: elapsed,
            current_rate,
            avg_rate: if elapsed > 0.0 { total as f64 / elapsed } else { 0.0 },
            bytes_transferred: bytes,
        }
    }
}

#[derive(Debug)]
pub struct ProgressUpdate {
    pub total_ops: u64,
    pub successful_ops: u64,
    pub failed_ops: u64,
    pub elapsed_secs: f64,
    pub current_rate: f64,
    pub avg_rate: f64,
    pub bytes_transferred: u64,
}
