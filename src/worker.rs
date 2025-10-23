use crate::metrics::Metrics;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Controls when workers should stop
pub struct StopSignal {
    should_stop: Arc<AtomicBool>,
    max_operations: Option<u64>,
    end_time: Option<Instant>,
}

impl StopSignal {
    pub fn new(duration: Option<Duration>, count: Option<u64>) -> Self {
        let end_time = duration.map(|d| Instant::now() + d);
        Self {
            should_stop: Arc::new(AtomicBool::new(false)),
            max_operations: count,
            end_time,
        }
    }

    pub fn should_stop(&self, operations_completed: u64) -> bool {
        // Check if manually stopped
        if self.should_stop.load(Ordering::Relaxed) {
            return true;
        }

        // Check operation count limit
        if let Some(max_ops) = self.max_operations {
            if operations_completed >= max_ops {
                return true;
            }
        }

        // Check time limit
        if let Some(end) = self.end_time {
            if Instant::now() >= end {
                return true;
            }
        }

        false
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }

    pub fn clone_signal(&self) -> Self {
        Self {
            should_stop: Arc::clone(&self.should_stop),
            max_operations: self.max_operations,
            end_time: self.end_time,
        }
    }
}

/// Rate limiter for controlling throughput
pub struct RateLimiter {
    semaphore: Option<Arc<Semaphore>>,
    refill_task: Option<tokio::task::JoinHandle<()>>,
}

impl RateLimiter {
    pub fn new(rate: u64) -> Self {
        if rate == 0 {
            // Unlimited rate - no semaphore needed
            return Self {
                semaphore: None,
                refill_task: None,
            };
        }

        let semaphore = Arc::new(Semaphore::new(rate as usize));
        let sem_clone = Arc::clone(&semaphore);
        let permits_per_sec = rate;

        // Spawn a task to refill permits
        let refill_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                // Add permits back up to the limit
                let current = sem_clone.available_permits();
                let to_add = if current < permits_per_sec as usize {
                    permits_per_sec as usize - current
                } else {
                    0
                };
                if to_add > 0 {
                    sem_clone.add_permits(to_add);
                }
            }
        });

        Self {
            semaphore: Some(semaphore),
            refill_task: Some(refill_task),
        }
    }

    pub async fn acquire(&self) {
        if let Some(sem) = &self.semaphore {
            let _ = sem.acquire().await;
        }
        // If no semaphore, no rate limiting - return immediately
    }

    pub fn shutdown(self) {
        if let Some(task) = self.refill_task {
            task.abort();
        }
    }
}

/// Worker pool coordinator
pub struct WorkerPool {
    pub num_workers: usize,
    pub metrics: Metrics,
    pub stop_signal: StopSignal,
    pub rate_limiter: Arc<RateLimiter>,
}

impl WorkerPool {
    pub fn new(
        num_workers: usize,
        duration: Option<Duration>,
        count: Option<u64>,
        rate: u64,
    ) -> Self {
        Self {
            num_workers,
            metrics: Metrics::new(num_workers),
            stop_signal: StopSignal::new(duration, count),
            rate_limiter: Arc::new(RateLimiter::new(rate)),
        }
    }

    pub fn get_worker_context(&self, worker_id: usize) -> WorkerContext {
        WorkerContext {
            worker_id,
            metrics: self.metrics.clone(),
            stop_signal: self.stop_signal.clone_signal(),
            rate_limiter: Arc::clone(&self.rate_limiter),
        }
    }
}

/// Context passed to each worker
pub struct WorkerContext {
    pub worker_id: usize,
    pub metrics: Metrics,
    pub stop_signal: StopSignal,
    pub rate_limiter: Arc<RateLimiter>,
}

impl WorkerContext {
    pub fn should_continue(&self, ops_completed: u64) -> bool {
        !self.stop_signal.should_stop(ops_completed)
    }

    pub async fn rate_limit(&self) {
        self.rate_limiter.acquire().await;
    }

    pub fn record_success(&self, latency_micros: u64, bytes: u64) {
        self.metrics.record_success(self.worker_id, latency_micros, bytes);
    }

    pub fn record_error(&self) {
        self.metrics.record_error(self.worker_id);
    }
}

/// Generate random data for payloads
pub fn generate_payload(size: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}

/// Generate key name for KV operations
pub fn generate_key(prefix: &str, index: usize, total_keys: usize) -> String {
    let key_num = index % total_keys;
    format!("{}_{:08}", prefix, key_num)
}
