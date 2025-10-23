use crate::benchmarks::Benchmark;
use crate::config::{KvConfig, KvMixedConfig};
use crate::metrics::BenchmarkResults;
use crate::worker::{generate_key, generate_payload, WorkerPool};
use anyhow::Result;
use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

pub struct KvPutBenchmark {
    client: Client,
    config: KvConfig,
    pub pool: Option<WorkerPool>,
}

impl KvPutBenchmark {
    pub fn new(client: Client, config: KvConfig) -> Self {
        Self {
            client,
            config,
            pool: None,
        }
    }
}

#[async_trait]
impl Benchmark for KvPutBenchmark {
    async fn setup(&mut self) -> Result<()> {
        info!("Setting up KV PUT benchmark");

        // Create or get the KV bucket
        let jetstream = async_nats::jetstream::new(self.client.clone());

        // Try to get existing bucket or create new one
        let _kv = match jetstream.get_key_value(&self.config.bucket).await {
            Ok(kv) => {
                info!("Using existing KV bucket: {}", self.config.bucket);
                kv
            }
            Err(_) => {
                info!("Creating new KV bucket: {}", self.config.bucket);
                jetstream.create_key_value(async_nats::jetstream::kv::Config {
                    bucket: self.config.bucket.clone(),
                    description: "Benchmark bucket".to_string(),
                    ..Default::default()
                }).await?
            }
        };

        // Initialize worker pool
        let pool = WorkerPool::new(
            self.config.threads,
            self.config.duration,
            self.config.count,
            self.config.rate,
        );
        self.pool = Some(pool);

        Ok(())
    }

    async fn run(&mut self) -> Result<BenchmarkResults> {
        info!("Starting KV PUT benchmark with {} workers", self.config.threads);

        let pool = self.pool.as_ref().expect("Pool not initialized");
        let jetstream = async_nats::jetstream::new(self.client.clone());
        let kv = jetstream.get_key_value(&self.config.bucket).await?;
        let kv = Arc::new(kv);

        let payload = generate_payload(self.config.message_size);
        let payload = Arc::new(Bytes::from(payload));

        // Spawn worker tasks
        let mut handles = Vec::new();
        for worker_id in 0..self.config.threads {
            let ctx = pool.get_worker_context(worker_id);
            let kv = Arc::clone(&kv);
            let payload = Arc::clone(&payload);
            let key_prefix = self.config.key_prefix.clone();
            let num_keys = self.config.num_keys;

            let handle = tokio::spawn(async move {
                let mut ops_completed = 0u64;

                while ctx.should_continue(ops_completed) {
                    ctx.rate_limit().await;

                    let key = generate_key(&key_prefix, ops_completed as usize, num_keys);

                    let start = Instant::now();
                    match kv.put(&key, (*payload).clone()).await {
                        Ok(_) => {
                            let latency = start.elapsed().as_micros() as u64;
                            ctx.record_success(latency, payload.len() as u64);
                        }
                        Err(e) => {
                            debug!("PUT error: {}", e);
                            ctx.record_error();
                        }
                    }

                    ops_completed += 1;
                }
            });

            handles.push(handle);
        }

        // Wait for all workers to complete
        for handle in handles {
            handle.await?;
        }

        Ok(pool.metrics.finalize())
    }

    async fn teardown(&mut self) -> Result<()> {
        info!("KV PUT benchmark completed");
        Ok(())
    }
}

pub struct KvGetBenchmark {
    client: Client,
    config: KvConfig,
    pub pool: Option<WorkerPool>,
}

impl KvGetBenchmark {
    pub fn new(client: Client, config: KvConfig) -> Self {
        Self {
            client,
            config,
            pool: None,
        }
    }
}

#[async_trait]
impl Benchmark for KvGetBenchmark {
    async fn setup(&mut self) -> Result<()> {
        info!("Setting up KV GET benchmark");

        let jetstream = async_nats::jetstream::new(self.client.clone());

        // Get or create bucket
        let kv = match jetstream.get_key_value(&self.config.bucket).await {
            Ok(kv) => kv,
            Err(_) => {
                info!("Creating KV bucket with test data: {}", self.config.bucket);
                let kv = jetstream.create_key_value(async_nats::jetstream::kv::Config {
                    bucket: self.config.bucket.clone(),
                    description: "Benchmark bucket".to_string(),
                    ..Default::default()
                }).await?;

                // Pre-populate with data
                info!("Pre-populating {} keys...", self.config.num_keys);
                let payload = Bytes::from(generate_payload(self.config.message_size));
                for i in 0..self.config.num_keys {
                    let key = generate_key(&self.config.key_prefix, i, self.config.num_keys);
                    kv.put(&key, payload.clone()).await?;
                }
                info!("Pre-population complete");
                kv
            }
        };

        // Verify keys exist
        let test_key = generate_key(&self.config.key_prefix, 0, self.config.num_keys);
        match kv.get(&test_key).await {
            Ok(Some(_)) => info!("KV bucket ready with data"),
            Ok(None) => {
                info!("Bucket exists but empty, pre-populating...");
                let payload = Bytes::from(generate_payload(self.config.message_size));
                for i in 0..self.config.num_keys {
                    let key = generate_key(&self.config.key_prefix, i, self.config.num_keys);
                    kv.put(&key, payload.clone()).await?;
                }
            }
            Err(e) => anyhow::bail!("Failed to verify KV bucket: {}", e),
        }

        let pool = WorkerPool::new(
            self.config.threads,
            self.config.duration,
            self.config.count,
            self.config.rate,
        );
        self.pool = Some(pool);

        Ok(())
    }

    async fn run(&mut self) -> Result<BenchmarkResults> {
        info!("Starting KV GET benchmark with {} workers", self.config.threads);

        let pool = self.pool.as_ref().expect("Pool not initialized");
        let jetstream = async_nats::jetstream::new(self.client.clone());
        let kv = jetstream.get_key_value(&self.config.bucket).await?;
        let kv = Arc::new(kv);

        let mut handles = Vec::new();
        for worker_id in 0..self.config.threads {
            let ctx = pool.get_worker_context(worker_id);
            let kv = Arc::clone(&kv);
            let key_prefix = self.config.key_prefix.clone();
            let num_keys = self.config.num_keys;

            let handle = tokio::spawn(async move {
                let mut ops_completed = 0u64;

                while ctx.should_continue(ops_completed) {
                    ctx.rate_limit().await;

                    let key = generate_key(&key_prefix, ops_completed as usize, num_keys);

                    let start = Instant::now();
                    match kv.get(&key).await {
                        Ok(Some(value)) => {
                            let latency = start.elapsed().as_micros() as u64;
                            ctx.record_success(latency, value.len() as u64);
                        }
                        Ok(None) => {
                            debug!("Key not found: {}", key);
                            ctx.record_error();
                        }
                        Err(e) => {
                            debug!("GET error: {}", e);
                            ctx.record_error();
                        }
                    }

                    ops_completed += 1;
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }

        Ok(pool.metrics.finalize())
    }

    async fn teardown(&mut self) -> Result<()> {
        info!("KV GET benchmark completed");
        Ok(())
    }
}

pub struct KvMixedBenchmark {
    client: Client,
    config: KvMixedConfig,
    pub pool: Option<WorkerPool>,
}

impl KvMixedBenchmark {
    pub fn new(client: Client, config: KvMixedConfig) -> Self {
        Self {
            client,
            config,
            pool: None,
        }
    }
}

#[async_trait]
impl Benchmark for KvMixedBenchmark {
    async fn setup(&mut self) -> Result<()> {
        info!("Setting up KV MIXED benchmark (read ratio: {})", self.config.read_ratio);

        let jetstream = async_nats::jetstream::new(self.client.clone());

        // Get or create bucket and pre-populate
        let kv = match jetstream.get_key_value(&self.config.kv_config.bucket).await {
            Ok(kv) => kv,
            Err(_) => {
                info!("Creating KV bucket: {}", self.config.kv_config.bucket);
                jetstream.create_key_value(async_nats::jetstream::kv::Config {
                    bucket: self.config.kv_config.bucket.clone(),
                    description: "Benchmark bucket".to_string(),
                    ..Default::default()
                }).await?
            }
        };

        // Pre-populate with initial data
        info!("Pre-populating {} keys...", self.config.kv_config.num_keys);
        let payload = Bytes::from(generate_payload(self.config.kv_config.message_size));
        for i in 0..self.config.kv_config.num_keys {
            let key = generate_key(&self.config.kv_config.key_prefix, i, self.config.kv_config.num_keys);
            kv.put(&key, payload.clone()).await?;
        }
        info!("Pre-population complete");

        let pool = WorkerPool::new(
            self.config.kv_config.threads,
            self.config.kv_config.duration,
            self.config.kv_config.count,
            self.config.kv_config.rate,
        );
        self.pool = Some(pool);

        Ok(())
    }

    async fn run(&mut self) -> Result<BenchmarkResults> {
        info!("Starting KV MIXED benchmark with {} workers", self.config.kv_config.threads);

        let pool = self.pool.as_ref().expect("Pool not initialized");
        let jetstream = async_nats::jetstream::new(self.client.clone());
        let kv = jetstream.get_key_value(&self.config.kv_config.bucket).await?;
        let kv = Arc::new(kv);

        let payload = generate_payload(self.config.kv_config.message_size);
        let payload = Arc::new(Bytes::from(payload));
        let read_ratio = self.config.read_ratio;

        let mut handles = Vec::new();
        for worker_id in 0..self.config.kv_config.threads {
            let ctx = pool.get_worker_context(worker_id);
            let kv = Arc::clone(&kv);
            let payload = Arc::clone(&payload);
            let key_prefix = self.config.kv_config.key_prefix.clone();
            let num_keys = self.config.kv_config.num_keys;

            let handle = tokio::spawn(async move {
                use rand::rngs::StdRng;
                use rand::{Rng, SeedableRng};
                let mut rng = StdRng::from_entropy();
                let mut ops_completed = 0u64;

                while ctx.should_continue(ops_completed) {
                    ctx.rate_limit().await;

                    let key = generate_key(&key_prefix, ops_completed as usize, num_keys);
                    let do_read = rng.gen::<f64>() < read_ratio;

                    let start = Instant::now();
                    if do_read {
                        // GET operation
                        match kv.get(&key).await {
                            Ok(Some(value)) => {
                                let latency = start.elapsed().as_micros() as u64;
                                ctx.record_success(latency, value.len() as u64);
                            }
                            Ok(None) => {
                                ctx.record_error();
                            }
                            Err(e) => {
                                debug!("GET error: {}", e);
                                ctx.record_error();
                            }
                        }
                    } else {
                        // PUT operation
                        match kv.put(&key, (*payload).clone()).await {
                            Ok(_) => {
                                let latency = start.elapsed().as_micros() as u64;
                                ctx.record_success(latency, payload.len() as u64);
                            }
                            Err(e) => {
                                debug!("PUT error: {}", e);
                                ctx.record_error();
                            }
                        }
                    }

                    ops_completed += 1;
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }

        Ok(pool.metrics.finalize())
    }

    async fn teardown(&mut self) -> Result<()> {
        info!("KV MIXED benchmark completed");
        Ok(())
    }
}
