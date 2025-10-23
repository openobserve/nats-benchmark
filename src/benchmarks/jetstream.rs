use crate::benchmarks::Benchmark;
use crate::config::{JetStreamConfig, JetStreamPubSubConfig};
use crate::metrics::BenchmarkResults;
use crate::worker::{generate_payload, WorkerPool};
use anyhow::Result;
use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

pub struct JetStreamPublishBenchmark {
    client: Client,
    config: JetStreamConfig,
    pub pool: Option<WorkerPool>,
}

impl JetStreamPublishBenchmark {
    pub fn new(client: Client, config: JetStreamConfig) -> Self {
        Self {
            client,
            config,
            pool: None,
        }
    }
}

#[async_trait]
impl Benchmark for JetStreamPublishBenchmark {
    async fn setup(&mut self) -> Result<()> {
        info!("Setting up JetStream PUBLISH benchmark");

        let jetstream = async_nats::jetstream::new(self.client.clone());

        // Create or get stream
        let stream_config = async_nats::jetstream::stream::Config {
            name: self.config.stream.clone(),
            subjects: vec![
                self.config.subject.to_string(),
                format!("{}.*", self.config.subject.clone()),
            ],
            ..Default::default()
        };

        match jetstream.get_stream(&self.config.stream).await {
            Ok(_) => {
                info!("Using existing stream: {}", self.config.stream);
            }
            Err(_) => {
                info!("Creating new stream: {}", self.config.stream);
                jetstream.create_stream(stream_config).await?;
            }
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
        info!(
            "Starting JetStream PUBLISH benchmark with {} workers",
            self.config.threads
        );

        let pool = self.pool.as_ref().expect("Pool not initialized");
        let jetstream = async_nats::jetstream::new(self.client.clone());
        let jetstream = Arc::new(jetstream);

        let payload = generate_payload(self.config.message_size);
        let payload = Arc::new(Bytes::from(payload));
        let wait_for_ack = self.config.wait_for_ack;
        let subject = self.config.subject.clone();

        let mut handles = Vec::new();
        for worker_id in 0..self.config.threads {
            let ctx = pool.get_worker_context(worker_id);
            let jetstream = Arc::clone(&jetstream);
            let payload = Arc::clone(&payload);
            let subject = subject.clone();

            let handle = tokio::spawn(async move {
                let mut ops_completed = 0u64;

                while ctx.should_continue(ops_completed) {
                    ctx.rate_limit().await;

                    let start = Instant::now();
                    let result = if wait_for_ack {
                        // Wait for acknowledgment
                        match jetstream.publish(subject.clone(), (*payload).clone()).await {
                            Ok(ack) => ack.await.map(|_| ()),
                            Err(e) => Err(e),
                        }
                    } else {
                        // Fire and forget
                        jetstream
                            .publish(subject.clone(), (*payload).clone())
                            .await
                            .map(|_| ())
                    };

                    match result {
                        Ok(_) => {
                            let latency = start.elapsed().as_micros() as u64;
                            ctx.record_success(latency, payload.len() as u64);
                        }
                        Err(e) => {
                            debug!("Publish error: {}", e);
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
        info!("JetStream PUBLISH benchmark completed");
        Ok(())
    }
}

pub struct JetStreamConsumeBenchmark {
    client: Client,
    config: JetStreamConfig,
    pub pool: Option<WorkerPool>,
}

impl JetStreamConsumeBenchmark {
    pub fn new(client: Client, config: JetStreamConfig) -> Self {
        Self {
            client,
            config,
            pool: None,
        }
    }
}

#[async_trait]
impl Benchmark for JetStreamConsumeBenchmark {
    async fn setup(&mut self) -> Result<()> {
        info!("Setting up JetStream CONSUME benchmark");

        let jetstream = async_nats::jetstream::new(self.client.clone());

        // Get or create stream
        let mut stream = match jetstream.get_stream(&self.config.stream).await {
            Ok(s) => s,
            Err(_) => {
                info!("Creating stream: {}", self.config.stream);
                let stream_config = async_nats::jetstream::stream::Config {
                    name: self.config.stream.clone(),
                    subjects: vec![
                        self.config.subject.to_string(),
                        format!("{}.*", self.config.subject.clone()),
                    ],
                    ..Default::default()
                };
                jetstream.create_stream(stream_config).await?
            }
        };

        // Create or get consumer
        let consumer_config = async_nats::jetstream::consumer::pull::Config {
            name: Some(self.config.consumer.clone()),
            ..Default::default()
        };

        match stream
            .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&self.config.consumer)
            .await
        {
            Ok(_) => {
                info!("Using existing consumer: {}", self.config.consumer);
            }
            Err(_) => {
                info!("Creating consumer: {}", self.config.consumer);
                stream.create_consumer(consumer_config).await?;
            }
        }

        // Pre-populate stream with messages if needed
        let stream_info = stream.info().await?;
        if stream_info.state.messages == 0 {
            info!("Stream is empty, pre-populating with 10000 messages...");
            let payload = generate_payload(self.config.message_size);
            let payload = Bytes::from(payload);
            for _ in 0..10000 {
                jetstream
                    .publish(self.config.subject.clone(), payload.clone())
                    .await?;
            }
            info!("Pre-population complete");
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
        info!(
            "Starting JetStream CONSUME benchmark with {} workers",
            self.config.threads
        );

        let pool = self.pool.as_ref().expect("Pool not initialized");
        let jetstream = async_nats::jetstream::new(self.client.clone());
        let stream = jetstream.get_stream(&self.config.stream).await?;
        let stream = Arc::new(stream);

        let mut handles = Vec::new();
        for worker_id in 0..self.config.threads {
            let ctx = pool.get_worker_context(worker_id);
            let stream = Arc::clone(&stream);
            let consumer_name = format!("{}-{}", self.config.consumer, worker_id);

            let handle = tokio::spawn(async move {
                // Create consumer for this worker
                let consumer_config = async_nats::jetstream::consumer::pull::Config {
                    name: Some(consumer_name.clone()),
                    ..Default::default()
                };

                let consumer = match stream.create_consumer(consumer_config).await {
                    Ok(c) => c,
                    Err(_) => match stream.get_consumer(&consumer_name).await {
                        Ok(c) => c,
                        Err(e) => {
                            debug!("Failed to create/get consumer: {}", e);
                            return;
                        }
                    },
                };

                let mut ops_completed = 0u64;

                while ctx.should_continue(ops_completed) {
                    ctx.rate_limit().await;

                    let start = Instant::now();
                    match consumer.batch().max_messages(1).messages().await {
                        Ok(mut messages) => {
                            if let Some(Ok(message)) = messages.next().await {
                                let latency = start.elapsed().as_micros() as u64;
                                let bytes = message.payload.len() as u64;

                                // Acknowledge the message
                                if let Err(e) = message.ack().await {
                                    debug!("Failed to ack message: {}", e);
                                    ctx.record_error();
                                } else {
                                    ctx.record_success(latency, bytes);
                                }
                            } else {
                                // No messages available
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                                continue;
                            }
                        }
                        Err(e) => {
                            debug!("Consume error: {}", e);
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
        info!("JetStream CONSUME benchmark completed");
        Ok(())
    }
}

pub struct JetStreamPubSubBenchmark {
    client: Client,
    config: JetStreamPubSubConfig,
    pub pool: Option<WorkerPool>,
}

impl JetStreamPubSubBenchmark {
    pub fn new(client: Client, config: JetStreamPubSubConfig) -> Self {
        Self {
            client,
            config,
            pool: None,
        }
    }
}

#[async_trait]
impl Benchmark for JetStreamPubSubBenchmark {
    async fn setup(&mut self) -> Result<()> {
        info!("Setting up JetStream PUB/SUB benchmark");

        let jetstream = async_nats::jetstream::new(self.client.clone());

        // Create or get stream
        let stream_config = async_nats::jetstream::stream::Config {
            name: self.config.js_config.stream.clone(),
            subjects: vec![
                self.config.js_config.subject.to_string(),
                format!("{}.*", self.config.js_config.subject.clone()),
            ],
            ..Default::default()
        };

        match jetstream.get_stream(&self.config.js_config.stream).await {
            Ok(_) => {
                info!("Using existing stream: {}", self.config.js_config.stream);
            }
            Err(_) => {
                info!("Creating new stream: {}", self.config.js_config.stream);
                jetstream.create_stream(stream_config).await?;
            }
        }

        // Create consumers for subscribers
        let stream = jetstream.get_stream(&self.config.js_config.stream).await?;
        for i in 0..self.config.sub_threads {
            let consumer_name = format!("{}-sub-{}", self.config.js_config.consumer, i);
            let consumer_config = async_nats::jetstream::consumer::pull::Config {
                name: Some(consumer_name.clone()),
                ..Default::default()
            };

            match stream.create_consumer(consumer_config).await {
                Ok(_) => debug!("Created consumer: {}", consumer_name),
                Err(_) => debug!("Consumer already exists: {}", consumer_name),
            }
        }

        let total_threads = self.config.pub_threads + self.config.sub_threads;
        let pool = WorkerPool::new(
            total_threads,
            self.config.js_config.duration,
            self.config.js_config.count,
            self.config.js_config.rate,
        );
        self.pool = Some(pool);

        Ok(())
    }

    async fn run(&mut self) -> Result<BenchmarkResults> {
        info!(
            "Starting JetStream PUB/SUB benchmark with {} publishers and {} subscribers",
            self.config.pub_threads, self.config.sub_threads
        );

        let pool = self.pool.as_ref().expect("Pool not initialized");
        let jetstream = async_nats::jetstream::new(self.client.clone());
        let jetstream = Arc::new(jetstream);

        let payload = generate_payload(self.config.js_config.message_size);
        let payload = Arc::new(Bytes::from(payload));

        let mut handles = Vec::new();
        let mut worker_id = 0;

        // Spawn publisher workers
        for _ in 0..self.config.pub_threads {
            let ctx = pool.get_worker_context(worker_id);
            let jetstream = Arc::clone(&jetstream);
            let payload = Arc::clone(&payload);
            let subject = self.config.js_config.subject.clone();
            let wait_for_ack = self.config.js_config.wait_for_ack;

            let handle = tokio::spawn(async move {
                let mut ops_completed = 0u64;

                while ctx.should_continue(ops_completed) {
                    ctx.rate_limit().await;

                    let start = Instant::now();
                    let result = if wait_for_ack {
                        match jetstream.publish(subject.clone(), (*payload).clone()).await {
                            Ok(ack) => ack.await.map(|_| ()),
                            Err(e) => Err(e),
                        }
                    } else {
                        jetstream
                            .publish(subject.clone(), (*payload).clone())
                            .await
                            .map(|_| ())
                    };

                    match result {
                        Ok(_) => {
                            let latency = start.elapsed().as_micros() as u64;
                            ctx.record_success(latency, payload.len() as u64);
                        }
                        Err(e) => {
                            debug!("Publish error: {}", e);
                            ctx.record_error();
                        }
                    }

                    ops_completed += 1;
                }
            });

            handles.push(handle);
            worker_id += 1;
        }

        // Spawn subscriber workers
        let stream = jetstream.get_stream(&self.config.js_config.stream).await?;
        let stream = Arc::new(stream);

        for i in 0..self.config.sub_threads {
            let ctx = pool.get_worker_context(worker_id);
            let stream = Arc::clone(&stream);
            let consumer_name = format!("{}-sub-{}", self.config.js_config.consumer, i);

            let handle = tokio::spawn(async move {
                let consumer = match stream.get_consumer(&consumer_name).await {
                    Ok(c) => c,
                    Err(e) => {
                        debug!("Failed to get consumer: {}", e);
                        return;
                    }
                };

                let mut ops_completed = 0u64;

                while ctx.should_continue(ops_completed) {
                    ctx.rate_limit().await;

                    let start = Instant::now();
                    match consumer.batch().max_messages(1).messages().await {
                        Ok(mut messages) => {
                            if let Some(Ok(message)) = messages.next().await {
                                let latency = start.elapsed().as_micros() as u64;
                                let bytes = message.payload.len() as u64;

                                if let Err(e) = message.ack().await {
                                    debug!("Failed to ack message: {}", e);
                                    ctx.record_error();
                                } else {
                                    ctx.record_success(latency, bytes);
                                }
                            } else {
                                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                                continue;
                            }
                        }
                        Err(e) => {
                            debug!("Consume error: {}", e);
                            ctx.record_error();
                        }
                    }

                    ops_completed += 1;
                }
            });

            handles.push(handle);
            worker_id += 1;
        }

        for handle in handles {
            handle.await?;
        }

        Ok(pool.metrics.finalize())
    }

    async fn teardown(&mut self) -> Result<()> {
        info!("JetStream PUB/SUB benchmark completed");
        Ok(())
    }
}
