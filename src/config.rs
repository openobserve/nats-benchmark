use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "nats-benchmark")]
#[command(about = "A comprehensive NATS benchmarking tool", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// NATS server URL(s), comma-separated for multiple servers
    #[arg(long, default_value = "nats://localhost:4222", global = true)]
    pub url: String,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Output format: text, json, csv
    #[arg(long, default_value = "text", global = true)]
    pub output: OutputFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    Text,
    Json,
    Csv,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(OutputFormat::Text),
            "json" => Ok(OutputFormat::Json),
            "csv" => Ok(OutputFormat::Csv),
            _ => Err(format!("Invalid output format: {}", s)),
        }
    }
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Benchmark KV PUT operations
    KvPut(KvConfig),

    /// Benchmark KV GET operations
    KvGet(KvConfig),

    /// Benchmark mixed KV operations (GET/PUT)
    KvMixed(KvMixedConfig),

    /// Benchmark JetStream publish operations
    JsPublish(JetStreamConfig),

    /// Benchmark JetStream consume operations
    JsConsume(JetStreamConfig),

    /// Benchmark JetStream pub/sub operations
    JsPubSub(JetStreamPubSubConfig),
}

#[derive(Parser, Debug, Clone)]
pub struct KvConfig {
    /// Number of worker threads
    #[arg(short, long, default_value = "1")]
    pub threads: usize,

    /// Message size in bytes
    #[arg(short, long, default_value = "1024")]
    pub message_size: usize,

    /// Duration to run the benchmark (e.g., 30s, 5m)
    #[arg(short, long, value_parser = humantime::parse_duration)]
    pub duration: Option<Duration>,

    /// Number of messages to send (if not using duration)
    #[arg(short, long)]
    pub count: Option<u64>,

    /// KV bucket name
    #[arg(short, long, default_value = "benchmark")]
    pub bucket: String,

    /// Key prefix for operations
    #[arg(long, default_value = "key")]
    pub key_prefix: String,

    /// Number of unique keys to use
    #[arg(long, default_value = "1000")]
    pub num_keys: usize,

    /// Rate limit (messages per second, 0 = unlimited)
    #[arg(short, long, default_value = "0")]
    pub rate: u64,
}

#[derive(Parser, Debug, Clone)]
pub struct KvMixedConfig {
    #[command(flatten)]
    pub kv_config: KvConfig,

    /// Read ratio (0.0 to 1.0, e.g., 0.7 = 70% reads, 30% writes)
    #[arg(long, default_value = "0.5")]
    pub read_ratio: f64,
}

#[derive(Parser, Debug, Clone)]
pub struct JetStreamConfig {
    /// Number of worker threads
    #[arg(short, long, default_value = "1")]
    pub threads: usize,

    /// Message size in bytes
    #[arg(short, long, default_value = "1024")]
    pub message_size: usize,

    /// Duration to run the benchmark (e.g., 30s, 5m)
    #[arg(short, long, value_parser = humantime::parse_duration)]
    pub duration: Option<Duration>,

    /// Number of messages to send (if not using duration)
    #[arg(short, long)]
    pub count: Option<u64>,

    /// Stream name
    #[arg(long, default_value = "BENCHMARK")]
    pub stream: String,

    /// Subject for publish/subscribe
    #[arg(long, default_value = "benchmark.test")]
    pub subject: String,

    /// Consumer name (for consume operations)
    #[arg(long, default_value = "benchmark-consumer")]
    pub consumer: String,

    /// Batch size for publishing
    #[arg(long, default_value = "1")]
    pub batch_size: usize,

    /// Rate limit (messages per second, 0 = unlimited)
    #[arg(short, long, default_value = "0")]
    pub rate: u64,

    /// Wait for ack (for publish operations)
    #[arg(long)]
    pub wait_for_ack: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct JetStreamPubSubConfig {
    #[command(flatten)]
    pub js_config: JetStreamConfig,

    /// Number of publisher threads
    #[arg(long, default_value = "1")]
    pub pub_threads: usize,

    /// Number of subscriber threads
    #[arg(long, default_value = "1")]
    pub sub_threads: usize,
}

impl KvConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.threads == 0 {
            anyhow::bail!("threads must be greater than 0");
        }
        if self.duration.is_none() && self.count.is_none() {
            anyhow::bail!("either duration or count must be specified");
        }
        if self.num_keys == 0 {
            anyhow::bail!("num_keys must be greater than 0");
        }
        Ok(())
    }
}

impl KvMixedConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.kv_config.validate()?;
        if self.read_ratio < 0.0 || self.read_ratio > 1.0 {
            anyhow::bail!("read_ratio must be between 0.0 and 1.0");
        }
        Ok(())
    }
}

impl JetStreamConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.threads == 0 {
            anyhow::bail!("threads must be greater than 0");
        }
        if self.duration.is_none() && self.count.is_none() {
            anyhow::bail!("either duration or count must be specified");
        }
        if self.batch_size == 0 {
            anyhow::bail!("batch_size must be greater than 0");
        }
        Ok(())
    }
}

impl JetStreamPubSubConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.js_config.validate()?;
        if self.pub_threads == 0 || self.sub_threads == 0 {
            anyhow::bail!("pub_threads and sub_threads must be greater than 0");
        }
        Ok(())
    }
}
