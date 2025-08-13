use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub ingest_addr: SocketAddr,
    pub default_base: String,
    pub redis_url: String,
    pub queue_key: String,
    pub idemp_ttl: u64,
    pub req_timeout: Duration,
    pub workers: usize,
    pub batch_size: usize,
    pub batch_flush_ms: u64,
}

fn env_str(k: &str, d: &str) -> String { std::env::var(k).unwrap_or_else(|_| d.to_string()) }
fn env_u64(k: &str, d: u64) -> u64 { std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d) }
fn env_usize(k: &str, d: usize) -> usize { std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d) }

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let bind_addr: SocketAddr = env_str("BIND_ADDR", "0.0.0.0:9999").parse()?;
        let ingest_addr: SocketAddr = env_str("INGEST_ADDR", "127.0.0.1:7070").parse()?;
        let default_base = env_str("DEFAULT_URL", "http://payment-processor-default:8080");
        let redis_url = env_str("REDIS_URL", "redis://127.0.0.1:6379");
        let queue_key = env_str("QUEUE_KEY", "q:payments");
        let idemp_ttl = env_u64("IDEMP_TTL_SEC", 24*60*60);
        let req_timeout = Duration::from_millis(env_u64("REQ_TIMEOUT_MS", 80));
        let workers = env_usize("WORKERS", 64);
        let batch_size = env_usize("BATCH_SIZE", 256);
        let batch_flush_ms = env_u64("BATCH_FLUSH_MS", 1);
        Ok(Self { bind_addr, ingest_addr, default_base, redis_url, queue_key, idemp_ttl, req_timeout, workers, batch_size, batch_flush_ms })
    }
}


