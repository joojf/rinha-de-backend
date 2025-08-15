use std::{net::SocketAddr, time::Duration};

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub default_base: String,
    pub fallback_base: String,
    pub redis_url: String,
    pub redis_socket: Option<String>,
    pub req_timeout: Duration,
    pub trace_slow_ms: u64,
    pub cb_fail_threshold: u32,
    pub cb_open_duration: Duration,
    pub queue_key: String,
    pub idemp_ttl: u64,
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub timeout_margin: Duration,
    pub workers: usize,
    pub admin_token: String,
    pub proc_concurrency: usize,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let bind_addr: SocketAddr = std::env::var("BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:9999".to_string())
            .parse()?;
        let default_base =
            std::env::var("DEFAULT_URL").unwrap_or_else(|_| "http://localhost:8001".to_string());
        let fallback_base =
            std::env::var("FALLBACK_URL").unwrap_or_else(|_| "http://localhost:8002".to_string());
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let redis_socket = std::env::var("REDIS_SOCKET").ok();
        let req_timeout = std::env::var("REQ_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(120));
        let trace_slow_ms = std::env::var("TRACE_SLOW_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(10);
        let cb_fail_threshold = std::env::var("CB_FAIL_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(3);
        let cb_open_duration = std::env::var("CB_OPEN_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(1500));
        let queue_key = std::env::var("QUEUE_KEY").unwrap_or_else(|_| "q:payments".to_string());
        let idemp_ttl = std::env::var("IDEMP_TTL_SEC")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(24 * 60 * 60);
        let max_retries = std::env::var("MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(3);
        let retry_backoff = std::env::var("RETRY_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(100));
        let timeout_margin = std::env::var("TIMEOUT_MARGIN_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(5));
        let workers = std::env::var("WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(32);
        let proc_concurrency = std::env::var("PROC_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(8);
        let admin_token = std::env::var("ADMIN_TOKEN").unwrap_or_else(|_| "123".to_string());

        Ok(Self {
            bind_addr,
            default_base,
            fallback_base,
            redis_url,
            redis_socket,
            req_timeout,
            trace_slow_ms,
            cb_fail_threshold,
            cb_open_duration,
            queue_key,
            idemp_ttl,
            max_retries,
            retry_backoff,
            timeout_margin,
            workers,
            admin_token,
            proc_concurrency,
        })
    }
}
