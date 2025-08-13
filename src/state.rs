use std::time::Duration;
use reqwest::Client;
use redis::aio::ConnectionManager;

#[derive(Clone)]
pub struct AppState {
    pub http: Client,
    pub redis_client: redis::Client,
    pub redis: ConnectionManager,
    pub queue_key: String,
    pub idemp_ttl: u64,
    pub req_timeout: Duration,
    pub default_base: String,
}


