use std::{sync::Arc, time::Duration};

use crate::config::Config;
use redis::Client as RedisClient;
use redis::ConnectionAddr;
use redis::ConnectionInfo;
use redis::RedisConnectionInfo;
use redis::aio::ConnectionManager;
use reqwest::Client;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio::sync::{mpsc, Mutex};
use std::collections::VecDeque;
use crate::models::JobPayload;
use crate::memstore::MemStoreClient;

#[derive(Clone)]
pub struct AppState {
    pub http: Client,
    pub redis: Option<ConnectionManager>,
    pub redis_client: Option<RedisClient>,
    pub default_base: String,
    pub fallback_base: String,
    pub req_timeout: Duration,
    pub trace_slow_ms: u64,
    pub health: Arc<RwLock<HealthState>>,
    pub cb_fail_threshold: u32,
    pub cb_open_duration: Duration,
    pub queue_key: String,
    pub idemp_ttl: u64,
    pub max_retries: u32,
    pub retry_backoff: Duration,
    pub timeout_margin: Duration,
    pub admin_token: String,
    pub sem_default: Arc<Semaphore>,
    pub sem_fallback: Arc<Semaphore>,
    pub memstore: Option<MemStoreClient>,
    pub queue_tx: mpsc::UnboundedSender<JobPayload>,
    pub queue_rx: Arc<Mutex<mpsc::UnboundedReceiver<JobPayload>>>,
}

#[derive(Debug, Default, Clone)]
pub struct HealthState {
    pub default: ProcStatus,
    pub fallback: ProcStatus,
}

#[derive(Debug, Default, Clone)]
pub struct ProcStatus {
    pub health: ProcHealth,
    pub circuit: Circuit,
}

#[derive(Debug, Default, Clone)]
pub struct ProcHealth {
    pub failing: Option<bool>,
    pub min_response_time_ms: Option<u64>,
    pub last_checked: Option<Instant>,
}

#[derive(Debug, Default, Clone)]
pub struct Circuit {
    pub failures: u32,
    pub open_until: Option<Instant>,
}

#[derive(Error, Debug)]
pub enum AppError {
    #[error("invalid amount")]
    InvalidAmount,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        use axum::{Json, http::StatusCode};
        let code = match self {
            AppError::InvalidAmount => StatusCode::BAD_REQUEST,
            AppError::Other(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, Json(serde_json::json!({ "error": self.to_string() }))).into_response()
    }
}

pub async fn build_state(cfg: &Config) -> anyhow::Result<AppState> {
    let http = Client::builder()
        .http1_only()
        .pool_max_idle_per_host(128)
        .pool_idle_timeout(Duration::from_secs(5))
        .tcp_keepalive(Duration::from_secs(10))
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_millis(20))
        .timeout(cfg.req_timeout)
        .build()?;

    let (redis_client, redis_manager) = if cfg.memstore_socket.is_some() {
        (None, None)
    } else {
        let client = if let Some(path) = &cfg.redis_socket {
            let info = ConnectionInfo {
                addr: ConnectionAddr::Unix(path.into()),
                redis: RedisConnectionInfo { db: 0, username: None, password: None },
            };
            redis::Client::open(info)?
        } else {
            redis::Client::open(cfg.redis_url.clone())?
        };
        let manager = ConnectionManager::new(client.clone()).await?;
        (Some(client), Some(manager))
    };

    // local mpsc queue (substitui Redis para enfileiramento)
    let (tx, rx) = mpsc::unbounded_channel::<JobPayload>();

    Ok(AppState {
        http,
        redis: redis_manager,
        redis_client: redis_client,
        default_base: cfg.default_base.clone(),
        fallback_base: cfg.fallback_base.clone(),
        req_timeout: cfg.req_timeout,
        trace_slow_ms: cfg.trace_slow_ms,
        health: Arc::new(RwLock::new(HealthState::default())),
        cb_fail_threshold: cfg.cb_fail_threshold,
        cb_open_duration: cfg.cb_open_duration,
        queue_key: cfg.queue_key.clone(),
        idemp_ttl: cfg.idemp_ttl,
        max_retries: cfg.max_retries,
        retry_backoff: cfg.retry_backoff,
        timeout_margin: cfg.timeout_margin,
        admin_token: cfg.admin_token.clone(),
        sem_default: Arc::new(Semaphore::new(cfg.proc_concurrency)),
        sem_fallback: Arc::new(Semaphore::new(cfg.proc_concurrency)),
        memstore: cfg.memstore_socket.clone().map(MemStoreClient::new),
        queue_tx: tx,
        queue_rx: Arc::new(Mutex::new(rx)),
    })
}

pub enum ProcChoice {
    Default,
    Fallback,
}

pub async fn choose_target(state: &AppState) -> ProcChoice {
    let now = Instant::now();
    let hs = state.health.read().await;
    
    // prioriza baseado em circuit breaker e health
    let def_circuit_open = hs.default.circuit.open_until.map_or(false, |t| t > now);
    let fb_circuit_open = hs.fallback.circuit.open_until.map_or(false, |t| t > now);
    let def_failing = hs.default.health.failing.unwrap_or(false);
    let fb_failing = hs.fallback.health.failing.unwrap_or(false);
    
    // caminho rápido: default disponível
    if !def_circuit_open && !def_failing {
        return ProcChoice::Default;
    }
    
    // fallback se disponível
    if !fb_circuit_open && !fb_failing {
        return ProcChoice::Fallback;
    }
    
    // último recurso: default mesmo se não ideal
    ProcChoice::Default
}

pub async fn update_circuit_after(state: &AppState, proc_name: &str, success: bool) {
    let now = Instant::now();
    let mut hs = state.health.write().await;
    let ps = match proc_name {
        "default" => &mut hs.default,
        _ => &mut hs.fallback,
    };
    if success {
        // recuperação gradual
        ps.circuit.failures = ps.circuit.failures.saturating_sub(1);
        if ps.circuit.failures == 0 {
            ps.circuit.open_until = None;
        }
    } else {
        ps.circuit.failures = ps.circuit.failures.saturating_add(1);
        if ps.circuit.failures >= state.cb_fail_threshold {
            // backoff exponencial do circuit breaker
            let multiplier = 1 << ps.circuit.failures.min(4);
            ps.circuit.open_until = Some(now + state.cb_open_duration * multiplier);
            ps.circuit.failures = 0;
        }
    }
}

pub async fn compute_timeout(state: &AppState, proc_name: &str) -> Duration {
    let hs = state.health.read().await;
    let (failing, min_rt) = if proc_name == "default" {
        (
            hs.default.health.failing,
            hs.default.health.min_response_time_ms,
        )
    } else {
        (
            hs.fallback.health.failing,
            hs.fallback.health.min_response_time_ms,
        )
    };
    if let (Some(false), Some(ms)) = (failing, min_rt) {
        let base = ms + state.timeout_margin.as_millis() as u64;
        // Permite base maior que req_timeout, com teto seguro para evitar pendurar
        let max_cap = 200u64;
        Duration::from_millis(base.max(state.req_timeout.as_millis() as u64).min(max_cap))
    } else {
        state.req_timeout
    }
}
